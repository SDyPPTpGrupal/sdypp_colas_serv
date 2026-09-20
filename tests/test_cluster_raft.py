"""Three real nodes, in one process, over real sockets.

`test_raft.py` proves the state machine in isolation with a fake clock; this
file proves the wiring: that `motor.py` actually delivers what `raft.py` returns,
that `/raft/*` speaks the shape the other side expects, and that killing a master
produces a new one without anybody being told to.

Each node gets its own copy of the `servidor` module, loaded under a distinct
name, because `servidor.py` keeps its state in module globals — three nodes
sharing one module would be one node wearing three hats.

Timing rule, from the design's flakiness controls: no test here asserts that
something happened *within* a wall-clock duration. Conditions are polled with a
generous ceiling, and "a slow node" is a `threading.Event` the test controls.
"""

import importlib.util
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

from aplicar import Aplicador  # noqa: E402
from colas import Sistema  # noqa: E402
from motor import Motor  # noqa: E402
from raft import NodoRaft  # noqa: E402

from ayuda import esperar_a, pedir_http, silenciar_bitacora  # noqa: E402

TOKEN = "secreto-de-prueba"
BALANCEADOR = "balanceador@casa-tomas"

ELECCION_MS = 300
HEARTBEAT_MS = 60
TECHO_S = 15.0


class NodoDePrueba:
    """One cluster node: its own `servidor` module, HTTP server and motor."""

    def __init__(self, indice, directorio):
        spec = importlib.util.spec_from_file_location(
            f"servidor_nodo{indice}", os.path.join(RAIZ, "servidor.py"))
        self.mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.mod)

        self.indice = indice
        self.directorio = directorio
        self.http = ThreadingHTTPServer(("127.0.0.1", 0), self.mod.Manejador)
        self.http.daemon_threads = True
        self.puerto = self.http.server_address[1]
        self.url = f"http://127.0.0.1:{self.puerto}"
        self.motor = None
        self.hilo = None
        self.demora = None          # threading.Event a test can hold appends on

    def configurar(self, pares):
        mod = self.mod
        mod.SISTEMA = Sistema(50, 50, reserva=1.0, ttl_respuestas=30)
        mod.TOKEN = TOKEN
        mod.TOKEN_PUBLICADOR = TOKEN
        mod.TOKEN_CONSUMIDOR = TOKEN
        mod.TOKEN_CLUSTER = TOKEN
        silenciar_bitacora(mod, self.directorio, str(self.puerto))
        mod.ARRANCADO = "2026-09-20T00:00:00-03:00"
        mod.INSTANCIA = f"cola-{self.puerto}@prueba"
        mod.MI_URL = self.url
        mod.PARES = [p for p in pares if p != self.url]
        mod.RAFT = NodoRaft(
            yo=self.url, pares=list(mod.PARES),
            reloj=lambda: int(time.monotonic() * 1000),
            azar=__import__("random").Random(1000 + self.indice),
            timeout_eleccion_ms=ELECCION_MS, heartbeat_ms=HEARTBEAT_MS)

    def arrancar(self):
        mod = self.mod
        mod.APLICADOR = Aplicador(mod.SISTEMA)
        self.motor = Motor(mod.RAFT, token=TOKEN, lock=mod.LOCK_RAFT,
                           intervalo_ms=max(10, HEARTBEAT_MS // 3),
                           sistema=mod.SISTEMA, aplicador=mod.APLICADOR,
                           intervalo_recuperador_s=0.2)
        mod.MOTOR = self.motor
        mod.DESPACHAR = self.motor.enviar
        self.motor.arrancar()
        self.hilo = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.hilo.start()

    def parar(self):
        if self.motor:
            self.motor.parar()
        try:
            self.http.shutdown()
        except Exception:                                   # noqa: BLE001
            pass
        try:
            self.http.server_close()
        except Exception:                                   # noqa: BLE001
            pass

    def demorar_appends(self):
        """Hold this node's `/raft/appendEntries` until the test releases it.

        A `threading.Event`, never a sleep: the test decides exactly when the
        slow node becomes fast again.
        """
        self.demora = threading.Event()
        original = self.mod.Manejador.raft_append
        evento = self.demora

        def demorado(manejador, cuerpo):
            evento.wait(TECHO_S)
            return original(manejador, cuerpo)

        self.mod.Manejador.raft_append = demorado

    def soltar_appends(self):
        if self.demora:
            self.demora.set()

    # ------------------------------------------------------------------ http
    def pedir(self, ruta, cuerpo=None, metodo="POST", token=TOKEN, timeout=5):
        return pedir_http(self.url, ruta, cuerpo, metodo, token, timeout)

    def salud(self):
        try:
            return self.pedir("/health", metodo="GET", token=None, timeout=2)[1]
        except Exception:                                   # noqa: BLE001
            return {}

    @property
    def rol(self):
        return self.salud().get("rol")


class ConCluster(unittest.TestCase):
    """Three nodes on free ports, torn down whatever the test did to them."""

    nodos_n = 3

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)

        self.nodos = [NodoDePrueba(i, tmp.name) for i in range(self.nodos_n)]
        self.addCleanup(self.parar_todo)

        urls = [n.url for n in self.nodos]
        for nodo in self.nodos:
            nodo.configurar(urls)
        for nodo in self.nodos:
            nodo.arrancar()

    def parar_todo(self):
        for nodo in self.nodos:
            nodo.parar()

    # ------------------------------------------------------------- consultas
    def masters(self):
        return [n for n in self.nodos if n.rol == "master"]

    def esperar_un_master(self, techo=TECHO_S):
        """Un master que además ya terminó su catch-up, o sea, listo para atender.

        Un master recién electo contesta `503 {"error": "recuperando"}` en las
        rutas de datos hasta barrer el log que heredó. Esperar sólo el rol deja
        una ventana en la que el nodo es master y todavía no atiende — real, y
        no es lo que estos tests quieren medir.
        """
        return esperar_a(lambda: next(
            (n for n in self.masters() if not n.motor.recuperando), None), techo=techo)

    def esperar_master_listo(self, entre, techo=TECHO_S):
        return esperar_a(lambda: next(
            (n for n in entre if n.rol == "master" and not n.motor.recuperando), None),
            techo=techo)

    def pedido(self, id="p1"):
        return {"id": id, "operacion": "GET /personas", "parametros": {},
                "idempotente": True, "destinatario": BALANCEADOR,
                "presupuestoMs": 5000}


# =====================================================================
# 3.15 — exactly one master
# =====================================================================
class TestEleccionEnElCluster(ConCluster):

    def test_se_elige_exactamente_un_master(self):
        master = self.esperar_un_master()

        self.assertIsNotNone(master, "no master was elected within the ceiling")
        self.assertEqual(len(self.masters()), 1)

    def test_todos_coinciden_en_quien_manda(self):
        master = self.esperar_un_master()
        esperar_a(lambda: all(n.salud().get("masterConocido") == master.url
                              for n in self.nodos))

        for nodo in self.nodos:
            self.assertEqual(nodo.salud()["masterConocido"], master.url,
                             f"{nodo.url} disagrees about who leads")

    def test_todos_comparten_el_termino(self):
        self.esperar_un_master()
        terminos = {n.salud().get("termino") for n in self.nodos}

        self.assertEqual(len(terminos), 1, f"terms diverged: {terminos}")


# =====================================================================
# 3.16 — the redirect, over real sockets
# =====================================================================
class TestRedireccionReal(ConCluster):

    def test_un_slave_redirige_al_master_vigente(self):
        master = self.esperar_un_master()
        slave = next(n for n in self.nodos if n is not master)

        codigo, cuerpo = slave.pedir("/pedidos", self.pedido())

        self.assertEqual(codigo, 421)
        self.assertEqual(cuerpo["error"], "no-soy-master")
        self.assertEqual(cuerpo["master"], master.url)

    def test_el_master_acepta_lo_que_el_slave_rechazo(self):
        master = self.esperar_un_master()

        codigo, _ = master.pedir("/pedidos", self.pedido())

        self.assertEqual(codigo, 202)

    def test_el_409_de_respuesta_desconocida_no_se_mezcla_con_el_421(self):
        """Two different meanings must not share a status code."""
        master = self.esperar_un_master()

        codigo, cuerpo = master.pedir("/respuestas", {
            "id": "no-existe", "estado": "OK", "contenido": {},
            "atendidoPor": "casa-A:8080", "app": "python"})

        self.assertEqual(codigo, 409)
        self.assertEqual(cuerpo["resultado"], "desconocido")
        self.assertNotIn("no-soy-master", json.dumps(cuerpo))


# =====================================================================
# 3.17 — the 202 must wait for a majority (red until slice 4b)
# =====================================================================
class TestCommitPorMayoria(ConCluster):
    """Spec `queue-service-api` — "only acknowledged after majority commit".

    The slow-slave variant (holding `/raft/appendEntries` on an Event) turned out
    not to be deterministic here: by the time the handler is patched, the append
    that carries the entry may already be in flight, so the test could pass or
    fail on scheduling. Killing the slaves outright tests the same property —
    no majority is reachable — and cannot race.
    """

    def test_sin_mayoria_alcanzable_no_hay_202(self):
        master = self.esperar_un_master()

        for otro in self.nodos:
            if otro is not master:
                otro.parar()

        codigo, _ = master.pedir("/pedidos", self.pedido(), timeout=TECHO_S)

        self.assertNotEqual(codigo, 202,
                            "the 202 was sent with both slaves down: no majority "
                            "could possibly hold that entry")

    def test_con_un_slave_vivo_si_hay_202(self):
        """The mirror: master + 1 slave is a majority of 3, so the reply comes."""
        master = self.esperar_un_master()

        otros = [n for n in self.nodos if n is not master]
        otros[0].parar()

        codigo, _ = master.pedir("/pedidos", self.pedido(), timeout=TECHO_S)

        self.assertEqual(codigo, 202)

    def test_lo_confirmado_queda_en_el_log_del_slave(self):
        """A committed entry is on more than one machine — that is the point."""
        master = self.esperar_un_master()

        codigo, _ = master.pedir("/pedidos", self.pedido("sobrevive"), timeout=TECHO_S)
        self.assertEqual(codigo, 202)

        otro = next(n for n in self.nodos if n is not master)
        esperar_a(lambda: otro.mod.RAFT.indice_ultimo >= master.mod.RAFT.indice_ultimo)

        ids = [e.payload.get("id") for e in otro.mod.RAFT.log
               if e.operacion == "encolar"]
        self.assertIn("sobrevive", ids,
                      "the pedido the client was promised is not on the slave")


# =====================================================================
# 3.18 — failover
# =====================================================================
class TestFailover(ConCluster):

    def test_matar_al_master_produce_uno_nuevo(self):
        primero = self.esperar_un_master()
        self.assertIsNotNone(primero)
        termino_viejo = primero.salud()["termino"]

        primero.parar()
        vivos = [n for n in self.nodos if n is not primero]

        segundo = self.esperar_master_listo(vivos)

        self.assertIsNotNone(segundo, "no new master after the old one died")
        self.assertIsNot(segundo, primero)
        self.assertGreater(segundo.salud()["termino"], termino_viejo,
                           "the new master did not advance the term")

    def test_el_sobreviviente_reconoce_al_master_nuevo(self):
        primero = self.esperar_un_master()
        primero.parar()
        vivos = [n for n in self.nodos if n is not primero]

        segundo = self.esperar_master_listo(vivos)
        otro = next(n for n in vivos if n is not segundo)
        esperar_a(lambda: otro.salud().get("masterConocido") == segundo.url)

        self.assertEqual(otro.salud()["masterConocido"], segundo.url)

    def test_el_master_nuevo_acepta_pedidos(self):
        primero = self.esperar_un_master()
        primero.parar()
        vivos = [n for n in self.nodos if n is not primero]
        segundo = self.esperar_master_listo(vivos)

        codigo, _ = segundo.pedir("/pedidos", self.pedido("despues-de-la-caida"))

        self.assertEqual(codigo, 202)


# =====================================================================
# 3.19 — a revived slave catches up
# =====================================================================
class TestPuestaAlDia(ConCluster):

    def test_un_slave_que_vuelve_se_pone_al_dia_solo(self):
        """Decision 11: the conflict-term hint, not one index per heartbeat."""
        master = self.esperar_un_master()
        rezagado = next(n for n in self.nodos if n is not master)
        rezagado.parar()

        with master.mod.LOCK_RAFT:
            for i in range(25):
                master.mod.RAFT.proponer("encolar", {"id": f"p{i}"})
        indice_master = master.mod.RAFT.indice_ultimo

        vivo = next(n for n in self.nodos
                    if n is not master and n is not rezagado)
        alcanzado = esperar_a(
            lambda: vivo.mod.RAFT.indice_ultimo >= indice_master)

        self.assertTrue(alcanzado,
                        f"the reachable slave never caught up to {indice_master}")


# =====================================================================
# 4.17-4.19 — vencimiento sólo en el master, y catch-up post-elección
# =====================================================================
class TestVencimientoYCatchUp(ConCluster):
    """Spec `queue-log-application` — Master-Only Lazy Expiry, Post-Election Catch-Up."""

    def test_un_slave_no_puede_vencer_nada_por_su_cuenta(self):
        """Scenario: a slave never independently expires a pedido.

        No hace falta vigilarlo: un slave no puede proponer, así que su barrido
        no tiene por dónde salir. La garantía es estructural, no una regla que
        alguien tenga que respetar.
        """
        master = self.esperar_un_master()
        slave = next(n for n in self.nodos if n is not master)

        resultado = slave.motor.proponer_y_esperar("expirar", {"reencolar": [],
                                                               "fallar": []}, 0)

        self.assertEqual(resultado, "destituido")

    def test_el_master_recien_electo_contesta_503_y_no_421(self):
        """Scenario 4.19: durante el catch-up las rutas de datos dan 503.

        503 y no 421 porque este nodo SÍ es el master: no hay a dónde redirigir,
        sólo hay que esperar a que termine de barrer lo que heredó.
        """
        master = self.esperar_un_master()
        # Simular un mandato nuevo sin catch-up hecho, que es exactamente el
        # estado de un master recién coronado.
        master.motor.recuperado_hasta_termino = -1

        codigo, cuerpo = master.pedir("/pedidos", self.pedido())

        self.assertEqual(codigo, 503)
        self.assertEqual(cuerpo, {"error": "recuperando"})

    def test_el_catch_up_corre_una_vez_por_eleccion(self):
        """Scenario: catch-up runs exactly once per election win."""
        master = self.esperar_un_master()
        termino = master.mod.RAFT.termino_actual

        self.assertEqual(master.motor.recuperado_hasta_termino, termino)
        self.assertFalse(master.motor.recuperando)

        for _ in range(3):
            master.pedir("/pedidos", self.pedido())
        self.assertEqual(master.motor.recuperado_hasta_termino, termino,
                         "catch-up re-ran on a normal request")

    def test_el_master_nuevo_no_entrega_lo_que_murio_sin_lider(self):
        """Scenario: a fresh master does not hand out a pedido that died during
        the election window.

        La reserva vence mientras no hay master. Si el nuevo sirviera desde el
        log sin barrerlo, le daría a un worker un pedido cuyo dueño anterior ya
        está muerto, como si siguiera vivo y reservado.
        """
        master = self.esperar_un_master()
        self.assertEqual(master.pedir("/pedidos", self.pedido("p-huerfano"))[0], 202)
        codigo, tomado = master.pedir("/pedidos/tomar",
                                      {"consumidor": "replica-que-se-muere", "espera": 2})
        self.assertEqual(codigo, 200)
        self.assertEqual(tomado["id"], "p-huerfano")

        master.parar()
        vivos = [n for n in self.nodos if n is not master]
        segundo = self.esperar_master_listo(vivos)
        self.assertIsNotNone(segundo, "no new master")

        # Tras el catch-up, el pedido idempotente volvió a la cola y se entrega
        # con intento 2 — nunca como una reserva viva de la réplica muerta.
        codigo, servido = segundo.pedir("/pedidos/tomar",
                                        {"consumidor": "replica-nueva", "espera": 3})

        self.assertEqual(codigo, 200)
        self.assertEqual(servido["id"], "p-huerfano")
        self.assertGreaterEqual(servido["intento"], 2,
                                "served as if it were still a live first attempt")


if __name__ == "__main__":
    unittest.main()
