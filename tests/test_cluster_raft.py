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
        self.motor = Motor(mod.RAFT, token=TOKEN, lock=mod.LOCK_RAFT,
                           intervalo_ms=max(10, HEARTBEAT_MS // 3))
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
        return esperar_a(lambda: (self.masters() or [None])[0], techo=techo)

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

    @unittest.expectedFailure
    def test_el_202_no_sale_antes_de_la_mayoria(self):
        """Spec `queue-service-api` — "only acknowledged after majority commit".

        EXPECTED RED UNTIL SLICE 4b. Today `POST /pedidos` answers as soon as the
        master has enqueued locally: the propose-and-wait that gates the reply on
        `indiceCommit` is `motor.py`'s half of slice 4. Until then the `202` is a
        promise the cluster has not yet made, and this test is the reminder.
        """
        master = self.esperar_un_master()
        for otro in self.nodos:
            if otro is not master:
                otro.demorar_appends()
        self.addCleanup(lambda: [n.soltar_appends() for n in self.nodos])

        respondio = threading.Event()

        def publicar():
            master.pedir("/pedidos", self.pedido(), timeout=TECHO_S)
            respondio.set()

        threading.Thread(target=publicar, daemon=True).start()

        llego_temprano = respondio.wait(1.0)
        self.assertFalse(llego_temprano,
                         "the 202 was sent before any slave acknowledged")


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

        segundo = esperar_a(lambda: next((n for n in vivos if n.rol == "master"), None))

        self.assertIsNotNone(segundo, "no new master after the old one died")
        self.assertIsNot(segundo, primero)
        self.assertGreater(segundo.salud()["termino"], termino_viejo,
                           "the new master did not advance the term")

    def test_el_sobreviviente_reconoce_al_master_nuevo(self):
        primero = self.esperar_un_master()
        primero.parar()
        vivos = [n for n in self.nodos if n is not primero]

        segundo = esperar_a(lambda: next((n for n in vivos if n.rol == "master"), None))
        otro = next(n for n in vivos if n is not segundo)
        esperar_a(lambda: otro.salud().get("masterConocido") == segundo.url)

        self.assertEqual(otro.salud()["masterConocido"], segundo.url)

    def test_el_master_nuevo_acepta_pedidos(self):
        primero = self.esperar_un_master()
        primero.parar()
        vivos = [n for n in self.nodos if n is not primero]
        segundo = esperar_a(lambda: next((n for n in vivos if n.rol == "master"), None))

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


if __name__ == "__main__":
    unittest.main()
