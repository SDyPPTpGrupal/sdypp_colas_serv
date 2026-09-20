"""HTTP surface of a clustered node: tokens, `421`, `/raft/*`, `/health`.

RED-first for slice 3a. These cases are written against the routes and fields
described in the `queue-service-api` spec before `servidor.py` grows any of them.

The node is talked to with plain `urllib`, exactly as the worker team's client
will, rather than through a client library that lives in the other repository —
so this file has no dependency outside this repo.

No test here asserts that something happened *within* a wall-clock duration.
Cluster state is forced by driving `servidor.RAFT` directly, never by waiting.
"""

import json
import os
import sys
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import servidor  # noqa: E402
from aplicar import Aplicador  # noqa: E402
from colas import Sistema  # noqa: E402
from motor import Motor  # noqa: E402
from raft import NodoRaft  # noqa: E402

from ayuda import pedir_http, silenciar_bitacora  # noqa: E402

BALANCEADOR = "balanceador@casa-tomas"

PUBLICADOR = "token-publicador"
CONSUMIDOR = "token-consumidor"
CLUSTER = "token-cluster"

RUTAS_DE_DATOS = (
    "/pedidos",
    "/pedidos/tomar",
    "/pedidos/devolver",
    "/respuestas",
    "/respuestas/tomar",
)


class ConNodo(unittest.TestCase):
    """Bring a node up on a free port, with the cluster knobs patchable."""

    pares = ["http://127.0.0.1:9101", "http://127.0.0.1:9102"]

    def setUp(self):
        servidor.SISTEMA = Sistema(10, 10, reserva=0.2, ttl_respuestas=5)
        servidor.TOKEN_PUBLICADOR = PUBLICADOR
        servidor.TOKEN_CONSUMIDOR = CONSUMIDOR
        servidor.TOKEN_CLUSTER = CLUSTER
        servidor.INSTANCIA = "cola-test@casa-tomas"

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        silenciar_bitacora(servidor, tmp.name, "prueba")

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), servidor.Manejador)
        self.http.daemon_threads = True
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.addCleanup(self.http.server_close)
        self.addCleanup(self.http.shutdown)
        self.url = f"http://127.0.0.1:{self.http.server_address[1]}"

        servidor.MI_URL = self.url
        servidor.PARES = list(self.pares)
        servidor.RAFT = NodoRaft(
            yo=self.url, pares=list(self.pares),
            reloj=lambda: 0, azar=_AzarFijo(),
            timeout_eleccion_ms=150, heartbeat_ms=50)
        servidor.APLICADOR = Aplicador(servidor.SISTEMA)
        servidor.MOTOR = Motor(servidor.RAFT, lock=servidor.LOCK_RAFT,
                               sistema=servidor.SISTEMA,
                               aplicador=servidor.APLICADOR)
        servidor.DESPACHAR = servidor.MOTOR.enviar
        # Sin ticker: estos tests fijan el rol a mano y una elección se lo
        # pisaría. El aplicador sí corre, porque es quien escribe el estado.
        servidor.MOTOR.arrancar(reloj=False)
        self.addCleanup(servidor.MOTOR.parar)
        self.marcar_recuperado()

    # ------------------------------------------------------------------ http
    def pedir(self, ruta, cuerpo=None, token=None, metodo="POST"):
        return pedir_http(self.url, ruta, cuerpo, metodo, token, timeout=5)

    def obtener(self, ruta, token=None):
        return self.pedir(ruta, token=token, metodo="GET")

    # ------------------------------------------------------------ raft state
    def marcar_recuperado(self):
        """Dar el catch-up por hecho para el mandato en curso.

        Sin el ticker nadie lo corre, y un master que no barrió su log contesta
        `503 recuperando` en las rutas de datos — correcto en producción, pero
        acá el escenario es un master ya operativo.
        """
        servidor.MOTOR.recuperado_hasta_termino = servidor.RAFT.termino_actual

    def hacerme_master(self):
        servidor.RAFT.rol = "master"
        servidor.RAFT.master_conocido = self.url
        servidor.RAFT.termino_actual = 4
        self.marcar_recuperado()

    def hacerme_slave(self, master="http://127.0.0.1:9101"):
        servidor.RAFT.rol = "slave"
        servidor.RAFT.master_conocido = master
        servidor.RAFT.termino_actual = 4

    def hacerme_candidato(self):
        servidor.RAFT.rol = "candidato"
        servidor.RAFT.master_conocido = None
        servidor.RAFT.termino_actual = 5

    def cuerpo_valido(self, ruta):
        """A well-formed body per route, so a 421 cannot be confused with a 400."""
        if ruta == "/pedidos":
            return {"id": "p1", "operacion": "GET /personas", "parametros": {},
                    "destinatario": BALANCEADOR, "presupuestoMs": 5000}
        if ruta == "/pedidos/tomar":
            return {"consumidor": "casa-A:8080", "espera": 0}
        if ruta == "/pedidos/devolver":
            return {"id": "p1", "consumidor": "casa-A:8080"}
        if ruta == "/respuestas":
            return {"id": "p1", "estado": "OK", "contenido": {},
                    "atendidoPor": "casa-A:8080", "app": "python"}
        return {"destinatario": BALANCEADOR, "espera": 0}

    def token_de(self, ruta):
        return PUBLICADOR if ruta in ("/pedidos", "/respuestas/tomar") else CONSUMIDOR


class _AzarFijo:
    """Deterministic stand-in for `random.Random` — no jitter in HTTP tests."""

    def randint(self, a, b):
        return a


# =====================================================================
# 3.1 — the cluster protocol is not open to outsiders
# =====================================================================
class TestTokenDeCluster(ConNodo):
    """Spec `queue-service-api` — Cluster-Internal Routes."""

    def test_raft_rechaza_token_de_publicador_y_de_consumidor(self):
        """Scenario: an outside caller cannot join the cluster protocol."""
        for ruta in ("/raft/appendEntries", "/raft/requestVote"):
            for token in (PUBLICADOR, CONSUMIDOR, None):
                with self.subTest(ruta=ruta, token=token):
                    codigo, cuerpo = self.pedir(ruta, {"termino": 99}, token=token)
                    self.assertEqual(codigo, 403)
                    self.assertEqual(cuerpo, {"error": "token inválido"})

    def test_un_raft_rechazado_no_toca_el_termino(self):
        """A refused call must not be processed as a valid Raft message."""
        antes = servidor.RAFT.termino_actual

        for token in (PUBLICADOR, CONSUMIDOR, None):
            self.pedir("/raft/requestVote", {
                "termino": 99, "candidato": "http://intruso:8085",
                "ultimoIndiceLog": 0, "ultimoTerminoLog": 0}, token=token)

        self.assertEqual(servidor.RAFT.termino_actual, antes,
                         "an unauthenticated message moved the term")

    def test_raft_estado_pide_token_de_cluster(self):
        self.assertEqual(self.obtener("/raft/estado", token=CONSUMIDOR)[0], 403)
        self.assertEqual(self.obtener("/raft/estado", token=CLUSTER)[0], 200)

    def test_raft_estado_expone_rol_y_termino(self):
        self.hacerme_master()
        codigo, cuerpo = self.obtener("/raft/estado", token=CLUSTER)

        self.assertEqual(codigo, 200)
        for clave in ("rol", "termino", "indiceLog", "indiceCommit"):
            self.assertIn(clave, cuerpo)


# =====================================================================
# 3.2 — one token class per route
# =====================================================================
class TestSeparacionDeTokens(ConNodo):
    """Spec `queue-service-api` — Three-Way Token Authorization Split."""

    def test_el_consumidor_no_puede_publicar(self):
        """Scenario: a consumer token cannot publish."""
        self.hacerme_master()
        codigo, cuerpo = self.pedir("/pedidos", self.cuerpo_valido("/pedidos"),
                                    token=CONSUMIDOR)

        self.assertEqual(codigo, 403)
        self.assertEqual(cuerpo, {"error": "token inválido"})

    def test_el_publicador_no_puede_consumir(self):
        self.hacerme_master()
        for ruta in ("/pedidos/tomar", "/pedidos/devolver", "/respuestas"):
            with self.subTest(ruta=ruta):
                codigo, _ = self.pedir(ruta, self.cuerpo_valido(ruta), token=PUBLICADOR)
                self.assertEqual(codigo, 403)

    def test_el_consumidor_no_puede_recolectar_respuestas(self):
        self.hacerme_master()
        codigo, _ = self.pedir("/respuestas/tomar",
                               self.cuerpo_valido("/respuestas/tomar"), token=CONSUMIDOR)

        self.assertEqual(codigo, 403)

    def test_el_token_de_cluster_no_abre_rutas_de_datos(self):
        self.hacerme_master()
        for ruta in RUTAS_DE_DATOS:
            with self.subTest(ruta=ruta):
                codigo, _ = self.pedir(ruta, self.cuerpo_valido(ruta), token=CLUSTER)
                self.assertEqual(codigo, 403)

    def test_cada_ruta_acepta_su_propio_token(self):
        """The mirror of the above: the right token is not rejected."""
        self.hacerme_master()
        for ruta in RUTAS_DE_DATOS:
            with self.subTest(ruta=ruta):
                codigo, _ = self.pedir(ruta, self.cuerpo_valido(ruta),
                                       token=self.token_de(ruta))
                self.assertNotEqual(codigo, 403)


# =====================================================================
# 3.3 — the wrong-node redirect
# =====================================================================
class TestRedireccion421(ConNodo):
    """Spec `queue-service-api` — Wrong-Node Redirect on Every Data Route."""

    def test_un_slave_redirige_con_la_url_del_master(self):
        """Scenario: a slave rejects a write with 421 and a known master."""
        self.hacerme_slave(master="http://127.0.0.1:9101")

        for ruta in RUTAS_DE_DATOS:
            with self.subTest(ruta=ruta):
                codigo, cuerpo = self.pedir(ruta, self.cuerpo_valido(ruta),
                                            token=self.token_de(ruta))
                self.assertEqual(codigo, 421)
                self.assertEqual(cuerpo, {"error": "no-soy-master",
                                          "master": "http://127.0.0.1:9101"})

    def test_el_slave_no_muta_nada_al_redirigir(self):
        """"MUST NOT apply any mutation before replying 421."""
        self.hacerme_slave()
        antes = servidor.SISTEMA.estado()

        self.pedir("/pedidos", self.cuerpo_valido("/pedidos"), token=PUBLICADOR)

        self.assertEqual(servidor.SISTEMA.estado(), antes)

    def test_sin_master_conocido_el_421_lleva_null(self):
        """Scenario: a node rejects with 421 and a null master during an election."""
        self.hacerme_candidato()

        codigo, cuerpo = self.pedir("/pedidos", self.cuerpo_valido("/pedidos"),
                                    token=PUBLICADOR)

        self.assertEqual(codigo, 421)
        self.assertEqual(cuerpo, {"error": "no-soy-master", "master": None})

    def test_el_421_no_aparece_en_rutas_que_no_son_de_datos(self):
        """Scenario: 421 never appears on a non-data route."""
        self.hacerme_slave()

        self.assertEqual(self.obtener("/health")[0], 200)
        self.assertEqual(self.obtener("/health/vivo")[0], 200)
        self.assertEqual(self.obtener("/raft/estado", token=CLUSTER)[0], 200)
        for ruta in ("/raft/appendEntries", "/raft/requestVote"):
            with self.subTest(ruta=ruta):
                codigo, _ = self.pedir(ruta, {"termino": 1, "master": "x",
                                              "indicePrevio": 0, "terminoPrevio": 0,
                                              "entradas": [], "indiceCommit": 0,
                                              "candidato": "x", "ultimoIndiceLog": 0,
                                              "ultimoTerminoLog": 0}, token=CLUSTER)
                self.assertNotEqual(codigo, 421)

    def test_el_master_no_redirige(self):
        self.hacerme_master()

        codigo, _ = self.pedir("/pedidos", self.cuerpo_valido("/pedidos"), token=PUBLICADOR)

        self.assertNotEqual(codigo, 421)


# =====================================================================
# 3.4 — health and liveness
# =====================================================================
class TestSalud(ConNodo):
    """Spec `queue-service-api` — /health and /health/vivo."""

    def test_health_informa_rol_termino_y_master(self):
        """Scenario: a slave reports the master it knows about."""
        self.hacerme_slave(master="http://127.0.0.1:9101")

        codigo, cuerpo = self.obtener("/health")

        self.assertEqual(codigo, 200)
        self.assertEqual(cuerpo["rol"], "slave")
        self.assertEqual(cuerpo["termino"], 4)
        self.assertEqual(cuerpo["masterConocido"], "http://127.0.0.1:9101")
        self.assertEqual(cuerpo["contrato"], "1.0")
        self.assertEqual(cuerpo["instancia"], "cola-test@casa-tomas")

    def test_health_conserva_los_campos_de_profundidad(self):
        """The pre-existing fields are additive, not replaced."""
        self.hacerme_master()
        _, cuerpo = self.obtener("/health")

        for clave in ("cola", "esperando", "enVuelo", "cota"):
            self.assertIn(clave, cuerpo)

    def test_en_eleccion_el_master_conocido_es_null(self):
        """Scenario: an electing cluster reports no known master."""
        self.hacerme_candidato()

        _, cuerpo = self.obtener("/health")

        self.assertIsNone(cuerpo["masterConocido"])
        self.assertEqual(cuerpo["rol"], "candidato")

    def test_health_no_pide_token(self):
        """Scenario: health routes require no token."""
        self.assertEqual(self.obtener("/health")[0], 200)
        self.assertEqual(self.obtener("/health/vivo")[0], 200)

    def test_vivo_responde_aunque_no_haya_master(self):
        """Scenario: a leaderless node still reports alive."""
        self.hacerme_candidato()

        codigo, cuerpo = self.obtener("/health/vivo")

        self.assertEqual(codigo, 200)
        self.assertEqual(cuerpo, {"vivo": True},
                         "the liveness body must be exactly this shape")


# =====================================================================
# 3.5 — single-node mode
# =====================================================================
class TestNodoSolo(ConNodo):
    """Spec `queue-service-api` — Single-Node Mode.

    A lone node is a majority of one. It is master from the start, and the data
    routes behave byte for byte as they did before any of this existed.
    """

    pares = []

    def test_un_nodo_solo_es_master_de_entrada(self):
        self.assertEqual(servidor.RAFT.rol, "master")
        self.assertEqual(servidor.RAFT.mayoria, 1)

        _, cuerpo = self.obtener("/health")
        self.assertEqual(cuerpo["rol"], "master")

    def test_un_nodo_solo_nunca_redirige(self):
        """Scenario: a lone node never redirects."""
        for ruta in RUTAS_DE_DATOS:
            with self.subTest(ruta=ruta):
                codigo, _ = self.pedir(ruta, self.cuerpo_valido(ruta),
                                       token=self.token_de(ruta))
                self.assertNotEqual(codigo, 421)

    def test_el_ciclo_completo_anda_sin_cluster(self):
        """The pre-clustering behaviour, end to end, on a lone node."""
        codigo, _ = self.pedir("/pedidos", self.cuerpo_valido("/pedidos"),
                               token=PUBLICADOR)
        self.assertEqual(codigo, 202)

        codigo, pedido = self.pedir("/pedidos/tomar",
                                    {"consumidor": "casa-A:8080", "espera": 0},
                                    token=CONSUMIDOR)
        self.assertEqual(codigo, 200)
        self.assertEqual(pedido["id"], "p1")
        self.assertIn("quedaMs", pedido)

        codigo, _ = self.pedir("/respuestas", {
            "id": "p1", "estado": "OK", "contenido": {"ok": True},
            "atendidoPor": "casa-A:8080", "app": "python"}, token=CONSUMIDOR)
        self.assertEqual(codigo, 202)

        codigo, respuesta = self.pedir("/respuestas/tomar",
                                       {"destinatario": BALANCEADOR, "espera": 0},
                                       token=PUBLICADOR)
        self.assertEqual(codigo, 200)
        self.assertEqual(respuesta["id"], "p1")

    def test_la_cola_vacia_sigue_contestando_204(self):
        """`204` on an empty pull is pre-existing behaviour and must survive."""
        codigo, _ = self.pedir("/pedidos/tomar",
                               {"consumidor": "casa-A:8080", "espera": 0},
                               token=CONSUMIDOR)

        self.assertEqual(codigo, 204)


if __name__ == "__main__":
    unittest.main()
