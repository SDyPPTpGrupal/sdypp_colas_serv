"""Deterministic tests for the Raft-lite state machine (`raft.py`).

RED-first: this file is written against the `NodoRaft` interface described in
Design Decision 4 before `raft.py` exists. Running it now must fail at import.

Flakiness controls, enforced as hard rules rather than hopes:

  * This module MUST NOT import `time` and MUST NOT call `sleep` anywhere.
    All timing is `ClusterFalso.avanzar(ms)` against a fake clock.
  * Election jitter comes from an injected, seeded `random.Random`, so every
    schedule is reproducible byte for byte from its seed.
  * Messages are *returned* by the state machine, never sent. "Deliver nothing"
    is therefore a first-class operation, which is how partitions are simulated.
"""

import random
import unittest
from collections import deque

from raft import Entrada, NodoRaft

TIMEOUT_ELECCION_MS = 150
HEARTBEAT_MS = 50


def entrada(indice, termino, operacion="encolar", payload=None):
    """Build a log entry without repeating the keyword soup in every test."""
    return Entrada(indice=indice, termino=termino, operacion=operacion,
                   payload=payload if payload is not None else {})


class ClusterFalso:
    """N `NodoRaft` over an in-memory bus. No threads, no sockets, no real time.

    The bus holds `(origen, mensaje)` pairs. `entregar()` moves them into the
    destination node and queues whatever that node returns. A partition is a set
    of forbidden unordered pairs, so a dropped message is simply never delivered
    — which is exactly what a network partition looks like to a Raft node.
    """

    def __init__(self, n, semilla=1234):
        self.semilla = semilla
        self.ahora_ms = 0
        self.ids = [f"n{i}" for i in range(n)]
        self.bus = deque()
        self.cortes = set()
        self.caidos = set()
        self.nodos = {}
        for i, yo in enumerate(self.ids):
            pares = [otro for otro in self.ids if otro != yo]
            self.nodos[yo] = NodoRaft(
                yo=yo,
                pares=pares,
                reloj=lambda: self.ahora_ms,
                azar=random.Random(semilla + i),
                timeout_eleccion_ms=TIMEOUT_ELECCION_MS,
                heartbeat_ms=HEARTBEAT_MS,
            )

    # ------------------------------------------------------------------ clock
    def avanzar(self, ms, nodos=None):
        """Step the fake clock and tick each node, queueing what they emit."""
        self.ahora_ms += ms
        for yo in (nodos if nodos is not None else self.ids):
            if yo in self.caidos:
                continue
            self._encolar(yo, self.nodos[yo].tic(self.ahora_ms))

    def forzar_eleccion(self, nodo):
        """Advance past the timeout but tick ONLY `nodo`, so it alone wakes up."""
        self.ahora_ms += TIMEOUT_ELECCION_MS * 4
        self._encolar(nodo, self.nodos[nodo].tic(self.ahora_ms))

    # -------------------------------------------------------------------- bus
    def _encolar(self, origen, mensajes):
        for mensaje in mensajes or []:
            self.bus.append((origen, mensaje))

    def _cortado(self, a, b):
        return frozenset((a, b)) in self.cortes

    def entregar(self, veces=None):
        """Deliver queued messages, honouring the partition and downed nodes.

        `veces=None` drains the bus as it stands; replies produced along the way
        land behind the current batch and are delivered on the next call, so a
        single `entregar()` is one network round, not an unbounded cascade.
        """
        ronda = len(self.bus) if veces is None else min(veces, len(self.bus))
        for _ in range(ronda):
            origen, mensaje = self.bus.popleft()
            destino = mensaje.destino
            if destino in self.caidos or origen in self.caidos:
                continue
            if self._cortado(origen, destino):
                continue
            self._entregar_uno(origen, destino, mensaje)

    def _entregar_uno(self, origen, destino, mensaje):
        nodo = self.nodos[destino]
        if mensaje.tipo == "solicitud_voto":
            respuesta, salientes = nodo.recibir_solicitud_voto(mensaje.cuerpo)
            self._responder(destino, origen, "respuesta_voto", respuesta)
        elif mensaje.tipo == "respuesta_voto":
            salientes = nodo.recibir_respuesta_voto(origen, mensaje.cuerpo)
        elif mensaje.tipo == "append":
            respuesta, salientes = nodo.recibir_append(mensaje.cuerpo)
            self._responder(destino, origen, "respuesta_append", respuesta)
        elif mensaje.tipo == "respuesta_append":
            salientes = nodo.recibir_respuesta_append(origen, mensaje.cuerpo)
        else:
            raise AssertionError(f"unknown message type: {mensaje.tipo}")
        self._encolar(destino, salientes)

    def _responder(self, de, para, tipo, cuerpo):
        if cuerpo is None:
            return
        self.bus.append((de, _MensajeFalso(destino=para, tipo=tipo, cuerpo=cuerpo)))

    # ------------------------------------------------------------- partitions
    def particionar(self, grupo_a, grupo_b):
        for a in grupo_a:
            for b in grupo_b:
                self.cortes.add(frozenset((a, b)))

    def sanar(self):
        self.cortes.clear()

    def matar(self, nodo):
        """Stop ticking and delivering to `nodo`; its state is kept, as a zombie's is."""
        self.caidos.add(nodo)

    def revivir(self, nodo):
        self.caidos.discard(nodo)

    # ----------------------------------------------------------- convergence
    def estabilizar(self, ms=2000):
        """Alternate avanzar/entregar until the bus is quiet or the budget runs out."""
        restante = ms
        while restante > 0:
            self.avanzar(HEARTBEAT_MS)
            restante -= HEARTBEAT_MS
            for _ in range(6):
                if not self.bus:
                    break
                self.entregar()
            if not self.bus and self.masters():
                return

    def bombear(self, veces, nodos=None):
        """Heartbeat rounds that tick only `nodos` (the master, typically).

        Ticking an isolated slave would start an election that is not what the
        scenario is about; slaves still reply, since replying needs no tick.
        """
        for _ in range(veces):
            self.avanzar(HEARTBEAT_MS, nodos=nodos)
            self.entregar()
            self.entregar()

    def masters(self):
        return [yo for yo in self.ids
                if yo not in self.caidos and self.nodos[yo].rol == "master"]

    # ----------------------------------------------------------------- state
    def sembrar(self, nodo, termino, entradas):
        """Plant a log and a term on a node, to build the pre-conditions a
        scenario describes without replaying the whole history that produced it."""
        objetivo = self.nodos[nodo]
        objetivo.termino_actual = termino
        for e in entradas:
            objetivo.log.append(e)

    def elegir_master(self, nodo):
        """Drive `nodo` to master through a real election, not by assignment."""
        self.forzar_eleccion(nodo)
        for _ in range(6):
            self.entregar()
        return self.nodos[nodo].rol == "master"


class _MensajeFalso:
    """Stand-in envelope for replies, which travel the same bus as requests."""

    __slots__ = ("destino", "tipo", "cuerpo")

    def __init__(self, destino, tipo, cuerpo):
        self.destino = destino
        self.tipo = tipo
        self.cuerpo = cuerpo


# =====================================================================
# 2.3 — majority election
# =====================================================================
class TestEleccionPorMayoria(unittest.TestCase):
    """Spec `queue-replication` — Election with Majority and Log-Freshness Vote Rule."""

    def test_nodo_nuevo_arranca_como_slave(self):
        """Scenario: a freshly started node with a seed list of size N>1 starts as slave."""
        cluster = ClusterFalso(3)
        for yo in cluster.ids:
            self.assertEqual(cluster.nodos[yo].rol, "slave")
            self.assertEqual(cluster.nodos[yo].termino_actual, 0)

    def test_mayoria_en_3_nodos(self):
        """Scenario: majority election succeeds at 3 nodes — 2 of 3 suffices."""
        cluster = ClusterFalso(3)
        cluster.forzar_eleccion("n0")
        cluster.entregar()   # requestVote out
        cluster.entregar()   # votes back

        self.assertEqual(cluster.nodos["n0"].rol, "master")
        self.assertEqual(cluster.masters(), ["n0"])

    def test_los_demas_pasan_a_slave_bajo_el_termino_nuevo(self):
        """Scenario: the other two nodes observe the new master's term."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        termino = cluster.nodos["n0"].termino_actual

        for otro in ("n1", "n2"):
            self.assertEqual(cluster.nodos[otro].rol, "slave")
            self.assertEqual(cluster.nodos[otro].termino_actual, termino)

    def test_mayoria_en_5_nodos(self):
        """Scenario: majority election succeeds at 5 nodes — 3 of 5, so 2 extra votes."""
        cluster = ClusterFalso(5)
        cluster.forzar_eleccion("n0")
        cluster.entregar()
        cluster.entregar()

        self.assertEqual(cluster.nodos["n0"].rol, "master")
        self.assertEqual(cluster.nodos["n0"].mayoria, 3)

    def test_mayoria_de_uno_en_nodo_solo(self):
        """Interfaces: `mayoria = (len(pares) + 1) // 2 + 1`; with one node that is 1."""
        cluster = ClusterFalso(1)
        self.assertEqual(cluster.nodos["n0"].mayoria, 1)

        cluster.forzar_eleccion("n0")
        self.assertEqual(cluster.nodos["n0"].rol, "master")


# =====================================================================
# 2.4 — the log-freshness vote rule
# =====================================================================
class TestReglaDeFrescuraDelLog(unittest.TestCase):
    """A candidate that would lose committed entries must not be able to win."""

    def test_candidato_con_termino_viejo_no_gana(self):
        """Scenario: candidate with a stale log cannot win.

        A's log ends at {indice: 5, termino: 3}; B's at {indice: 6, termino: 4}.
        B must refuse, because A's last log term is lower.
        """
        cluster = ClusterFalso(3)
        cluster.sembrar("n0", termino=4, entradas=[entrada(i, 3) for i in range(1, 6)])
        cluster.sembrar("n1", termino=4, entradas=[entrada(i, 3) for i in range(1, 6)]
                        + [entrada(6, 4)])

        respuesta, _ = cluster.nodos["n1"].recibir_solicitud_voto({
            "termino": 5, "candidato": "n0",
            "ultimoIndiceLog": 5, "ultimoTerminoLog": 3,
        })

        self.assertFalse(respuesta["votoConcedido"])

    def test_candidato_con_log_mas_corto_pierde_el_desempate(self):
        """Scenario: equally fresh but shorter log loses the tiebreak.

        Same last term (4), candidate at index 5, voter at index 6: index decides.
        """
        cluster = ClusterFalso(3)
        cluster.sembrar("n1", termino=4,
                        entradas=[entrada(i, 4) for i in range(1, 7)])

        respuesta, _ = cluster.nodos["n1"].recibir_solicitud_voto({
            "termino": 5, "candidato": "n0",
            "ultimoIndiceLog": 5, "ultimoTerminoLog": 4,
        })

        self.assertFalse(respuesta["votoConcedido"])

    def test_candidato_al_dia_si_gana_el_voto(self):
        """The mirror case: an equal log is 'at least as up to date', so it is granted."""
        cluster = ClusterFalso(3)
        cluster.sembrar("n1", termino=4,
                        entradas=[entrada(i, 4) for i in range(1, 7)])

        respuesta, _ = cluster.nodos["n1"].recibir_solicitud_voto({
            "termino": 5, "candidato": "n0",
            "ultimoIndiceLog": 6, "ultimoTerminoLog": 4,
        })

        self.assertTrue(respuesta["votoConcedido"])

    def test_candidato_atrasado_no_junta_mayoria_en_el_cluster(self):
        """End to end: the stale node loses the election, it does not merely lose a vote."""
        cluster = ClusterFalso(3)
        for al_dia in ("n1", "n2"):
            cluster.sembrar(al_dia, termino=4, entradas=[entrada(i, 4) for i in range(1, 7)])
        cluster.sembrar("n0", termino=4, entradas=[entrada(i, 3) for i in range(1, 4)])

        cluster.forzar_eleccion("n0")
        for _ in range(4):
            cluster.entregar()

        self.assertNotEqual(cluster.nodos["n0"].rol, "master")
        self.assertEqual(cluster.masters(), [])


# =====================================================================
# 2.5 — one vote per term, and monotonic terms
# =====================================================================
class TestVotoUnicoYTerminoMonotono(unittest.TestCase):

    def test_un_voto_por_termino(self):
        """Scenario: a node votes at most once per term."""
        cluster = ClusterFalso(3)
        votante = cluster.nodos["n2"]

        primera, _ = votante.recibir_solicitud_voto({
            "termino": 5, "candidato": "n0", "ultimoIndiceLog": 0, "ultimoTerminoLog": 0})
        segunda, _ = votante.recibir_solicitud_voto({
            "termino": 5, "candidato": "n1", "ultimoIndiceLog": 0, "ultimoTerminoLog": 0})

        self.assertTrue(primera["votoConcedido"])
        self.assertFalse(segunda["votoConcedido"])
        self.assertEqual(votante.voto_para, "n0")

    def test_repetir_el_pedido_del_mismo_candidato_sigue_concediendo(self):
        """`voto_para in (None, candidato)`: a retransmitted request is idempotent."""
        cluster = ClusterFalso(3)
        votante = cluster.nodos["n2"]
        cuerpo = {"termino": 5, "candidato": "n0",
                  "ultimoIndiceLog": 0, "ultimoTerminoLog": 0}

        primera, _ = votante.recibir_solicitud_voto(dict(cuerpo))
        repetida, _ = votante.recibir_solicitud_voto(dict(cuerpo))

        self.assertTrue(primera["votoConcedido"])
        self.assertTrue(repetida["votoConcedido"])

    def test_el_termino_nunca_baja(self):
        """Scenario: term only increases, never decreases."""
        cluster = ClusterFalso(3)
        nodo = cluster.nodos["n0"]
        nodo.termino_actual = 7

        nodo.recibir_append({"termino": 3, "master": "n1", "indicePrevio": 0,
                             "terminoPrevio": 0, "entradas": [], "indiceCommit": 0})
        nodo.recibir_solicitud_voto({"termino": 6, "candidato": "n1",
                                     "ultimoIndiceLog": 0, "ultimoTerminoLog": 0})

        self.assertEqual(nodo.termino_actual, 7)

    def test_no_concede_voto_para_un_termino_menor_o_igual(self):
        """A vote for a term that is not strictly ahead must be refused."""
        cluster = ClusterFalso(3)
        nodo = cluster.nodos["n0"]
        nodo.termino_actual = 7
        nodo.voto_para = "n2"

        respuesta, _ = nodo.recibir_solicitud_voto({
            "termino": 7, "candidato": "n1", "ultimoIndiceLog": 0, "ultimoTerminoLog": 0})

        self.assertFalse(respuesta["votoConcedido"])


# =====================================================================
# 2.6 — term fencing and the no-two-masters invariant
# =====================================================================
class TestFencingYSplitBrain(unittest.TestCase):
    """Spec `queue-replication` — Term Fencing (No Split-Brain)."""

    def test_master_ve_termino_mayor_y_se_degrada(self):
        """Scenario: a master sees a higher term and steps down."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        master = cluster.nodos["n0"]
        termino_nuevo = master.termino_actual + 1

        master.recibir_append({"termino": termino_nuevo, "master": "n1",
                               "indicePrevio": 0, "terminoPrevio": 0,
                               "entradas": [], "indiceCommit": 0})

        self.assertEqual(master.rol, "slave")
        self.assertEqual(master.termino_actual, termino_nuevo)
        self.assertIsNone(master.voto_para)

    def test_el_fencing_tambien_aplica_por_solicitud_de_voto(self):
        """`_fencing` is the first statement of all four `recibir_*`, not just append."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        master = cluster.nodos["n0"]
        termino_nuevo = master.termino_actual + 3

        master.recibir_solicitud_voto({"termino": termino_nuevo, "candidato": "n2",
                                       "ultimoIndiceLog": 0, "ultimoTerminoLog": 0})

        self.assertEqual(master.rol, "slave")
        self.assertEqual(master.termino_actual, termino_nuevo)

    def test_minoria_aislada_no_puede_comprometer(self):
        """Scenario: no two masters coexist during an active partition.

        The isolated old master keeps believing it leads, but cannot commit:
        one node is not a majority of three.
        """
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        cluster.particionar(["n0"], ["n1", "n2"])

        commit_antes = cluster.nodos["n0"].indice_commit
        cluster.nodos["n0"].proponer("encolar", {"id": "p1"})
        for _ in range(8):
            cluster.avanzar(HEARTBEAT_MS)
            cluster.entregar()

        self.assertEqual(cluster.nodos["n0"].indice_commit, commit_antes)

    def test_la_mayoria_elige_master_nuevo_durante_la_particion(self):
        """Scenario: the majority side must elect a new master within its timeout."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        cluster.particionar(["n0"], ["n1", "n2"])

        cluster.forzar_eleccion("n1")
        for _ in range(4):
            cluster.entregar()

        self.assertEqual(cluster.nodos["n1"].rol, "master")

    def test_al_sanar_la_particion_el_master_viejo_se_degrada(self):
        """Scenario: no two masters coexist under a healed partition."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        cluster.particionar(["n0"], ["n1", "n2"])
        cluster.forzar_eleccion("n1")
        for _ in range(4):
            cluster.entregar()

        cluster.sanar()
        for _ in range(8):
            cluster.avanzar(HEARTBEAT_MS)
            cluster.entregar()
            self.assertLessEqual(len(cluster.masters()), 1,
                                 "two masters coexisted while the partition healed")

        self.assertEqual(cluster.masters(), ["n1"])
        self.assertEqual(cluster.nodos["n0"].rol, "slave")

    def test_master_zombi_revive_se_degrada_y_no_compromete_nada(self):
        """The explicit zombie case: revived old master, higher term on first contact."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        termino_viejo = cluster.nodos["n0"].termino_actual

        cluster.matar("n0")
        cluster.forzar_eleccion("n1")
        for _ in range(4):
            cluster.entregar()
        self.assertEqual(cluster.nodos["n1"].rol, "master")

        cluster.revivir("n0")
        zombi = cluster.nodos["n0"]
        self.assertEqual(zombi.rol, "master")          # still believes it leads
        self.assertEqual(zombi.termino_actual, termino_viejo)

        commit_antes = zombi.indice_commit
        zombi.proponer("encolar", {"id": "fantasma"})
        for _ in range(10):
            cluster.avanzar(HEARTBEAT_MS)
            cluster.entregar()
            self.assertLessEqual(len(cluster.masters()), 1,
                                 "the zombie and the new master were both master")

        self.assertEqual(zombi.rol, "slave")
        self.assertEqual(zombi.indice_commit, commit_antes)

    def test_invariante_de_seguridad_sobre_un_schedule_sembrado(self):
        """Property-style: never two masters, over 200 seeded steps of chaos.

        The seed is printed on failure so the exact schedule can be replayed.
        """
        semilla = 20260920
        azar = random.Random(semilla)
        cluster = ClusterFalso(3, semilla=semilla)

        for paso in range(200):
            accion = azar.choice(
                ["avanzar", "avanzar", "avanzar", "entregar", "entregar",
                 "particionar", "sanar", "matar", "revivir", "proponer"])
            vivo = [n for n in cluster.ids if n not in cluster.caidos]

            if accion == "avanzar":
                cluster.avanzar(azar.choice([HEARTBEAT_MS, TIMEOUT_ELECCION_MS + 10]))
            elif accion == "entregar":
                cluster.entregar()
            elif accion == "particionar" and len(vivo) >= 2:
                corte = azar.randrange(1, len(cluster.ids))
                cluster.particionar(cluster.ids[:corte], cluster.ids[corte:])
            elif accion == "sanar":
                cluster.sanar()
            elif accion == "matar" and len(vivo) > 1:
                cluster.matar(azar.choice(vivo))
            elif accion == "revivir" and cluster.caidos:
                cluster.revivir(azar.choice(sorted(cluster.caidos)))
            elif accion == "proponer":
                for yo in cluster.masters():
                    cluster.nodos[yo].proponer("encolar", {"id": f"p{paso}"})

            self.assertLessEqual(
                len(cluster.masters()), 1,
                f"two masters at step {paso} (seed={semilla}): {cluster.masters()}")


# =====================================================================
# 2.7 — heartbeats and election timeout
# =====================================================================
class TestHeartbeatsYTimeout(unittest.TestCase):

    def test_slave_sin_heartbeats_arranca_eleccion(self):
        """Scenario: a slave that stops receiving heartbeats starts an election."""
        cluster = ClusterFalso(3)
        cluster.avanzar(TIMEOUT_ELECCION_MS * 3, nodos=["n1"])

        self.assertEqual(cluster.nodos["n1"].rol, "candidato")
        self.assertGreaterEqual(cluster.nodos["n1"].termino_actual, 1)

    def test_slave_con_heartbeats_nunca_arranca_eleccion(self):
        """Scenario: a slave receiving heartbeats never starts an election."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))

        for _ in range(40):
            cluster.avanzar(HEARTBEAT_MS)
            cluster.entregar()
            cluster.entregar()

        self.assertEqual(cluster.nodos["n1"].rol, "slave")
        self.assertEqual(cluster.nodos["n2"].rol, "slave")
        self.assertEqual(cluster.masters(), ["n0"])

    def test_el_master_emite_heartbeats_al_tickear(self):
        """The master path of `tic()` emits appendEntries to every peer."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        cluster.bus.clear()

        cluster.avanzar(HEARTBEAT_MS, nodos=["n0"])

        destinos = sorted(m.destino for _, m in cluster.bus)
        self.assertEqual(destinos, ["n1", "n2"])


# =====================================================================
# 2.8 — replication and commit-index advancement
# =====================================================================
class TestReplicacionYCommit(unittest.TestCase):
    """Spec `queue-replication` — Log Replication and Commit-Index Advancement."""

    def test_no_compromete_sin_mayoria_y_si_con_un_solo_ack(self):
        """Scenario: an entry commits only after majority acknowledgement."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        master = cluster.nodos["n0"]
        cluster.particionar(["n0"], ["n1", "n2"])

        indice, _ = master.proponer("encolar", {"id": "p1"})
        cluster.bombear(4, nodos=["n0"])
        self.assertLess(master.indice_commit, indice,
                        "committed with 0 of 2 slaves acknowledging")

        cluster.cortes = {frozenset(("n0", "n2"))}     # n1 reachable again
        cluster.bombear(6, nodos=["n0"])

        self.assertGreaterEqual(master.indice_commit, indice,
                                "master + 1 slave is a majority of 3")

    def test_el_commit_no_avanza_sobre_una_entrada_de_termino_previo_sola(self):
        """Raft 5.4.2: replicas of a prior-term entry alone never commit it."""
        cluster = ClusterFalso(3)
        for yo in cluster.ids:
            cluster.sembrar(yo, termino=4, entradas=[entrada(1, 3)])
        self.assertTrue(cluster.elegir_master("n0"))
        master = cluster.nodos["n0"]

        for _ in range(4):
            cluster.avanzar(HEARTBEAT_MS)
            cluster.entregar()

        self.assertEqual(master.indice_commit, 0,
                         "a prior-term entry was committed by replica count alone")

    def test_una_entrada_del_termino_propio_arrastra_a_la_anterior(self):
        """Committing a same-term entry carries the older ones with it.

        Note on the `NOT break` rule in `avanzar_commit`: with a well-formed log
        its terms are monotonically non-decreasing, so scanning downwards hits
        every same-term entry before any prior-term one. `continue` and `break`
        are therefore indistinguishable by behaviour here — swapping them leaves
        this suite green. The `continue` is kept because it is what the design
        specifies and it stays correct if a log ever arrives out of order, but
        it is a robustness choice, not something these tests can pin down.
        """
        cluster = ClusterFalso(3)
        for yo in cluster.ids:
            cluster.sembrar(yo, termino=4, entradas=[entrada(1, 3)])
        self.assertTrue(cluster.elegir_master("n0"))
        master = cluster.nodos["n0"]

        indice, _ = master.proponer("encolar", {"id": "p2"})
        for _ in range(8):
            cluster.avanzar(HEARTBEAT_MS)
            cluster.entregar()

        self.assertGreaterEqual(master.indice_commit, indice)
        self.assertGreaterEqual(master.indice_commit, 1,
                                "the prior-term entry did not ride along")

    def test_slave_atrasado_recibe_lo_que_le_falta_en_orden(self):
        """Scenario: a slower slave still receives entries it missed, in order."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        master = cluster.nodos["n0"]
        cluster.particionar(["n2"], ["n0", "n1"])

        for i in range(5):
            master.proponer("encolar", {"id": f"p{i}"})
            cluster.bombear(2, nodos=["n0"])

        cluster.sanar()
        cluster.bombear(12, nodos=["n0"])

        rezagado = [e.indice for e in cluster.nodos["n2"].log if e.operacion != "sentinela"]
        self.assertEqual(rezagado, sorted(rezagado), "entries arrived out of order")
        self.assertEqual(rezagado[-1], master.indice_ultimo)

    def test_rechazo_devuelve_la_pista_de_termino_en_conflicto(self):
        """Decision 11: rejection carries `terminoConflicto`/`primerIndiceDelTermino`."""
        cluster = ClusterFalso(3)
        cluster.sembrar("n1", termino=3, entradas=[entrada(i, 3) for i in range(1, 4)])

        respuesta, _ = cluster.nodos["n1"].recibir_append({
            "termino": 4, "master": "n0",
            "indicePrevio": 9, "terminoPrevio": 4,      # n1 has nothing at index 9
            "entradas": [], "indiceCommit": 0})

        self.assertFalse(respuesta["exito"])
        self.assertIn("terminoConflicto", respuesta)
        self.assertIn("primerIndiceDelTermino", respuesta)

    def test_el_master_salta_al_primer_indice_del_termino_en_vez_de_restar_uno(self):
        """Decision 11: a slave down for many entries resyncs in a couple of rounds."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        master = cluster.nodos["n0"]
        cluster.particionar(["n2"], ["n0", "n1"])

        for i in range(30):
            master.proponer("encolar", {"id": f"p{i}"})
        cluster.bombear(6, nodos=["n0"])

        cluster.sanar()
        rondas = 0
        while cluster.nodos["n2"].indice_ultimo < master.indice_ultimo and rondas < 8:
            cluster.bombear(1, nodos=["n0"])
            rondas += 1

        self.assertEqual(cluster.nodos["n2"].indice_ultimo, master.indice_ultimo)
        self.assertLess(rondas, 8, "resync took one round per entry, not a term jump")

    def test_solo_el_master_propone(self):
        """`proponer` on a slave returns None: there is one writer."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))

        self.assertIsNone(cluster.nodos["n1"].proponer("encolar", {"id": "p1"}))

    def test_entradas_a_aplicar_entrega_solo_lo_comprometido_y_avanza(self):
        """`entradas_a_aplicar()` returns log[aplicado+1 : commit+1] and advances."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))
        master = cluster.nodos["n0"]

        master.proponer("encolar", {"id": "p1"})
        for _ in range(6):
            cluster.avanzar(HEARTBEAT_MS)
            cluster.entregar()

        primera = master.entradas_a_aplicar()
        segunda = master.entradas_a_aplicar()

        self.assertTrue(primera, "nothing was available to apply after a majority ack")
        self.assertTrue(all(e.indice <= master.indice_commit for e in primera))
        self.assertEqual(segunda, [], "the same entries were handed out twice")


# =====================================================================
# 2.9 — injectable clock
# =====================================================================
class TestRelojInyectable(unittest.TestCase):
    """Spec `queue-replication` — Injectable Clock for Deterministic Testing."""

    def test_forzar_eleccion_equivale_a_que_venza_el_timeout(self):
        """Scenario: a test forces an election without waiting on wall-clock time."""
        cluster = ClusterFalso(3)
        cluster.forzar_eleccion("n0")

        self.assertIn(cluster.nodos["n0"].rol, ("candidato", "master"))
        self.assertEqual(cluster.nodos["n0"].termino_actual, 1)

    def test_el_jitter_sale_del_azar_inyectado_y_es_reproducible(self):
        """Same seed, same schedule: the election is reproducible byte for byte."""
        primera = ClusterFalso(3, semilla=99)
        segunda = ClusterFalso(3, semilla=99)
        for cluster in (primera, segunda):
            cluster.avanzar(TIMEOUT_ELECCION_MS + 1)

        self.assertEqual(
            [primera.nodos[n].rol for n in primera.ids],
            [segunda.nodos[n].rol for n in segunda.ids])

    def test_instantanea_expone_el_estado_para_health(self):
        """`instantanea()` feeds `/raft/estado` and `/health`."""
        cluster = ClusterFalso(3)
        self.assertTrue(cluster.elegir_master("n0"))

        foto = cluster.nodos["n0"].instantanea()

        for clave in ("rol", "termino", "indiceLog", "indiceCommit", "masterConocido"):
            self.assertIn(clave, foto)
        self.assertEqual(foto["rol"], "master")


if __name__ == "__main__":
    unittest.main()
