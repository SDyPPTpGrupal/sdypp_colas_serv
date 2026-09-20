"""Raft-lite consensus state machine for the replicated queue.

This module is deliberately inert: it owns no thread, opens no socket and reads
no clock. Time arrives as an argument to `tic()`, randomness arrives as an
injected `random.Random`, and outbound messages are *returned* rather than sent.
Everything that would make a consensus test a race lives outside this file.

The design that fixes this shape is Decision 4 of
`openspec/changes/desacople-cola-replicada/design.md`; the behaviour it must
satisfy is the `queue-replication` capability spec.

Safety rests on three rules, each implemented in one place here:

  1. A node grants at most one vote per term, and only to a candidate whose log
     is at least as up to date as its own (`_al_menos_tan_al_dia`). This is what
     keeps a stale node from winning and dropping committed entries.
  2. An entry is committed only once a majority holds it, and only entries from
     the master's own term are committed by counting replicas (`_avanzar_commit`).
  3. Any message carrying a higher term forces an immediate step down to slave
     (`_fencing`), which is what makes two simultaneous masters impossible.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

MASTER = "master"
SLAVE = "slave"
CANDIDATO = "candidato"


@dataclass(frozen=True)
class Entrada:
    """One replicated log entry. `indice` is 1-based and contiguous.

    `payload` is fully resolved by the master before it is appended: no clock
    reads, no uuid4, no predicates. A slave applying this entry months later
    must reach exactly the same state the master did.
    """

    indice: int
    termino: int
    operacion: str
    payload: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Mensaje:
    """An outbound message. The caller decides how it travels."""

    destino: str
    tipo: str          # "solicitud_voto" | "append"
    cuerpo: dict


# Index 0 is a sentinel so that `log[i]` is the entry with `indice == i`,
# and so log matching has something to anchor against on an empty log.
SENTINELA = Entrada(indice=0, termino=0, operacion="sentinela", payload={})


class NodoRaft:
    """One node's consensus state. Pure: drive it with `tic()` and `recibir_*`."""

    def __init__(self, yo: str, pares: list, reloj: Callable[[], int],
                 azar: Any, timeout_eleccion_ms: int, heartbeat_ms: int):
        self.yo = yo
        self.pares = list(pares)
        self.reloj = reloj
        self.azar = azar
        self.timeout_eleccion_ms = timeout_eleccion_ms
        self.heartbeat_ms = heartbeat_ms

        # Term state. A single-node cluster is not a special case: it is a
        # majority of one, and it becomes master through a normal election.
        self.termino_actual = 0
        self.voto_para: Optional[str] = None
        self.log = [SENTINELA]
        self.rol = SLAVE
        self.master_conocido: Optional[str] = None

        self.indice_commit = 0
        self.indice_aplicado = 0

        # Master bookkeeping, meaningless while this node is not master.
        self.votos: set = set()
        self.siguiente_indice: dict = {}
        self.indice_coincidente: dict = {}

        self._vence_eleccion_ms = 0
        self._proximo_heartbeat_ms = 0
        self._reiniciar_timeout(0)

        # Single-node mode is a majority of one, so this node is master from the
        # start. Not a special case in the data path — it is the same `mayoria`
        # arithmetic — but it must hold from the first instant: a window of being
        # a slave is a window of emitting a `421` a lone node must never emit.
        if not self.pares:
            self.termino_actual = 1
            self.voto_para = self.yo
            self.rol = MASTER
            self.master_conocido = self.yo

    # ------------------------------------------------------------- properties
    @property
    def mayoria(self) -> int:
        """Nodes needed to commit, counting this one. With a single node: 1."""
        return (len(self.pares) + 1) // 2 + 1

    @property
    def indice_ultimo(self) -> int:
        return self.log[-1].indice

    @property
    def termino_ultimo(self) -> int:
        return self.log[-1].termino

    # ------------------------------------------------------------------ clock
    def _reiniciar_timeout(self, ahora_ms: int) -> None:
        """Re-arm the election timeout with per-node jitter.

        The jitter is what keeps two slaves from standing for election in the
        same millisecond; it comes from the injected generator so a test replays
        the exact same schedule from the same seed.
        """
        jitter = self.azar.randint(0, self.timeout_eleccion_ms)
        self._vence_eleccion_ms = ahora_ms + self.timeout_eleccion_ms + jitter

    def tic(self, ahora_ms: int) -> list:
        """The only thing that starts an election or emits a heartbeat."""
        if self.rol == MASTER:
            if ahora_ms >= self._proximo_heartbeat_ms:
                self._proximo_heartbeat_ms = ahora_ms + self.heartbeat_ms
                return self._emitir_appends(ahora_ms)
            return []

        if ahora_ms >= self._vence_eleccion_ms:
            return self._arrancar_eleccion(ahora_ms)
        return []

    # --------------------------------------------------------------- election
    def _arrancar_eleccion(self, ahora_ms: int) -> list:
        self.termino_actual += 1
        self.rol = CANDIDATO
        self.voto_para = self.yo
        self.votos = {self.yo}
        self.master_conocido = None
        self._reiniciar_timeout(ahora_ms)

        if len(self.votos) >= self.mayoria:      # majority of one
            return self._hacerse_master(ahora_ms)

        cuerpo = {
            "termino": self.termino_actual,
            "candidato": self.yo,
            "ultimoIndiceLog": self.indice_ultimo,
            "ultimoTerminoLog": self.termino_ultimo,
        }
        return [Mensaje(destino=par, tipo="solicitud_voto", cuerpo=dict(cuerpo))
                for par in self.pares]

    def _hacerse_master(self, ahora_ms: int) -> list:
        self.rol = MASTER
        self.master_conocido = self.yo
        self.votos = set()
        # Optimistic: assume every peer matches us, and let the log-matching
        # rejection walk us back. That is one round trip in the common case.
        self.siguiente_indice = {par: self.indice_ultimo + 1 for par in self.pares}
        self.indice_coincidente = {par: 0 for par in self.pares}
        self._proximo_heartbeat_ms = ahora_ms + self.heartbeat_ms
        self._avanzar_commit()
        return self._emitir_appends(ahora_ms)

    def _al_menos_tan_al_dia(self, ultimo_indice: int, ultimo_termino: int) -> bool:
        """Raft's log-freshness rule: last term first, index only on a tie."""
        if ultimo_termino != self.termino_ultimo:
            return ultimo_termino > self.termino_ultimo
        return ultimo_indice >= self.indice_ultimo

    def recibir_solicitud_voto(self, msg: dict):
        self._fencing(msg["termino"])

        conceder = (
            msg["termino"] >= self.termino_actual
            and self.voto_para in (None, msg["candidato"])
            and self._al_menos_tan_al_dia(msg["ultimoIndiceLog"], msg["ultimoTerminoLog"])
        )
        if conceder:
            self.voto_para = msg["candidato"]
            self._reiniciar_timeout(self.reloj())

        return {"termino": self.termino_actual, "votoConcedido": conceder}, []

    def recibir_respuesta_voto(self, de: str, msg: dict) -> list:
        if not self._respuesta_vigente(msg, CANDIDATO):
            return []          # a reply from an election we already left

        if msg.get("votoConcedido"):
            self.votos.add(de)
            if len(self.votos) >= self.mayoria:
                return self._hacerse_master(self.reloj())
        return []

    # ------------------------------------------------------------ replication
    def _emitir_appends(self, ahora_ms: int) -> list:
        return [self._append_para(par) for par in self.pares]

    def _append_para(self, par: str) -> Mensaje:
        siguiente = self.siguiente_indice.get(par, self.indice_ultimo + 1)
        siguiente = max(1, min(siguiente, self.indice_ultimo + 1))
        previo = siguiente - 1
        return Mensaje(destino=par, tipo="append", cuerpo={
            "termino": self.termino_actual,
            "master": self.yo,
            "indicePrevio": previo,
            "terminoPrevio": self.log[previo].termino,
            "entradas": list(self.log[siguiente:]),
            "indiceCommit": self.indice_commit,
        })

    def _pista_de_conflicto(self, indice_previo: int) -> dict:
        """Decision 11: tell the master where our term actually starts.

        Backing up one index per round trip means a slave that missed a thousand
        entries needs a thousand heartbeats to catch up. Returning the first
        index of the conflicting term turns that into a couple of rounds.
        """
        if indice_previo > self.indice_ultimo:
            return {"terminoConflicto": None,
                    "primerIndiceDelTermino": self.indice_ultimo + 1}

        termino_conflicto = self.log[indice_previo].termino
        primero = indice_previo
        while primero > 1 and self.log[primero - 1].termino == termino_conflicto:
            primero -= 1
        return {"terminoConflicto": termino_conflicto,
                "primerIndiceDelTermino": primero}

    def recibir_append(self, msg: dict):
        self._fencing(msg["termino"])

        if msg["termino"] < self.termino_actual:
            return {"termino": self.termino_actual, "exito": False,
                    "terminoConflicto": None, "primerIndiceDelTermino": 1}, []

        # A valid append from the current term: whoever sent it leads.
        if self.rol != SLAVE:
            self.rol = SLAVE
        self.master_conocido = msg.get("master")
        self._reiniciar_timeout(self.reloj())

        indice_previo = msg["indicePrevio"]
        if (indice_previo > self.indice_ultimo
                or self.log[indice_previo].termino != msg["terminoPrevio"]):
            respuesta = {"termino": self.termino_actual, "exito": False}
            respuesta.update(self._pista_de_conflicto(indice_previo))
            return respuesta, []

        # Log matching held: truncate anything that diverges, then append.
        for entrada in msg.get("entradas") or []:
            if entrada.indice <= self.indice_ultimo:
                if self.log[entrada.indice].termino == entrada.termino:
                    continue
                del self.log[entrada.indice:]
            self.log.append(entrada)

        if msg["indiceCommit"] > self.indice_commit:
            self.indice_commit = min(msg["indiceCommit"], self.indice_ultimo)

        return {"termino": self.termino_actual, "exito": True,
                "indiceCoincidente": self.indice_ultimo}, []

    def recibir_respuesta_append(self, de: str, msg: dict) -> list:
        if not self._respuesta_vigente(msg, MASTER):
            return []

        if msg.get("exito"):
            self.indice_coincidente[de] = msg["indiceCoincidente"]
            self.siguiente_indice[de] = msg["indiceCoincidente"] + 1
            self._avanzar_commit()
            if self.siguiente_indice[de] <= self.indice_ultimo:
                return [self._append_para(de)]      # still behind, keep feeding
            return []

        primero = msg.get("primerIndiceDelTermino") or 1
        self.siguiente_indice[de] = max(1, primero)
        return [self._append_para(de)]

    def _avanzar_commit(self) -> None:
        """Raft 5.4.2: a master commits only entries from its own term by
        counting replicas. Prior-term entries ride along once a same-term entry
        commits — hence `continue` and not `break`.
        """
        for n in range(self.indice_ultimo, self.indice_commit, -1):
            if self.log[n].termino != self.termino_actual:
                continue
            replicas = 1 + sum(1 for par in self.pares
                               if self.indice_coincidente.get(par, 0) >= n)
            if replicas >= self.mayoria:
                self.indice_commit = n
                return

    # ---------------------------------------------------------------- fencing
    def _respuesta_vigente(self, msg: dict, rol_esperado: str) -> bool:
        """Fencing first, then: is this reply still about who we are now?

        A reply can outlive the role that sent the request — we may have been
        deposed, or moved on to a newer term — and acting on a stale one is how
        a node resurrects an election it already lost.
        """
        if self._fencing(msg["termino"]):
            return False
        return self.rol == rol_esperado and msg["termino"] == self.termino_actual

    def _fencing(self, termino_ajeno: int) -> bool:
        """Rule 3. First statement of all four `recibir_*`.

        A node that sees a higher term is out of date by definition, whatever it
        believed it was. Stepping down here is what makes a revived old master
        harmless without anyone having to detect or evict it.
        """
        if termino_ajeno > self.termino_actual:
            self.termino_actual = termino_ajeno
            self.voto_para = None
            self.rol = SLAVE
            self.master_conocido = None
            self.votos = set()
            return True
        return False

    # ------------------------------------------------------------------- api
    def proponer(self, operacion: str, payload: dict):
        """Append a new entry. Master only; returns `(indice, termino)`.

        Returning `None` on a slave is what makes "there is exactly one writer"
        a property of the state machine rather than a convention callers follow.
        """
        if self.rol != MASTER:
            return None

        entrada = Entrada(indice=self.indice_ultimo + 1,
                          termino=self.termino_actual,
                          operacion=operacion,
                          payload=payload)
        self.log.append(entrada)
        self._avanzar_commit()      # a majority of one commits immediately
        return entrada.indice, entrada.termino

    def entradas_a_aplicar(self) -> list:
        """Committed-but-unapplied entries, in order, advancing `indice_aplicado`.

        Pulled rather than pushed through a callback, so a test can assert on a
        value instead of on a spy's call log.
        """
        if self.indice_aplicado >= self.indice_commit:
            return []
        pendientes = self.log[self.indice_aplicado + 1:self.indice_commit + 1]
        self.indice_aplicado = self.indice_commit
        return pendientes

    def instantanea(self) -> dict:
        """State for `/raft/estado` and `/health`."""
        return {
            "yo": self.yo,
            "rol": self.rol,
            "termino": self.termino_actual,
            "indiceLog": self.indice_ultimo,
            "indiceCommit": self.indice_commit,
            "indiceAplicado": self.indice_aplicado,
            "masterConocido": self.master_conocido,
            "pares": list(self.pares),
        }
