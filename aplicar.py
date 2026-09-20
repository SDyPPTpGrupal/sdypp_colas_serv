"""Committed log entries becoming queue state.

One committed entry, one `Sistema` call. Never the lower-level `ColaPedidos` /
`ColaRespuestas` primitives — and that is the whole point of this module, not a
stylistic preference.

`Sistema.responder()` performs two mutations (pop the pedido, publish the
response) and `Sistema.expirar()` performs two or more. If those travelled as
separate log entries, a commit-index boundary or a failover could land between
them, and a node could be promoted holding "pedido removed, response never
published" — a request silently lost *after* the client was told `202`. Applying
through `Sistema`, which is already this codebase's compound-atomicity boundary,
makes each entry all-or-nothing on replay for free.

Two rules hold for every operation here:

  * **Tolerant.** An entry may name a pedido that is already gone — it was
    applied before, or a later entry overtook it. That is a no-op, never an
    error.
  * **Never raises.** An exception in the applier thread stops a node from
    applying anything further, which is far worse than one mutation that found
    nothing to do. Unknown operations are skipped, not raised: they are a newer
    node's vocabulary, not a reason to stop.
"""

from colas import Pedido


class Aplicador:
    """Applies committed entries to one `Sistema`. One per node."""

    def __init__(self, sistema, registrar=None):
        self.sistema = sistema
        self.registrar = registrar or (lambda *_: None)
        self.aplicadas = 0
        self._tabla = {
            "encolar": self._encolar,
            "tomar": self._tomar,
            "devolver": self._devolver,
            "responder": self._responder,
            "retirar-respuesta": self._retirar_respuesta,
            "expirar": self._expirar,
            "sentinela": self._sentinela,
        }

    def aplicar(self, entrada):
        """Apply one committed entry. Returns whatever the operation produced."""
        manejar = self._tabla.get(entrada.operacion)
        if manejar is None:
            self.registrar("aplicar", 400,
                           f"operación desconocida: {entrada.operacion!r} "
                           f"(índice {entrada.indice})")
            return None
        try:
            resultado = manejar(entrada.payload or {})
        except Exception as e:                                  # noqa: BLE001
            # Deliberately broad: a node that stops applying is a node that
            # silently falls behind the cluster while still answering /health.
            self.registrar("aplicar", 500,
                           f"{entrada.operacion} índice {entrada.indice}: {e}")
            return None
        self.aplicadas += 1
        return resultado

    # ------------------------------------------------------------ operaciones
    def _encolar(self, payload):
        pedido = Pedido(
            id=payload["id"],
            operacion=payload["operacion"],
            parametros=payload.get("parametros") or {},
            idempotente=bool(payload.get("idempotente", False)),
            destinatario=payload["destinatario"],
            vence_en_ms=payload["venceEnMs"],
            encolado_en_ms=payload["encoladoEnMs"],
            cliente=payload.get("cliente"),
            reloj_ms=self.sistema.reloj_ms,
        )
        return self.sistema.publicar_pedido(pedido)

    def _tomar(self, payload):
        return self.sistema.reservar_pedido(
            payload["id"], payload["consumidor"], payload["reservadoHastaMs"])

    def _devolver(self, payload):
        return self.sistema.devolver_pedido(payload["id"], payload.get("consumidor"))

    def _responder(self, payload):
        return self.sistema.responder(
            payload["id"], payload.get("estado"), payload.get("contenido") or {},
            payload.get("atendidoPor"), payload.get("app"))

    def _retirar_respuesta(self, payload):
        return self.sistema.retirar_respuesta(payload["destinatario"])

    def _expirar(self, payload):
        return self.sistema.expirar(payload)

    def _sentinela(self, _payload):
        """A no-op entry.

        It exists so a fresh master can prove its log is caught up: committing
        one entry of its own term is what makes every earlier entry commit too,
        and it has to be an entry that changes nothing.
        """
        return None
