"""The apply step: committed log entries becoming `colas.py` state.

The property that matters here is **determinism**. Two nodes that apply the same
entries in the same order must end up byte-identical, because the moment they do
not, a failover silently changes what the cluster believes happened. So the
central test is not "does encolar work" but "do two fresh `Sistema` instances fed
the same sequence produce the same `estado()` and the same `como_json()`".

The second property is **tolerance**. An entry may be applied twice after a
failover, or name a pedido that is already gone. Every operation must be a no-op
against a stale target and must never raise: an exception in the applier thread
stops a node from applying anything further, which is far worse than a mutation
that finds nothing to do.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aplicar import Aplicador  # noqa: E402
from colas import Sistema  # noqa: E402
from raft import Entrada  # noqa: E402

BALANCEADOR = "balanceador@casa-tomas"
T0 = 1_758_326_400_000          # a fixed epoch ms, so nothing reads a real clock


def reloj_fijo(valor=T0):
    return lambda: valor


def sistema(reloj=None):
    return Sistema(cota_pedidos=10, cota_respuestas=10, reserva=1,
                   ttl_respuestas=30, reloj_ms=reloj or reloj_fijo())


def entrada(indice, operacion, payload):
    return Entrada(indice=indice, termino=1, operacion=operacion, payload=payload)


def encolar(id="p1", idempotente=True, presupuesto_ms=5000, destinatario=BALANCEADOR):
    return {"id": id, "operacion": "GET /personas", "parametros": {},
            "idempotente": idempotente, "destinatario": destinatario,
            "cliente": "100.118.61.111",
            "venceEnMs": T0 + presupuesto_ms, "encoladoEnMs": T0}


SECUENCIA = [
    entrada(1, "encolar", encolar(id="p1")),
    entrada(2, "encolar", encolar(id="p2", idempotente=False)),
    entrada(3, "tomar", {"id": "p1", "consumidor": "casa-A:8080",
                         "reservadoHastaMs": T0 + 2000}),
    entrada(4, "responder", {"id": "p1", "estado": "OK", "contenido": {"ok": True},
                             "atendidoPor": "casa-A:8080", "app": "python",
                             "resueltoEnMs": T0 + 500}),
    entrada(5, "tomar", {"id": "p2", "consumidor": "casa-B:8080",
                         "reservadoHastaMs": T0 + 2000}),
    entrada(6, "devolver", {"id": "p2", "consumidor": "casa-B:8080"}),
    entrada(7, "retirar-respuesta", {"destinatario": BALANCEADOR}),
]


class PruebasDeterminismo(unittest.TestCase):
    """Spec: mutations apply only from committed entries — and identically."""

    def aplicar_todo(self, entradas, reloj=None):
        s = sistema(reloj)
        aplicador = Aplicador(s)
        for e in entradas:
            aplicador.aplicar(e)
        return s

    def test_dos_sistemas_con_la_misma_secuencia_quedan_identicos(self):
        uno = self.aplicar_todo(SECUENCIA)
        otro = self.aplicar_todo(SECUENCIA)

        self.assertEqual(uno.estado(), otro.estado())

    def test_los_pedidos_quedan_identicos_campo_por_campo(self):
        uno = self.aplicar_todo(SECUENCIA)
        otro = self.aplicar_todo(SECUENCIA)

        self.assertEqual(
            [p.como_json() for p in uno.pedidos._esperando],
            [p.como_json() for p in otro.pedidos._esperando])

    def test_el_esperaMs_no_depende_del_reloj_local(self):
        """`esperaMs` is a contract field: it must come from the replicated
        `encoladoEnMs`, not from whenever this node happened to apply."""
        uno = self.aplicar_todo(SECUENCIA[:4], reloj=reloj_fijo(T0 + 1000))
        otro = self.aplicar_todo(SECUENCIA[:4], reloj=reloj_fijo(T0 + 1000))

        self.assertEqual(uno.tomar_respuesta(BALANCEADOR, 0),
                         otro.tomar_respuesta(BALANCEADOR, 0))

    def test_el_orden_importa_y_se_respeta(self):
        s = self.aplicar_todo(SECUENCIA[:3])

        self.assertTrue(s.pedido_en_vuelo("p1"))
        self.assertFalse(s.pedido_en_vuelo("p2"))


class PruebasTolerancia(unittest.TestCase):
    """Every operation is a no-op against a target that is already gone."""

    def setUp(self):
        self.sistema = sistema()
        self.aplicador = Aplicador(self.sistema)

    def test_ninguna_operacion_explota_contra_un_id_fantasma(self):
        fantasmas = [
            entrada(1, "tomar", {"id": "x", "consumidor": "c", "reservadoHastaMs": T0}),
            entrada(2, "devolver", {"id": "x", "consumidor": "c"}),
            entrada(3, "responder", {"id": "x", "estado": "OK", "contenido": {},
                                     "atendidoPor": "c", "app": "python"}),
            entrada(4, "retirar-respuesta", {"destinatario": "nadie"}),
            entrada(5, "expirar", {"reencolar": ["x"], "fallar": [
                {"id": "y", "estado": "DEADLINE_EXCEEDED", "detalle": "-"}]}),
        ]

        for e in fantasmas:
            with self.subTest(operacion=e.operacion):
                self.aplicador.aplicar(e)      # must not raise

    def test_aplicar_la_misma_entrada_dos_veces_no_duplica(self):
        self.aplicador.aplicar(entrada(1, "encolar", encolar()))
        self.aplicador.aplicar(entrada(1, "encolar", encolar()))

        self.assertEqual(self.sistema.estado()["pedidos"]["esperando"], 2,
                         "encolar is not deduplicated by the applier — the log is")

    def test_una_operacion_desconocida_se_saltea_y_no_explota(self):
        """An unknown op is a newer node's vocabulary, not a reason to stop."""
        resultado = self.aplicador.aplicar(entrada(1, "ordeñar-la-cola", {"que": "?"}))

        self.assertIsNone(resultado)
        self.assertEqual(self.sistema.estado()["pedidos"]["esperando"], 0)

    def test_la_sentinela_no_hace_nada(self):
        antes = self.sistema.estado()

        self.aplicador.aplicar(entrada(1, "sentinela", {}))

        self.assertEqual(self.sistema.estado(), antes)


class PruebasOperaciones(unittest.TestCase):
    """Decision 1: each operation maps to exactly one `Sistema` call."""

    def setUp(self):
        self.sistema = sistema()
        self.aplicador = Aplicador(self.sistema)

    def test_encolar(self):
        self.aplicador.aplicar(entrada(1, "encolar", encolar()))

        self.assertEqual(self.sistema.estado()["pedidos"]["esperando"], 1)

    def test_tomar_reserva_el_pedido_nombrado(self):
        self.aplicador.aplicar(entrada(1, "encolar", encolar()))
        self.aplicador.aplicar(entrada(2, "tomar", {
            "id": "p1", "consumidor": "casa-A:8080", "reservadoHastaMs": T0 + 2000}))

        self.assertTrue(self.sistema.pedido_en_vuelo("p1"))

    def test_responder_es_atomico(self):
        """Spec: responder's two mutations are indivisible."""
        self.aplicador.aplicar(entrada(1, "encolar", encolar()))
        self.aplicador.aplicar(entrada(2, "responder", {
            "id": "p1", "estado": "OK", "contenido": {"ok": True},
            "atendidoPor": "casa-A:8080", "app": "python"}))

        self.assertEqual(self.sistema.estado()["pedidos"]["esperando"], 0)
        self.assertIsNotNone(self.sistema.tomar_respuesta(BALANCEADOR, 0))

    def test_expirar_falla_y_publica_en_un_solo_paso(self):
        """Spec: recuperar's expiry-and-refail is indivisible."""
        self.aplicador.aplicar(entrada(1, "encolar", encolar(idempotente=False)))
        self.aplicador.aplicar(entrada(2, "tomar", {
            "id": "p1", "consumidor": "casa-A:8080", "reservadoHastaMs": T0 - 1}))
        self.aplicador.aplicar(entrada(3, "expirar", {
            "reencolar": [],
            "fallar": [{"id": "p1", "estado": "DEADLINE_EXCEEDED",
                        "detalle": "casa-A:8080 no contestó"}],
            "decididoEnMs": T0}))

        self.assertFalse(self.sistema.pedido_en_vuelo("p1"))
        respuesta = self.sistema.tomar_respuesta(BALANCEADOR, 0)
        self.assertEqual(respuesta["estado"], "DEADLINE_EXCEEDED")

    def test_expirar_reencola_una_lectura(self):
        self.aplicador.aplicar(entrada(1, "encolar", encolar(idempotente=True)))
        self.aplicador.aplicar(entrada(2, "tomar", {
            "id": "p1", "consumidor": "casa-A:8080", "reservadoHastaMs": T0 - 1}))
        self.aplicador.aplicar(entrada(3, "expirar", {
            "reencolar": ["p1"], "fallar": [], "decididoEnMs": T0}))

        self.assertFalse(self.sistema.pedido_en_vuelo("p1"))
        self.assertEqual(self.sistema.estado()["pedidos"]["esperando"], 1)

    def test_retirar_respuesta(self):
        self.aplicador.aplicar(entrada(1, "encolar", encolar()))
        self.aplicador.aplicar(entrada(2, "responder", {
            "id": "p1", "estado": "OK", "contenido": {}, "atendidoPor": "c", "app": "python"}))
        self.aplicador.aplicar(entrada(3, "retirar-respuesta", {"destinatario": BALANCEADOR}))

        self.assertEqual(self.sistema.estado()["respuestas"]["pendientes"], 0)


if __name__ == "__main__":
    unittest.main()
