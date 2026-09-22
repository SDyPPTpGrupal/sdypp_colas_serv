"""Pruebas de las dos colas, sin HTTP.

Lo que se fija acá es lo que hace que sacar los workers del balanceador no
cueste tolerancia a fallos. Con los workers adentro, el que se comía el error
del RPC devolvía el pedido al frente y listo. Ahora el worker está en la réplica
y una réplica que se murió no devuelve nada: la única que puede notarlo es la
cola, por la reserva vencida. Si estas pruebas pasan, un `docker stop` en medio
de una request sigue terminando en un 200 para el usuario.

    python -m unittest discover -s tests -v
"""

import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from colas import ColaPedidos, ColaRespuestas, Pedido, Sistema  # noqa: E402


def pedido(id="p1", operacion="GET /personas", idempotente=True, presupuesto=5.0,
           destinatario="balanceador@casa-tomas"):
    ahora = int(time.time() * 1000)
    return Pedido(id=id, operacion=operacion, parametros={}, idempotente=idempotente,
                  destinatario=destinatario,
                  vence_en_ms=ahora + int(presupuesto * 1000), encolado_en_ms=ahora)


class PruebasColaPedidos(unittest.TestCase):

    def setUp(self):
        self.cola = ColaPedidos(cota=3, reserva=0.2)

    def test_sale_en_el_orden_en_que_entro(self):
        for i in range(3):
            self.cola.publicar(pedido(id=f"p{i}"))
        self.assertEqual([self.cola.tomar("r1", 0).id for _ in range(3)], ["p0", "p1", "p2"])

    def test_la_cota_rechaza_en_el_acto(self):
        """No bloquea al que publica: el balanceador prefiere contestar 503 ya a
        tener al cliente esperando por un lugar además de por su respuesta."""
        for i in range(3):
            self.assertTrue(self.cola.publicar(pedido(id=f"p{i}")))
        antes = time.monotonic()
        self.assertFalse(self.cola.publicar(pedido(id="de-mas")))
        self.assertLess(time.monotonic() - antes, 0.05)

    def test_tomar_sin_nada_espera_y_devuelve_none(self):
        antes = time.monotonic()
        self.assertIsNone(self.cola.tomar("r1", 0.2))
        self.assertGreaterEqual(time.monotonic() - antes, 0.2)

    def test_tomar_reserva_el_pedido_y_anota_quien(self):
        self.cola.publicar(pedido())
        p = self.cola.tomar("10.0.0.1:8080", 0)
        self.assertEqual(p.reservado_por, "10.0.0.1:8080")
        self.assertEqual(p.intentos, ["10.0.0.1:8080"])
        # Reservado no es entregado: sigue contando como en vuelo hasta que
        # alguien conteste, que es lo que permite recuperarlo.
        self.assertEqual(self.cola.estado()["enVuelo"], 1)
        self.assertEqual(self.cola.estado()["esperando"], 0)

    def test_dos_consumidores_no_se_llevan_el_mismo(self):
        self.cola.publicar(pedido())
        self.assertIsNotNone(self.cola.tomar("r1", 0))
        self.assertIsNone(self.cola.tomar("r2", 0))

    def test_devolver_lo_pone_primero_y_no_ultimo(self):
        """Al frente y no al final: el pedido ya esperó una vez, y mandarlo atrás
        de los que llegaron después le agrega la espera de todos ellos."""
        self.cola.publicar(pedido(id="viejo"))
        self.cola.publicar(pedido(id="nuevo"))
        p = self.cola.tomar("r1", 0)
        self.cola.devolver(p.id, "r1")
        self.assertEqual(self.cola.tomar("r2", 0).id, "viejo")

    def test_devolver_de_otro_consumidor_no_hace_nada(self):
        self.cola.publicar(pedido())
        self.cola.tomar("r1", 0)
        self.assertFalse(self.cola.devolver("p1", "r2"))
        self.assertFalse(self.cola.devolver("no-existe", "r1"))

    def test_completar_lo_saca_de_en_vuelo(self):
        self.cola.publicar(pedido())
        self.cola.tomar("r1", 0)
        self.assertIsNotNone(self.cola.completar("p1"))
        self.assertEqual(self.cola.estado()["enVuelo"], 0)

    def test_completar_dos_veces_devuelve_none(self):
        """La segunda respuesta de un pedido que se atendió dos veces. Tiene que
        poder distinguirse o el balanceador despacharía una respuesta repetida."""
        self.cola.publicar(pedido())
        self.cola.tomar("r1", 0)
        self.cola.completar("p1")
        self.assertIsNone(self.cola.completar("p1"))

    def test_completar_alcanza_a_uno_que_ya_habia_vuelto_a_la_cola(self):
        """La réplica lenta contestó después de que la reserva venció y el
        pedido volvió a la cola. Gana la primera respuesta que llega: el cliente
        se lleva su 200 en vez de esperar una segunda vuelta entera."""
        self.cola.publicar(pedido())
        self.cola.tomar("r1", 0)
        time.sleep(0.25)
        self.cola.recuperar()
        self.assertEqual(self.cola.estado()["esperando"], 1)
        self.assertIsNotNone(self.cola.completar("p1"))
        self.assertEqual(self.cola.estado()["esperando"], 0)

    def test_no_entrega_un_pedido_que_ya_vencio(self):
        """Dárselo a un worker sería gastarle una réplica a algo que nadie va a
        leer. Queda apartado para que el recuperador lo falle."""
        self.cola.publicar(pedido(presupuesto=0.05))
        time.sleep(0.1)
        self.assertIsNone(self.cola.tomar("r1", 0))
        fallidos = self.cola.recuperar()
        self.assertEqual([(p.id, e) for p, e, _ in fallidos], [("p1", "DEADLINE_EXCEEDED")])


class PruebasRecuperar(unittest.TestCase):
    """La regla de reintento, que es contrato y no detalle de implementación."""

    def setUp(self):
        self.cola = ColaPedidos(cota=10, reserva=0.1)

    def test_una_lectura_abandonada_vuelve_al_frente(self):
        self.cola.publicar(pedido(idempotente=True))
        self.cola.tomar("replica-muerta", 0)
        time.sleep(0.15)
        fallidos = self.cola.recuperar()
        self.assertEqual(fallidos, [])
        self.assertEqual(self.cola.estado()["esperando"], 1)
        self.assertEqual(self.cola.reasignados, 1)
        # Y el que la toma después queda anotado como segundo intento: es la
        # evidencia de la reasignación que se muestra en la demo.
        self.assertEqual(self.cola.tomar("otra", 0).intentos, ["replica-muerta", "otra"])

    def test_una_escritura_abandonada_se_falla_sin_reintentar(self):
        """"No contestó" no dice si alcanzó a ejecutarse. Repetir un
        POST /personas puede crear la persona dos veces, y preferimos un 504
        honesto a un duplicado silencioso."""
        self.cola.publicar(pedido(operacion="POST /personas", idempotente=False))
        self.cola.tomar("replica-muerta", 0)
        time.sleep(0.15)
        fallidos = self.cola.recuperar()
        self.assertEqual(len(fallidos), 1)
        p, estado, detalle = fallidos[0]
        self.assertEqual(estado, "DEADLINE_EXCEEDED")
        self.assertIn("no es idempotente", detalle)
        self.assertEqual(self.cola.estado(), dict(self.cola.estado(), esperando=0, enVuelo=0))

    def test_sin_presupuesto_no_se_reintenta_ni_siquiera_una_lectura(self):
        self.cola.publicar(pedido(idempotente=True, presupuesto=0.12))
        self.cola.tomar("replica-muerta", 0)
        time.sleep(0.2)
        fallidos = self.cola.recuperar()
        self.assertEqual(len(fallidos), 1)
        self.assertIn("venció mientras lo atendía", fallidos[0][2])

    def test_el_que_espera_de_mas_se_falla_y_libera_lugar(self):
        self.cola.publicar(pedido(id="corto", presupuesto=0.05))
        self.cola.publicar(pedido(id="largo", presupuesto=5))
        time.sleep(0.1)
        fallidos = self.cola.recuperar()
        self.assertEqual([p.id for p, _, _ in fallidos], ["corto"])
        self.assertEqual(self.cola.estado()["esperando"], 1)

    def test_la_reserva_no_pasa_del_vencimiento_del_pedido(self):
        """Reservarlo más allá de su presupuesto sería retener algo que ya nadie
        va a leer: la reserva se corta donde se corta el pedido."""
        cola = ColaPedidos(cota=10, reserva=10)
        cola.publicar(pedido(presupuesto=0.5))
        p = cola.tomar("r1", 0)
        self.assertLessEqual(p.reservado_hasta_ms, p.vence_en_ms)


class PruebasColaRespuestas(unittest.TestCase):

    def setUp(self):
        self.cola = ColaRespuestas(cota=2, ttl=0.2)

    def test_cada_uno_se_lleva_solo_lo_suyo(self):
        """Con una FIFO sola, el segundo balanceador se comería las respuestas
        del primero y los dos clientes esperarían de más."""
        self.cola.publicar("ba-1", {"id": "a"})
        self.cola.publicar("ba-2", {"id": "b"})
        self.assertEqual(self.cola.tomar("ba-2", 0)["id"], "b")
        self.assertIsNone(self.cola.tomar("ba-2", 0))
        self.assertEqual(self.cola.tomar("ba-1", 0)["id"], "a")

    def test_espera_y_devuelve_none_si_no_llega_nada(self):
        antes = time.monotonic()
        self.assertIsNone(self.cola.tomar("ba-1", 0.2))
        self.assertGreaterEqual(time.monotonic() - antes, 0.2)

    def test_la_cota_es_por_destinatario(self):
        self.assertTrue(self.cola.publicar("ba-1", {"id": "a"}))
        self.assertTrue(self.cola.publicar("ba-1", {"id": "b"}))
        self.assertFalse(self.cola.publicar("ba-1", {"id": "c"}))
        self.assertTrue(self.cola.publicar("ba-2", {"id": "d"}))

    def test_retirar_la_ultima_borra_la_caja(self):
        """Con un destinatario por ticket, una caja vacía que queda es una por
        pedido asincrónico, para siempre. Y el mismo destinatario tiene que
        poder volver a recibir después de vaciarse."""
        self.cola.publicar("ticket:a", {"id": "a"})
        self.assertEqual(self.cola.retirar("ticket:a")["id"], "a")
        self.assertEqual(self.cola.estado()["porDestinatario"], {})

        self.cola.publicar("ticket:a", {"id": "a2"})
        self.assertEqual(self.cola.tomar("ticket:a", 0)["id"], "a2")

    def test_dos_recolectores_sobre_la_misma_respuesta_no_pierden_la_siguiente(self):
        """El balanceador abre BA_RECOLECTORES long-polls contra el mismo
        destinatario, así que dos pueden espiar la misma respuesta antes de que
        ninguno haya comprometido su retiro.

        Sin el `id` en el retiro, el segundo en aplicarse sacaba la respuesta
        SIGUIENTE y la tiraba: nadie la entregaba nunca, y el cliente que la
        esperaba se comía el timeout entero con su pedido ya contestado. Es
        pérdida silenciosa, y era invisible con un solo recolector.
        """
        self.cola.publicar("ba-1", {"id": "r1"})
        self.cola.publicar("ba-1", {"id": "r2"})

        espiada_a = self.cola.espiar("ba-1")
        espiada_b = self.cola.espiar("ba-1")          # los dos ven r1
        self.assertEqual(espiada_a["id"], espiada_b["id"])

        self.assertEqual(self.cola.retirar("ba-1", espiada_a["id"])["id"], "r1")
        self.assertIsNone(self.cola.retirar("ba-1", espiada_b["id"]),
                          "el segundo retiro no tiene nada que sacar")

        self.assertEqual(self.cola.espiar("ba-1")["id"], "r2",
                         "r2 sigue ahí, esperando a quien la pidió")

    def test_espiar_saltea_la_que_otro_ya_reclamo(self):
        """El caso común: el segundo recolector no llega a espiar siquiera."""
        self.cola.publicar("ba-1", {"id": "r1"})

        self.assertEqual(self.cola.espiar("ba-1")["id"], "r1")
        self.assertIsNone(self.cola.espiar("ba-1", excluidos={"r1"}))

    def test_purgar_tira_lo_que_nadie_recolecto(self):
        """El balanceador que se murió sin recolectar: sus respuestas no las va
        a leer nadie nunca y sin esto harían crecer la memoria hasta que duela."""
        self.cola.publicar("ba-muerto", {"id": "a"})
        time.sleep(0.25)
        self.assertEqual(self.cola.purgar(), 1)
        self.assertEqual(self.cola.estado()["pendientes"], 0)


class PruebasSistema(unittest.TestCase):
    """Las dos colas trabajando juntas, que es como se usan siempre."""

    def setUp(self):
        self.sistema = Sistema(cota_pedidos=10, cota_respuestas=10,
                               reserva=0.1, ttl_respuestas=5)

    def test_el_ciclo_completo(self):
        self.sistema.publicar_pedido(pedido())
        p = self.sistema.tomar_pedido("10.0.0.1:8080", 0)
        aceptada, motivo = self.sistema.responder(
            p.id, "OK", {"personas": []}, "10.0.0.1:8080", "python")
        self.assertTrue(aceptada)
        self.assertEqual(motivo, "entregada")
        r = self.sistema.tomar_respuesta("balanceador@casa-tomas", 0)
        self.assertEqual(r["estado"], "OK")
        self.assertEqual(r["atendidoPor"], "10.0.0.1:8080")
        self.assertEqual(r["intentos"], ["10.0.0.1:8080"])
        self.assertIn("esperaMs", r)

    def test_el_destinatario_lo_pone_la_cola_no_el_worker(self):
        """El worker no tiene por qué saber quién le pidió, y si se lo
        preguntáramos podría mentir: contestar apuntando a otro balanceador le
        metería una respuesta ajena en la cola."""
        self.sistema.publicar_pedido(pedido(destinatario="ba-1"))
        p = self.sistema.tomar_pedido("r1", 0)
        self.sistema.responder(p.id, "OK", {}, "r1", "python")
        self.assertIsNone(self.sistema.tomar_respuesta("ba-2", 0))
        self.assertIsNotNone(self.sistema.tomar_respuesta("ba-1", 0))

    def test_la_segunda_respuesta_del_mismo_pedido_se_descarta(self):
        self.sistema.publicar_pedido(pedido())
        p = self.sistema.tomar_pedido("r1", 0)
        self.sistema.responder(p.id, "OK", {}, "r1", "python")
        self.assertEqual(self.sistema.responder(p.id, "OK", {}, "r2", "python"),
                         (False, "desconocido"))

    def test_recuperar_le_avisa_al_balanceador_en_vez_de_dejarlo_esperando(self):
        """Mejor un 504 con motivo en cuanto se sabe que un cliente colgado
        hasta su propio timeout sin que nadie le diga por qué."""
        self.sistema.publicar_pedido(pedido(operacion="POST /personas", idempotente=False))
        self.sistema.tomar_pedido("replica-muerta", 0)
        time.sleep(0.15)
        reasignados, fallados, _ = self.sistema.recuperar()
        self.assertEqual(reasignados, 0)
        self.assertEqual(len(fallados), 1)
        r = self.sistema.tomar_respuesta("balanceador@casa-tomas", 0)
        self.assertEqual(r["estado"], "DEADLINE_EXCEEDED")
        self.assertIn("error", r["contenido"])

    def test_el_estado_cruza_consumidores_para_health(self):
        """El balanceador ya no puede contar cuántas atendió cada réplica: los
        pedidos no pasan por él. Las cuenta la cola, indexadas por el mismo
        `host:puerto` que el pool usa como destino."""
        self.sistema.publicar_pedido(pedido(id="a"))
        self.sistema.publicar_pedido(pedido(id="b"))
        self.sistema.tomar_pedido("10.0.0.1:8080", 0)
        p = self.sistema.tomar_pedido("10.0.0.1:8080", 0)
        self.sistema.responder(p.id, "OK", {}, "10.0.0.1:8080", "python")
        visto = self.sistema.estado()["consumidores"]["10.0.0.1:8080"]
        self.assertEqual(visto["enVuelo"], 1)
        self.assertEqual(visto["atendidos"], 1)
        self.assertIsNotNone(visto["ultimoPedidoHaceMs"])


if __name__ == "__main__":
    unittest.main(verbosity=2)


# =====================================================================
# 4.1-4.5 — los seams que hacen replicable a colas.py
# =====================================================================
def ahora_ms():
    return int(time.time() * 1000)


class PruebasRelojInyectado(unittest.TestCase):
    """Design Decision 2, Seam A — an injected clock and a required `encolado_en_ms`.

    Without this, two nodes applying the same `encolar` entry build pedidos with
    different `encolado_en`, and therefore report different `esperaMs` for the
    same request: observable divergence in a contract field.
    """

    def test_el_pedido_acepta_un_reloj_inyectado(self):
        reloj = lambda: 1_000_000
        p = Pedido(id="p1", operacion="GET /personas", parametros={}, idempotente=True,
                   destinatario="balanceador@casa-tomas",
                   vence_en_ms=1_005_000, encolado_en_ms=1_000_000, reloj_ms=reloj)

        self.assertAlmostEqual(p.queda(), 5.0, places=3)

    def test_encolado_en_ms_es_obligatorio(self):
        """A `default_factory` here is a clock read, and a clock read is divergence."""
        with self.assertRaises(TypeError):
            Pedido(id="p1", operacion="GET /", parametros={}, idempotente=True,
                   destinatario="b", vence_en_ms=1)

    def test_dos_nodos_con_el_mismo_payload_dan_el_mismo_esperaMs(self):
        reloj = lambda: 2_000_000
        campos = dict(id="p1", operacion="GET /", parametros={}, idempotente=True,
                      destinatario="b", vence_en_ms=2_005_000, encolado_en_ms=1_999_000)
        uno = Sistema(10, 10, reserva=1, ttl_respuestas=30, reloj_ms=reloj)
        otro = Sistema(10, 10, reserva=1, ttl_respuestas=30, reloj_ms=reloj)

        for sistema in (uno, otro):
            sistema.publicar_pedido(Pedido(**campos, reloj_ms=reloj))
            sistema.reservar_pedido("p1", "casa-A", 2_001_000)
            sistema.responder("p1", "OK", {}, "casa-A", "python")

        self.assertEqual(uno.tomar_respuesta("b", 0), otro.tomar_respuesta("b", 0))


class PruebasInspeccionarFrente(unittest.TestCase):
    """4.2 — a pure look at the head of the queue, mutating nothing.

    The destructive `_proximo_vivo()` had to go: under replication, noticing an
    expiry cannot itself be a mutation, because the mutation has to travel
    through the log first.
    """

    def setUp(self):
        self.cola = ColaPedidos(cota=10, reserva=1)

    def test_cola_vacia(self):
        self.assertEqual(self.cola.inspeccionar_frente(ahora_ms()), ("vacio", None))

    def test_frente_vivo_devuelve_su_id_sin_sacarlo(self):
        self.cola.publicar(pedido())

        self.assertEqual(self.cola.inspeccionar_frente(ahora_ms()), ("vivo", "p1"))
        self.assertEqual(self.cola.estado()["esperando"], 1, "inspecting removed it")

    def test_frente_vencido_se_informa_sin_sacarlo(self):
        self.cola.publicar(pedido(presupuesto=-1))

        clase, ids = self.cola.inspeccionar_frente(ahora_ms())

        self.assertEqual(clase, "vencidos")
        self.assertEqual(ids, ["p1"])
        self.assertEqual(self.cola.estado()["esperando"], 1)

    def test_es_idempotente(self):
        self.cola.publicar(pedido())
        primera = self.cola.inspeccionar_frente(ahora_ms())

        self.assertEqual(self.cola.inspeccionar_frente(ahora_ms()), primera)
        self.assertEqual(self.cola.inspeccionar_frente(ahora_ms()), primera)

    def test_excluidos_se_saltean(self):
        """An id already named by an appended-but-unapplied entry is not offered."""
        self.cola.publicar(pedido(id="p1"))
        self.cola.publicar(pedido(id="p2"))

        self.assertEqual(self.cola.inspeccionar_frente(ahora_ms(), excluidos={"p1"}),
                         ("vivo", "p2"))


class PruebasDecidirYAplicarExpiry(unittest.TestCase):
    """4.3 — expiry splits into a pure decision and a pure application."""

    def setUp(self):
        self.cola = ColaPedidos(cota=10, reserva=1)

    def test_la_decision_no_muta_nada(self):
        self.cola.publicar(pedido(presupuesto=-1))
        antes = self.cola.estado()

        decision = self.cola.detectar_vencidos(ahora_ms())

        self.assertEqual(self.cola.estado(), antes)
        self.assertEqual([f["id"] for f in decision["fallar"]], ["p1"])

    def test_la_decision_lleva_ids_y_no_predicados(self):
        """A predicate re-evaluated on another node at another instant is a
        different decision. Only explicit id lists travel."""
        self.cola.publicar(pedido(presupuesto=-1))
        decision = self.cola.detectar_vencidos(ahora_ms())

        self.assertIn("reencolar", decision)
        self.assertIn("fallar", decision)
        self.assertIn("decididoEnMs", decision)
        self.assertTrue(all(isinstance(i, str) for i in decision["reencolar"]))

    def test_aplicar_dos_veces_es_no_op_la_segunda(self):
        """Replay safety: the same entry may be applied twice after a failover."""
        self.cola.publicar(pedido(presupuesto=-1))
        decision = self.cola.detectar_vencidos(ahora_ms())

        primera = self.cola.aplicar_expiry(decision)
        segunda = self.cola.aplicar_expiry(decision)

        self.assertEqual(len(primera), 1)
        self.assertEqual(segunda, [], "applying the same decision twice acted twice")

    def test_la_regla_de_reintento_se_conserva(self):
        """Contract, unchanged: idempotent requeues, non-idempotent fails."""
        self.cola.publicar(pedido(id="lectura", idempotente=True))
        self.cola.publicar(pedido(id="alta", idempotente=False))
        self.cola.tomar("r1", 0)
        self.cola.tomar("r1", 0)
        time.sleep(1.1)

        decision = self.cola.detectar_vencidos(ahora_ms())

        self.assertEqual(decision["reencolar"], ["lectura"])
        self.assertEqual([f["id"] for f in decision["fallar"]], ["alta"])

    def test_purgar_tambien_se_parte_en_decidir_y_aplicar(self):
        respuestas = ColaRespuestas(cota=10, ttl=0.05)
        respuestas.publicar("b", {"id": "r1"})
        time.sleep(0.1)

        purga = respuestas.detectar_purgables(ahora_ms())
        self.assertEqual(purga, {"b": 1})

        self.assertEqual(respuestas.aplicar_purga(purga), 1)
        self.assertEqual(respuestas.aplicar_purga(purga), 0, "purged twice")


class PruebasSistemaExpiry(unittest.TestCase):
    """4.4 y 4.5 — the `Sistema` surface the apply step calls, and atomicity."""

    def setUp(self):
        self.sistema = Sistema(cota_pedidos=10, cota_respuestas=10,
                               reserva=0.1, ttl_respuestas=30)

    def test_decidir_expiry_y_expirar_reemplazan_a_recuperar(self):
        self.sistema.publicar_pedido(pedido(operacion="POST /personas", idempotente=False))
        self.sistema.reservar_pedido("p1", "replica-muerta", ahora_ms() - 1)

        decision = self.sistema.decidir_expiry(ahora_ms())
        reencolados, fallados, purgadas = self.sistema.expirar(decision)

        self.assertEqual(reencolados, 0)
        self.assertEqual(len(fallados), 1)
        r = self.sistema.tomar_respuesta("balanceador@casa-tomas", 0)
        self.assertEqual(r["estado"], "DEADLINE_EXCEEDED")

    def test_reservar_pedido_toma_por_id_y_no_por_turno(self):
        """The apply step replays a decision already made; it does not re-choose."""
        self.sistema.publicar_pedido(pedido(id="p1"))
        self.sistema.publicar_pedido(pedido(id="p2"))

        p = self.sistema.reservar_pedido("p2", "casa-A", ahora_ms() + 1000)

        self.assertIsNotNone(p)
        self.assertEqual(p.id, "p2")
        self.assertTrue(self.sistema.pedido_en_vuelo("p2"))
        self.assertFalse(self.sistema.pedido_en_vuelo("p1"))

    def test_reservar_un_id_que_no_esta_es_no_op_tolerante(self):
        self.assertIsNone(self.sistema.reservar_pedido("fantasma", "casa-A", 0))

    def test_retirar_respuesta_saca_una_sola(self):
        self.sistema.publicar_pedido(pedido())
        self.sistema.responder("p1", "OK", {}, "casa-A", "python")

        self.assertIsNotNone(self.sistema.retirar_respuesta("balanceador@casa-tomas"))
        self.assertIsNone(self.sistema.retirar_respuesta("balanceador@casa-tomas"))

    def test_responder_no_muta_nada_si_el_destinatario_esta_saturado(self):
        """Spec: responder's two mutations are indivisible.

        Today the pedido is popped and *then* saturation is discovered, which is
        a half-apply. Under replication that divergence is silent and permanent.
        """
        chico = Sistema(cota_pedidos=10, cota_respuestas=1, reserva=1, ttl_respuestas=30)
        chico.publicar_pedido(pedido(id="ocupa"))
        chico.responder("ocupa", "OK", {}, "casa-A", "python")   # llena la cota
        chico.publicar_pedido(pedido(id="p2"))

        aceptada, motivo = chico.responder("p2", "OK", {}, "casa-A", "python")

        self.assertFalse(aceptada)
        self.assertEqual(motivo, "destinatario-saturado")
        self.assertIsNotNone(chico.pedidos.completar("p2"),
                             "the pedido was popped despite the response being refused")
