"""Las dos colas del sistema: la de pedidos y la de respuestas.

El balanceador publica en **pedidos** y consume de **respuestas**. Los workers
—que ahora viven adentro de cada réplica— hacen exactamente lo contrario. Nadie
elige a quién le toca: el worker que esté libre toma el próximo pedido. Es el
mismo modelo de consumidores que compiten que teníamos con los hilos adentro del
balanceador, pero ahora la competencia cruza la red.

Dos colas y no una sola bidireccional porque son dos flujos con dueños
distintos: en `pedidos` hay N consumidores que compiten por el mismo elemento,
en `respuestas` cada elemento tiene **un** destinatario y nadie más lo puede
tomar. Mezclarlos obligaría a que cada consumidor filtre lo que no es suyo.

Este módulo es sólo la estructura de datos, sin HTTP: así se prueba sin abrir un
socket. El servicio que la expone está en `servidor.py`.

Lo que hace que esto sea una cola y no una lista: la **reserva**. Un pedido que
un worker toma no se borra, queda "en vuelo" con un vencimiento. Si el worker no
contesta antes, el pedido vuelve al frente y otro lo toma. Es lo que reemplaza
al `devolver_al_frente()` que antes hacía el worker cuando el gRPC le fallaba:
ahora el worker está en la réplica y una réplica que se murió no puede devolver
nada, así que la devolución la tiene que hacer la cola sola.
"""

import threading
import time
from collections import deque
from dataclasses import dataclass, field


# Cada cuánto vuelve a mirar quien está bloqueado esperando. No hace falta que
# sea corto para la latencia (el que publica despierta al que espera con un
# notify), sólo para que un cliente que se fue no quede colgado para siempre.
LATIDO = 0.5


@dataclass
class Pedido:
    """Una request HTTP esperando a que una réplica la atienda.

    A diferencia de la versión anterior, acá no hay ningún `callable`: el pedido
    tiene que poder viajar como JSON hasta otro proceso, en otra máquina, que
    quizá ni siquiera esté escrito en Python. Por eso la operación es un string
    y los parámetros un diccionario.

    `vence_en` es un solo presupuesto para todo el viaje, espera en la cola
    incluida: un pedido que esperó 4 s tiene 1 s para que lo atiendan, no 5.
    Se guarda en el reloj monótono **de este proceso**, y lo que viaja al worker
    es cuánto le queda (`quedaMs`), no un instante absoluto: así los relojes
    desincronizados de cuatro casas no entran en la cuenta.
    """

    id: str
    operacion: str                  # "POST /personas", "GET /personas", ...
    parametros: dict                # lo que necesita esa operación, ya validado
    idempotente: bool               # False sólo para POST /personas
    destinatario: str               # quién espera la respuesta: "balanceador@casa-tomas"
    vence_en: float                 # time.monotonic() + presupuesto
    cliente: str | None = None      # IP de quien hizo la request
    encolado_en: float = field(default_factory=time.monotonic)
    intentos: list = field(default_factory=list)   # consumidores que lo tomaron, en orden
    reservado_por: str | None = None
    reservado_hasta: float | None = None

    def queda(self):
        """Segundos de presupuesto que le quedan. Negativo si ya venció."""
        return self.vence_en - time.monotonic()

    def como_json(self):
        """Lo que ve el worker. No lleva `vence_en` ni `reservado_hasta`: son
        instantes del reloj de este proceso y allá no significarían nada."""
        return {
            "id": self.id,
            "operacion": self.operacion,
            "parametros": self.parametros,
            "idempotente": self.idempotente,
            "cliente": self.cliente,
            "quedaMs": max(int(self.queda() * 1000), 0),
            "intento": len(self.intentos),
        }


class ColaPedidos:
    """FIFO acotada con devolución al frente y reserva por consumidor.

    `deque` y no `queue.Queue` por lo mismo de siempre: `Queue` no sabe insertar
    al frente, y devolver al frente es justamente lo que hace que un pedido
    reasignado no pierda su lugar detrás de los que llegaron después.

    Un solo `Condition` hace de candado y de campana: quien publica avisa, quien
    toma espera. Sin candado, dos workers podrían llevarse el mismo pedido.
    """

    def __init__(self, cota, reserva):
        self.cota = cota
        self.reserva = reserva          # segundos que un worker tiene para contestar
        self._esperando = deque()       # los que nadie tomó todavía
        self._en_vuelo = {}             # id -> Pedido tomado y sin contestar
        self._a_fallar = deque()        # (pedido, estado, detalle) que el recuperador drena
        self._hay = threading.Condition()
        self.publicados = 0
        self.reasignados = 0
        # Última vez que cada consumidor vino a pedir trabajo. Es la única forma
        # que tiene este proceso de saber qué réplicas están realmente
        # consumiendo: nadie se registra, se nota porque preguntan.
        self.vistos = {}

    # -- lado del balanceador --

    def publicar(self, pedido):
        """Encola al final. Devuelve False si está llena.

        Nunca bloquea: el balanceador prefiere contestar 503 en el acto a tener
        al cliente esperando por un lugar en la cola *además* de por su
        respuesta.
        """
        with self._hay:
            if len(self._esperando) >= self.cota:
                return False
            self._esperando.append(pedido)
            self.publicados += 1
            self._hay.notify()
            return True

    # -- lado del worker --

    def tomar(self, consumidor, espera):
        """Saca el primero y lo reserva. Devuelve None si no hubo nada en `espera` segundos.

        La reserva vence a los `self.reserva` segundos, o cuando se agota el
        presupuesto del pedido si eso pasa antes: reservarlo más allá de su
        vencimiento sería retener algo que ya nadie va a leer.
        """
        limite = time.monotonic() + espera
        with self._hay:
            self.vistos[consumidor] = time.monotonic()
            while True:
                pedido = self._proximo_vivo()
                if pedido is not None:
                    ahora = time.monotonic()
                    pedido.reservado_por = consumidor
                    pedido.reservado_hasta = min(ahora + self.reserva, pedido.vence_en)
                    pedido.intentos.append(consumidor)
                    self._en_vuelo[pedido.id] = pedido
                    return pedido
                restante = limite - time.monotonic()
                if restante <= 0:
                    return None
                self._hay.wait(timeout=min(restante, LATIDO))

    def devolver(self, id, consumidor=None):
        """El worker lo suelta a propósito (se está apagando, no puede atenderlo).

        Vuelve al frente, no al final: ya esperó una vez. Devuelve False si ese
        id no estaba en vuelo — pasa cuando la reserva venció y el recuperador
        se le adelantó.
        """
        with self._hay:
            pedido = self._en_vuelo.get(id)
            if pedido is None or (consumidor and pedido.reservado_por != consumidor):
                return False
            del self._en_vuelo[id]
            pedido.reservado_por = None
            pedido.reservado_hasta = None
            self._esperando.appendleft(pedido)
            self._hay.notify()
            return True

    def completar(self, id):
        """Alguien contestó: saca el pedido de donde esté y lo devuelve.

        Lo busca también entre los que están esperando y no sólo entre los que
        están en vuelo, porque una respuesta puede llegar *después* de que el
        recuperador lo haya reencolado: el worker tardó más que la reserva pero
        contestó igual. En ese caso gana la primera respuesta y el pedido sale
        de la cola, que es mejor para el cliente que descartarla y hacerlo
        esperar una segunda vuelta.

        Devuelve None si ese id no existe: es la segunda respuesta de un pedido
        que se atendió dos veces, y hay que tirarla.
        """
        with self._hay:
            pedido = self._en_vuelo.pop(id, None)
            if pedido is not None:
                return pedido
            for i, p in enumerate(self._esperando):
                if p.id == id:
                    del self._esperando[i]
                    return p
            return None

    # -- mantenimiento --

    def recuperar(self):
        """Devuelve la lista de (pedido, estado, detalle) que hay que dar por fallidos.

        Reencola sola a las que sí se pueden reintentar. Es el corazón de la
        tolerancia a fallos de este diseño: con los workers afuera, una réplica
        que se muere con un pedido en la mano no avisa nada, así que la única
        forma de recuperar ese pedido es que la cola note que la reserva venció.

        La regla de reintento es la misma de siempre, y es contrato:

          * el pedido vencido esperando en la cola no se reintenta, se falla;
          * la reserva vencida de una operación **idempotente** se reencola: la
            réplica pudo no haber hecho nada, y repetir una lectura no cuesta;
          * la reserva vencida de una escritura (`POST /personas`) **no** se
            reencola: "no contestó" no dice si alcanzó a ejecutarse, y repetirla
            puede crear la persona dos veces. Preferimos un 504 honesto a un
            duplicado silencioso.
        """
        ahora = time.monotonic()
        fallidos = []
        with self._hay:
            while self._a_fallar:
                fallidos.append(self._a_fallar.popleft())

            for pedido in list(self._en_vuelo.values()):
                if pedido.reservado_hasta > ahora:
                    continue
                del self._en_vuelo[pedido.id]
                quien = pedido.reservado_por
                pedido.reservado_por = None
                pedido.reservado_hasta = None
                if pedido.queda() <= 0:
                    fallidos.append((pedido, "DEADLINE_EXCEEDED",
                                     f"venció mientras lo atendía {quien}"))
                elif pedido.idempotente:
                    self._esperando.appendleft(pedido)
                    self.reasignados += 1
                    self._hay.notify()
                else:
                    fallidos.append((pedido, "DEADLINE_EXCEEDED",
                                     f"{quien} no contestó y la operación no es idempotente"))

            # Los que siguen esperando turno y ya no llegan a tiempo. Se fallan
            # acá y no cuando alguien los tome: sacarlos ya libera lugar en la
            # cola para pedidos que sí pueden llegar a tiempo.
            vivos = deque()
            for pedido in self._esperando:
                if pedido.queda() <= 0:
                    fallidos.append((pedido, "DEADLINE_EXCEEDED", "venció esperando en la cola"))
                else:
                    vivos.append(pedido)
            self._esperando = vivos
        return fallidos

    def estado(self):
        ahora = time.monotonic()
        with self._hay:
            por_consumidor = {}
            for p in self._en_vuelo.values():
                por_consumidor[p.reservado_por] = por_consumidor.get(p.reservado_por, 0) + 1
            return {
                "esperando": len(self._esperando),
                "enVuelo": len(self._en_vuelo),
                "cota": self.cota,
                "reservaSegundos": self.reserva,
                "publicados": self.publicados,
                "reasignados": self.reasignados,
                "enVueloPorConsumidor": por_consumidor,
                "ultimoPedidoHaceMs": {c: int((ahora - t) * 1000) for c, t in self.vistos.items()},
            }

    # -- interno --

    def _proximo_vivo(self):
        """El primero que todavía tiene presupuesto. Los vencidos que encuentra
        en el camino los aparta para que el recuperador los falle: entregarle a
        un worker un pedido que ya venció es gastarle una réplica al pedo."""
        while self._esperando:
            pedido = self._esperando.popleft()
            if pedido.queda() > 0:
                return pedido
            self._a_fallar.append((pedido, "DEADLINE_EXCEEDED", "venció esperando en la cola"))
        return None


class ColaRespuestas:
    """Las respuestas listas, agrupadas por destinatario.

    Es una cola y no un diccionario de resultados porque el balanceador la
    consume como cola: un hilo recolector pide "dame la próxima que sea para
    mí" y despacha. Pero está indexada por destinatario porque en la Etapa 3 hay
    dos balanceadores contra la misma cola, y uno no puede llevarse la respuesta
    que el otro está esperando. Con una FIFO sola, el segundo balanceador se
    comería las respuestas del primero y los dos clientes esperarían de más.

    `ttl` existe para el caso feo: un balanceador que se murió sin recolectar.
    Sus respuestas no las va a leer nadie nunca, y sin vencimiento harían crecer
    la memoria de este proceso hasta que alguien lo note.
    """

    def __init__(self, cota, ttl):
        self.cota = cota                # por destinatario, no total
        self.ttl = ttl
        self._por_destinatario = {}     # destinatario -> deque de (instante, respuesta)
        self._hay = threading.Condition()
        self.publicadas = 0
        self.descartadas = 0

    def publicar(self, destinatario, respuesta):
        """Deja la respuesta para su dueño. False si ese destinatario está saturado.

        Saturado quiere decir que hace rato que no recolecta: casi siempre, que
        se cayó. Rechazar es preferible a crecer sin límite, y el worker ya sabe
        qué hacer con un 503 (reintentar o soltarlo).
        """
        with self._hay:
            pendientes = self._por_destinatario.setdefault(destinatario, deque())
            if len(pendientes) >= self.cota:
                return False
            pendientes.append((time.monotonic(), respuesta))
            self.publicadas += 1
            # notify_all y no notify: los que esperan son de destinatarios
            # distintos y el que despierte puede no ser el dueño de esta.
            self._hay.notify_all()
            return True

    def tomar(self, destinatario, espera):
        """La próxima respuesta de ese destinatario. None si no hubo en `espera` segundos."""
        limite = time.monotonic() + espera
        with self._hay:
            while True:
                pendientes = self._por_destinatario.get(destinatario)
                if pendientes:
                    return pendientes.popleft()[1]
                restante = limite - time.monotonic()
                if restante <= 0:
                    return None
                self._hay.wait(timeout=min(restante, LATIDO))

    def purgar(self):
        """Tira las respuestas que nadie recolectó a tiempo. Devuelve cuántas."""
        corte = time.monotonic() - self.ttl
        tiradas = 0
        with self._hay:
            for destinatario, pendientes in list(self._por_destinatario.items()):
                while pendientes and pendientes[0][0] < corte:
                    pendientes.popleft()
                    tiradas += 1
                if not pendientes:
                    del self._por_destinatario[destinatario]
        self.descartadas += tiradas
        return tiradas

    def estado(self):
        with self._hay:
            return {
                "pendientes": sum(len(p) for p in self._por_destinatario.values()),
                "porDestinatario": {d: len(p) for d, p in self._por_destinatario.items()},
                "cota": self.cota,
                "ttlSegundos": self.ttl,
                "publicadas": self.publicadas,
                "descartadas": self.descartadas,
            }


class Sistema:
    """Las dos colas juntas, que es como se usan siempre.

    Existe porque las tres operaciones interesantes tocan las dos: publicar una
    respuesta cierra un pedido, recuperar un pedido vencido produce una
    respuesta de error, y el estado que mira `/health` es el de ambas. Tenerlo
    en un solo objeto es también lo que fija el orden en que se toman los
    candados —siempre pedidos y después respuestas, nunca al revés—, que es la
    única forma de que dos hilos no se traben entre sí.
    """

    def __init__(self, cota_pedidos, cota_respuestas, reserva, ttl_respuestas):
        self.pedidos = ColaPedidos(cota_pedidos, reserva)
        self.respuestas = ColaRespuestas(cota_respuestas, ttl_respuestas)
        self.atendidos = {}          # consumidor -> cuántas contestó, para /health
        self._lock = threading.Lock()

    def publicar_pedido(self, pedido):
        return self.pedidos.publicar(pedido)

    def tomar_pedido(self, consumidor, espera):
        return self.pedidos.tomar(consumidor, espera)

    def devolver_pedido(self, id, consumidor=None):
        return self.pedidos.devolver(id, consumidor)

    def responder(self, id, estado, contenido, atendido_por=None, app=None):
        """El worker contestó. Cierra el pedido y deja la respuesta para su dueño.

        Devuelve (aceptada, motivo). No se acepta la respuesta de un pedido que
        ya no existe: es la segunda de un pedido que se atendió dos veces —pasa
        cuando una réplica lenta contesta después de que la reserva venció y
        otra ya lo resolvió— y entregarla haría que el balanceador despache una
        respuesta que nadie espera.

        El `destinatario` y los `intentos` los pone la cola con lo que guardó
        del pedido, no el worker: el worker no tiene por qué saber quién le pidió
        ni por cuántas réplicas pasó antes el pedido, y si se lo preguntáramos
        podría mentir.
        """
        pedido = self.pedidos.completar(id)
        if pedido is None:
            return False, "desconocido"
        if atendido_por:
            with self._lock:
                self.atendidos[atendido_por] = self.atendidos.get(atendido_por, 0) + 1
        respuesta = self._armar(pedido, estado, contenido, atendido_por, app)
        if not self.respuestas.publicar(pedido.destinatario, respuesta):
            return False, "destinatario-saturado"
        return True, "entregada"

    def recuperar(self):
        """Un ciclo de mantenimiento. Devuelve (reencolados, fallados, purgadas).

        Los pedidos que no se pueden reintentar salen de acá como respuestas de
        error dirigidas a su balanceador: mejor un 504 con motivo en cuanto se
        sabe que dejar al cliente esperando hasta su propio timeout sin que nadie
        le diga por qué.
        """
        antes = self.pedidos.reasignados
        fallados = []
        for pedido, estado, detalle in self.pedidos.recuperar():
            respuesta = self._armar(pedido, estado, {"error": detalle}, None, None)
            self.respuestas.publicar(pedido.destinatario, respuesta)
            fallados.append((pedido, estado, detalle))
        return self.pedidos.reasignados - antes, fallados, self.respuestas.purgar()

    def tomar_respuesta(self, destinatario, espera):
        return self.respuestas.tomar(destinatario, espera)

    def estado(self):
        """Lo que muestran `/estado` de la cola y `/health` del balanceador.

        Los consumidores salen en un solo mapa y no en tres paralelos porque el
        balanceador los cruza con su pool por `destino`: un mapa por réplica se
        recorre una vez, tres obligan a tres lookups y a acordarse de los tres.
        """
        pedidos = self.pedidos.estado()
        en_vuelo = pedidos.pop("enVueloPorConsumidor")
        visto = pedidos.pop("ultimoPedidoHaceMs")
        with self._lock:
            atendidos = dict(self.atendidos)
        consumidores = {}
        for destino in set(en_vuelo) | set(visto) | set(atendidos):
            consumidores[destino] = {
                "enVuelo": en_vuelo.get(destino, 0),
                "atendidos": atendidos.get(destino, 0),
                "ultimoPedidoHaceMs": visto.get(destino),
            }
        return {
            "pedidos": pedidos,
            "respuestas": self.respuestas.estado(),
            "consumidores": consumidores,
        }

    def _armar(self, pedido, estado, contenido, atendido_por, app):
        return {
            "id": pedido.id,
            "operacion": pedido.operacion,
            "estado": estado,
            "contenido": contenido if isinstance(contenido, dict) else {},
            "atendidoPor": atendido_por,
            "app": app,
            "intentos": list(pedido.intentos),
            "esperaMs": int((time.monotonic() - pedido.encolado_en) * 1000),
        }
