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


def _ahora_epoch_ms():
    """Milisegundos de época. Los vencimientos se replican, así que no pueden
    ser monótonos: un valor monótono sólo significa algo adentro del proceso que
    lo produjo, y hay que poder compararlos entre procesos distintos."""
    return int(time.time() * 1000)


@dataclass
class Pedido:
    """Una request HTTP esperando a que una réplica la atienda.

    A diferencia de la versión anterior, acá no hay ningún `callable`: el pedido
    tiene que poder viajar como JSON hasta otro proceso, en otra máquina, que
    quizá ni siquiera esté escrito en Python. Por eso la operación es un string
    y los parámetros un diccionario.

    `vence_en_ms` es un solo presupuesto para todo el viaje, espera en la cola
    incluida: un pedido que esperó 4 s tiene 1 s para que lo atiendan, no 5.

    Es un instante **absoluto en milisegundos de época**, sellado por el master
    al agregar la entrada al log, y cada nodo lo guarda tal cual. Un slave que
    guardara el `vence_en` monótono del master tendría una cola entera de
    vencimientos de una época ajena, y al promoverse estarían todos mal por un
    margen impredecible.

    Lo que viaja al worker sigue siendo cuánto le queda (`quedaMs`) y nunca un
    instante: los relojes de cuatro casas siguen fuera de la cuenta del
    presupuesto. Lo que cambia es la exposición, mucho más chica, de tres nodos
    discrepando sobre la hora, y sólo en el instante del failover.
    """

    id: str
    operacion: str                  # "POST /personas", "GET /personas", ...
    parametros: dict                # lo que necesita esa operación, ya validado
    idempotente: bool               # False sólo para POST /personas
    destinatario: str               # quién espera la respuesta: "balanceador@casa-tomas"
    vence_en_ms: int                # época ms; lo sella el master
    encolado_en_ms: int             # época ms; obligatorio, nunca un default_factory
    cliente: str | None = None      # IP de quien hizo la request
    intentos: list = field(default_factory=list)   # consumidores que lo tomaron, en orden
    reservado_por: str | None = None
    reservado_hasta_ms: int | None = None
    # Inyectable para que un test no dependa del reloj de pared. Fuera del
    # repr y de la igualdad: es una dependencia, no un dato del pedido.
    reloj_ms: object = field(default=_ahora_epoch_ms, repr=False, compare=False)

    def queda(self):
        """Segundos de presupuesto que le quedan. Negativo si ya venció."""
        return (self.vence_en_ms - self.reloj_ms()) / 1000

    def como_json(self):
        """Lo que ve el worker. No lleva vencimientos: son instantes absolutos
        y lo único que le sirve al worker es cuánto le queda."""
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

    def __init__(self, cota, reserva, reloj_ms=_ahora_epoch_ms):
        self.cota = cota
        self.reserva = reserva          # segundos que un worker tiene para contestar
        self.reloj_ms = reloj_ms
        self._esperando = deque()       # los que nadie tomó todavía
        self._en_vuelo = {}             # id -> Pedido tomado y sin contestar
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
                ahora_ms = self.reloj_ms()
                clase, dato = self._inspeccionar(ahora_ms, ())
                if clase == "vivo":
                    hasta = min(ahora_ms + int(self.reserva * 1000),
                                self._buscar(dato).vence_en_ms)
                    return self._reservar(dato, consumidor, hasta)
                # "vencidos" no se entrega ni se limpia acá: sacarlos es una
                # mutación, y bajo replicación toda mutación viaja por el log.
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
            pedido.reservado_hasta_ms = None
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
        return self.aplicar_expiry(self.detectar_vencidos(self.reloj_ms()))

    # -- expiry, partido en decidir (impuro) y aplicar (determinista) --

    def detectar_vencidos(self, ahora_ms):
        """Qué habría que vencer, sin tocar nada. Sólo lo corre el master.

        Devuelve listas de ids explícitas y nunca predicados: un predicado
        reevaluado en otro nodo, en otro instante, es una decisión distinta. Lo
        que viaja por el log es la decisión ya tomada.
        """
        reencolar, fallar = [], []
        with self._hay:
            for pedido in self._en_vuelo.values():
                if pedido.reservado_hasta_ms is not None and pedido.reservado_hasta_ms > ahora_ms:
                    continue
                quien = pedido.reservado_por
                if pedido.vence_en_ms <= ahora_ms:
                    fallar.append({"id": pedido.id, "estado": "DEADLINE_EXCEEDED",
                                   "detalle": f"venció mientras lo atendía {quien}"})
                elif pedido.idempotente:
                    reencolar.append(pedido.id)
                else:
                    fallar.append({"id": pedido.id, "estado": "DEADLINE_EXCEEDED",
                                   "detalle": f"{quien} no contestó y la operación "
                                              f"no es idempotente"})
            # Los que siguen esperando turno y ya no llegan a tiempo. Se fallan
            # acá y no cuando alguien los tome: sacarlos ya libera lugar en la
            # cola para pedidos que sí pueden llegar a tiempo.
            for pedido in self._esperando:
                if pedido.vence_en_ms <= ahora_ms:
                    fallar.append({"id": pedido.id, "estado": "DEADLINE_EXCEEDED",
                                   "detalle": "venció esperando en la cola"})
        return {"reencolar": reencolar, "fallar": fallar, "decididoEnMs": ahora_ms}

    def aplicar_expiry(self, decision):
        """Ejecuta una decisión ya tomada. Determinista y repetible.

        Aplicarla dos veces no hace nada la segunda: cada id se busca antes de
        tocarlo y, si ya no está, se saltea. Hace falta porque una entrada del
        log puede reaplicarse después de un failover.
        """
        fallidos = []
        with self._hay:
            for id in decision.get("reencolar", ()):
                pedido = self._en_vuelo.pop(id, None)
                if pedido is None:
                    continue
                pedido.reservado_por = None
                pedido.reservado_hasta_ms = None
                self._esperando.appendleft(pedido)
                self.reasignados += 1
                self._hay.notify()
            for caso in decision.get("fallar", ()):
                pedido = self._sacar(caso["id"])
                if pedido is None:
                    continue
                pedido.reservado_por = None
                pedido.reservado_hasta_ms = None
                fallidos.append((pedido, caso["estado"], caso["detalle"]))
        return fallidos

    def inspeccionar_frente(self, ahora_ms, excluidos=()):
        """Qué hay al frente, sin sacarlo: ("vacio"|"vencidos"|"vivo", dato).

        Reemplaza al viejo `_proximo_vivo()`, que sacaba los vencidos del camino
        y los apartaba. Bajo replicación eso no se puede: notar un vencimiento
        no puede ser, en sí mismo, una mutación — la mutación tiene que pasar
        primero por el log.
        """
        with self._hay:
            return self._inspeccionar(ahora_ms, excluidos)

    def reservar(self, id, consumidor, reservado_hasta_ms):
        """Reserva un pedido puntual. None si no estaba esperando.

        Toma por id y no por turno porque el paso de aplicación repite una
        decisión ya tomada por el master: no vuelve a elegir.
        """
        with self._hay:
            return self._reservar(id, consumidor, reservado_hasta_ms)

    def en_vuelo(self, id):
        with self._hay:
            return id in self._en_vuelo

    def obtener(self, id):
        """El pedido, esté esperando o en vuelo. None si no está."""
        with self._hay:
            pedido = self._en_vuelo.get(id)
            return pedido if pedido is not None else self._buscar(id)

    def esperar_cambio(self, timeout):
        """El long-poll de siempre. Despierta cuando el paso de aplicación avisa.

        Los que esperan se despiertan sólo con estado ya comprometido, porque el
        `notify` sale del aplicador y el aplicador sólo toca entradas commiteadas.
        """
        with self._hay:
            self._hay.wait(timeout=timeout)

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

    def _inspeccionar(self, ahora_ms, excluidos):
        """Sin candado propio: lo llaman con `_hay` tomado."""
        vencidos = []
        for pedido in self._esperando:
            if pedido.id in excluidos:
                continue
            if pedido.vence_en_ms > ahora_ms:
                return "vivo", pedido.id
            vencidos.append(pedido.id)
        if vencidos:
            return "vencidos", vencidos
        return "vacio", None

    def _reservar(self, id, consumidor, reservado_hasta_ms):
        for i, pedido in enumerate(self._esperando):
            if pedido.id == id:
                del self._esperando[i]
                pedido.reservado_por = consumidor
                pedido.reservado_hasta_ms = reservado_hasta_ms
                pedido.intentos.append(consumidor)
                self._en_vuelo[pedido.id] = pedido
                return pedido
        return None

    def _buscar(self, id):
        for pedido in self._esperando:
            if pedido.id == id:
                return pedido
        return None

    def _sacar(self, id):
        """Lo saca de donde esté. None si ya no está en ninguna de las dos."""
        pedido = self._en_vuelo.pop(id, None)
        if pedido is not None:
            return pedido
        for i, p in enumerate(self._esperando):
            if p.id == id:
                del self._esperando[i]
                return p
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

    def __init__(self, cota, ttl, reloj_ms=_ahora_epoch_ms):
        self.cota = cota                # por destinatario, no total
        self.ttl = ttl
        self.reloj_ms = reloj_ms
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
            pendientes.append((self.reloj_ms(), respuesta))
            self.publicadas += 1
            # notify_all y no notify: los que esperan son de destinatarios
            # distintos y el que despierte puede no ser el dueño de esta.
            self._hay.notify_all()
            return True

    def espiar(self, destinatario):
        """La próxima respuesta de ese destinatario, sin sacarla. None si no hay.

        La saca el paso de aplicación, no el handler: el handler sólo necesita
        saber qué va a salir para poder devolverla cuando la entrada commitee.
        """
        with self._hay:
            pendientes = self._por_destinatario.get(destinatario)
            return pendientes[0][1] if pendientes else None

    def esperar_cambio(self, timeout):
        with self._hay:
            self._hay.wait(timeout=timeout)

    def saturado(self, destinatario):
        """¿Rechazaría una respuesta más para ese destinatario?

        Lo consulta `Sistema.responder()` antes de sacar el pedido, para no
        dejar un medio-apply si la respuesta no entra.
        """
        with self._hay:
            pendientes = self._por_destinatario.get(destinatario)
            return pendientes is not None and len(pendientes) >= self.cota

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
        """Tira las respuestas que nadie recolectó a tiempo. Devuelve cuántas.

        Envoltorio de decidir+aplicar, para un nodo solo y para los tests.
        """
        ahora_ms = self.reloj_ms()
        return self.aplicar_purga(self.detectar_purgables(ahora_ms),
                                  corte_ms=ahora_ms - int(self.ttl * 1000))

    def detectar_purgables(self, ahora_ms):
        """Cuántas hay para tirar por destinatario. No toca nada."""
        corte = ahora_ms - int(self.ttl * 1000)
        purga = {}
        with self._hay:
            for destinatario, pendientes in self._por_destinatario.items():
                cuantas = sum(1 for instante, _ in pendientes if instante < corte)
                if cuantas:
                    purga[destinatario] = cuantas
        return purga

    def aplicar_purga(self, purga, corte_ms=None):
        """Tira lo que dice la decisión. Devuelve cuántas tiró.

        Con `corte_ms`, sólo tira las anteriores a ese instante, y por eso
        reaplicar la misma decisión no tira de más. Sin él tira por cantidad,
        que alcanza para un nodo solo.
        """
        tiradas = 0
        with self._hay:
            for destinatario, cuantas in (purga or {}).items():
                pendientes = self._por_destinatario.get(destinatario)
                if not pendientes:
                    continue
                for _ in range(cuantas):
                    if not pendientes:
                        break
                    if corte_ms is not None and pendientes[0][0] >= corte_ms:
                        break
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

    def __init__(self, cota_pedidos, cota_respuestas, reserva, ttl_respuestas,
                 reloj_ms=_ahora_epoch_ms):
        self.reloj_ms = reloj_ms
        self.pedidos = ColaPedidos(cota_pedidos, reserva, reloj_ms)
        self.respuestas = ColaRespuestas(cota_respuestas, ttl_respuestas, reloj_ms)
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
        with self.pedidos._hay:
            pedido = self.pedidos._en_vuelo.get(id) or self.pedidos._buscar(id)
            if pedido is None:
                return False, "desconocido"
            # La saturación se evalúa ANTES de sacar el pedido. Antes se sacaba
            # primero y se descubría después, que es un medio-apply: el pedido
            # desaparecía y la respuesta no se publicaba. Con un solo nodo era
            # un pedido perdido; replicado, es divergencia silenciosa y
            # permanente entre nodos que aplicaron la misma entrada.
            if self.respuestas.saturado(pedido.destinatario):
                return False, "destinatario-saturado"
            pedido = self.pedidos.completar(id)

        if atendido_por:
            with self._lock:
                self.atendidos[atendido_por] = self.atendidos.get(atendido_por, 0) + 1
        respuesta = self._armar(pedido, estado, contenido, atendido_por, app)
        self.respuestas.publicar(pedido.destinatario, respuesta)
        return True, "entregada"

    def recuperar(self):
        """Un ciclo de mantenimiento. Devuelve (reencolados, fallados, purgadas).

        Los pedidos que no se pueden reintentar salen de acá como respuestas de
        error dirigidas a su balanceador: mejor un 504 con motivo en cuanto se
        sabe que dejar al cliente esperando hasta su propio timeout sin que nadie
        le diga por qué.
        """
        return self.expirar(self.decidir_expiry(self.reloj_ms()))

    def decidir_expiry(self, ahora_ms):
        """La decisión completa de un ciclo de mantenimiento. No muta nada.

        Sólo la corre el master: los slaves no vencen nada por su cuenta, porque
        con tres relojes distintos tomarían tres decisiones distintas en el mismo
        instante y divergirían.
        """
        decision = self.pedidos.detectar_vencidos(ahora_ms)
        decision["purgar"] = self.respuestas.detectar_purgables(ahora_ms)
        decision["corteRespuestasMs"] = ahora_ms - int(self.respuestas.ttl * 1000)
        return decision

    def expirar(self, decision):
        """Aplica una decisión ya tomada. Devuelve (reencolados, fallados, purgadas).

        Las dos mutaciones —sacar el pedido y publicar su respuesta de error— son
        una sola entrada del log y se aplican juntas, así que ningún lector puede
        ver un estado intermedio donde el pedido ya no está y la respuesta no
        llegó.
        """
        antes = self.pedidos.reasignados
        fallados = []
        for pedido, estado, detalle in self.pedidos.aplicar_expiry(decision):
            respuesta = self._armar(pedido, estado, {"error": detalle}, None, None)
            self.respuestas.publicar(pedido.destinatario, respuesta)
            fallados.append((pedido, estado, detalle))
        purgadas = self.respuestas.aplicar_purga(decision.get("purgar"),
                                                 decision.get("corteRespuestasMs"))
        return self.pedidos.reasignados - antes, fallados, purgadas

    # -- superficie que usa el paso de aplicación del log --

    def reservar_pedido(self, id, consumidor, reservado_hasta_ms):
        return self.pedidos.reservar(id, consumidor, reservado_hasta_ms)

    def retirar_respuesta(self, destinatario):
        """Saca una respuesta sin esperar. None si no había."""
        return self.respuestas.tomar(destinatario, 0)

    def pedido_en_vuelo(self, id):
        return self.pedidos.en_vuelo(id)

    def obtener_pedido(self, id):
        return self.pedidos.obtener(id)

    def espiar_respuesta(self, destinatario):
        return self.respuestas.espiar(destinatario)

    def esperar_pedidos(self, timeout):
        self.pedidos.esperar_cambio(timeout)

    def esperar_respuestas(self, timeout):
        self.respuestas.esperar_cambio(timeout)

    def saturado(self, destinatario):
        return self.respuestas.saturado(destinatario)

    def inspeccionar_frente(self, ahora_ms, excluidos=()):
        return self.pedidos.inspeccionar_frente(ahora_ms, excluidos)

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
            "esperaMs": max(int(self.reloj_ms() - pedido.encolado_en_ms), 0),
        }
