#!/usr/bin/env python3
"""El sistema de colas — un proceso aparte del balanceador.

Dos colas y nada más:

    POST /pedidos            el balanceador publica un pedido
    POST /pedidos/tomar      un worker de una réplica se lleva el próximo
    POST /pedidos/devolver   un worker lo suelta sin atenderlo
    POST /respuestas         un worker deja la respuesta
    POST /respuestas/tomar   el balanceador recolecta las suyas
    GET  /health             ¿está viva?
    GET  /estado             el detalle, para /health del balanceador y la consola

    python3 cola/servidor.py
    COLA_PUERTO=8085 COLA_BIND=100.101.15.93 python3 cola/servidor.py

**Por qué un proceso aparte.** Antes la cola era un `deque` adentro del
balanceador y los workers eran hilos suyos. Sacarla cambia quién depende de
quién: el balanceador deja de saber qué réplicas hay y cómo se les habla, y las
réplicas dejan de ser servidores pasivos para ir a buscar trabajo. El precio,
que hay que decirlo en el informe, es un salto de red más por pedido y un
componente nuevo que puede caerse; a cambio, agregar una réplica ya no es
reconfigurar el balanceador, es prender un worker más que se pone a consumir.

**Por qué `tomar` es POST y no GET.** Saca el elemento de la cola: cambia el
estado del servidor. Un GET que muta es el tipo de cosa que un reintento
automático de cualquier cliente HTTP convierte en pedidos perdidos.

**Por qué long-poll y no que el worker pregunte cada tanto.** `tomar` se queda
colgado hasta `espera` segundos esperando que aparezca algo. Preguntar en un
bucle sería o latencia (si pregunta poco) o tráfico al pedo (si pregunta mucho);
colgarse es las dos cosas bien y cuesta un hilo, que es lo que sobra acá.
"""

import json
import os
import random
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aplicar import Aplicador
from colas import Pedido, Sistema
from motor import COMPROMETIDO, DESTITUIDO, Motor
from raft import Entrada, NodoRaft

# --- Configuración ---------------------------------------------------------
# Todo por entorno con default, como en el resto del sistema.

PUERTO = int(os.environ.get("COLA_PUERTO", "8085"))

# Por defecto sólo loopback. Las réplicas están en otras casas, así que en la
# demo esto se abre a la IP de Tailscale de la Plataforma (COLA_BIND). El
# default cerrado es a propósito: un arranque distraído no deja las colas del
# servicio escuchando en toda la red.
BIND = os.environ.get("COLA_BIND", "127.0.0.1")

CASA = os.environ.get("COLA_CASA", "casa-tomas")
NOMBRE = os.environ.get("COLA_NOMBRE", "cola")

DIRECTORIO_LOGS = os.environ.get("COLA_LOGS", "logs")
ARCHIVO_BITACORA = os.path.join(DIRECTORIO_LOGS, f"bitacora-{NOMBRE}-{CASA}.log")

# Cuántos pedidos pueden esperar turno. Pasado eso, 503 al balanceador en el
# acto: aceptar un pedido que casi seguro va a vencer es peor que rechazarlo,
# porque el cliente igual espera el presupuesto entero para recibir un error.
COTA_PEDIDOS = int(os.environ.get("COLA_COTA_PEDIDOS", "100"))

# Cuántas respuestas sin recolectar se le aguantan a un destinatario. Es alto a
# propósito: rechazar una respuesta deja a un cliente esperando al pedo. La cota
# está para que un balanceador muerto no haga crecer la memoria sin fin.
COTA_RESPUESTAS = int(os.environ.get("COLA_COTA_RESPUESTAS", "1000"))

# Cuánto tiempo tiene un worker para contestar un pedido que tomó antes de que
# la cola lo dé por perdido y se lo ofrezca a otro. Tiene que ser bastante menor
# que el presupuesto del pedido (5 s), o cuando se note que la réplica murió ya
# no quedaría tiempo para reintentar en otra.
RESERVA = float(os.environ.get("COLA_RESERVA", "2"))

# Cuánto sobrevive una respuesta que nadie recolecta.
TTL_RESPUESTAS = float(os.environ.get("COLA_TTL_RESPUESTAS", "60"))

# Cota del long-poll: nadie retiene un hilo del servidor más que esto.
ESPERA_MAXIMA = float(os.environ.get("COLA_ESPERA_MAXIMA", "30"))

# Presupuesto máximo que se le acepta a un pedido. Sin esto, un cliente podría
# ocupar un lugar de la cola por horas.
PRESUPUESTO_MAXIMO = float(os.environ.get("COLA_PRESUPUESTO_MAXIMO", "60"))

# Cada cuánto corre el recuperador: reservas vencidas, pedidos sin presupuesto y
# respuestas que nadie fue a buscar.
INTERVALO_RECUPERADOR = float(os.environ.get("COLA_INTERVALO_RECUPERADOR", "0.25"))

# Token compartido. Vacío = sin autenticación (sólo para probar en loopback).
# En la demo va con token: sin él, cualquiera en el tailnet podría publicar
# pedidos falsos o —peor— **tomarlos** y quedarse con tráfico real de usuarios.
TOKEN = os.environ.get("COLA_TOKEN", "")

# Publicar, consumir y hablar el protocolo del clúster no son el mismo permiso,
# así que no comparten secreto. Los tres caen a COLA_TOKEN cuando no se definen,
# que es lo que deja migrar un despliegue existente sin tocarle la configuración.
#
# El tercero no es simetría burocrática: sin él, cualquiera que alcance el puerto
# puede postularse candidato o inyectar entradas en el log, que es la diferencia
# entre replicar y ser replicado por un desconocido.
TOKEN_PUBLICADOR = os.environ.get("COLA_TOKEN_PUBLICADOR", TOKEN)
TOKEN_CONSUMIDOR = os.environ.get("COLA_TOKEN_CONSUMIDOR", TOKEN)
TOKEN_CLUSTER = os.environ.get("COLA_TOKEN_CLUSTER", TOKEN)

CONTRATO = "1.0"
INSTANCIA = os.environ.get("COLA_INSTANCIA", f"{NOMBRE}-{PUERTO}@{CASA}")
MI_URL = os.environ.get("COLA_URL", f"http://{BIND}:{PUERTO}")

# Lista de pares, separada por comas y sin contarse a sí mismo. Vacía = nodo
# solo, que es mayoría de uno y master de entrada (no hay rama especial en el
# camino de datos: es la misma aritmética de `mayoria`).
PARES = [u.strip().rstrip("/") for u in os.environ.get("COLA_PARES", "").split(",")
         if u.strip() and u.strip().rstrip("/") != MI_URL.rstrip("/")]

RAFT_HEARTBEAT_MS = int(os.environ.get("RAFT_HEARTBEAT_MS", "150"))
RAFT_ELECCION_TIMEOUT_MS = int(os.environ.get("RAFT_ELECCION_TIMEOUT_MS", "600"))

# Cuánto espera una escritura a que la mayoría tenga su entrada. Es un techo,
# no una latencia esperada: en un clúster sano cuesta un round-trip.
ESPERA_COMMIT = float(os.environ.get("COLA_ESPERA_COMMIT", "5"))


SISTEMA = Sistema(COTA_PEDIDOS, COTA_RESPUESTAS, RESERVA, TTL_RESPUESTAS)
ARRANCADO = None

RAFT = NodoRaft(yo=MI_URL, pares=PARES, reloj=lambda: int(time.monotonic() * 1000),
                azar=random.Random(), timeout_eleccion_ms=RAFT_ELECCION_TIMEOUT_MS,
                heartbeat_ms=RAFT_HEARTBEAT_MS)

# `NodoRaft` no es thread-safe a propósito: meterle locks adentro habría puesto
# concurrencia en la lógica que se testea, y los tests dejarían de ser
# deterministas. El candado vive acá, que es donde está la concurrencia real.
LOCK_RAFT = threading.RLock()

APLICADOR = Aplicador(SISTEMA)

# Siempre hay motor, incluso con un nodo: es el único que aplica entradas, y un
# nodo solo es mayoría de uno, así que sus propuestas se comprometen en el acto.
MOTOR = None

# Lo setea `motor.py` cuando hay red que atender.
DESPACHAR = None


def despachar(mensajes):
    """Hand outbound Raft messages to the motor, if one is wired."""
    if DESPACHAR is not None and mensajes:
        DESPACHAR(mensajes)


# Qué token abre cada ruta. La tabla es la autorización: si una ruta no está
# acá, no existe, y agregar una obliga a decidir de quién es.
CLASE_DE_TOKEN = {
    "/pedidos": "publicador",
    "/respuestas/tomar": "publicador",
    "/pedidos/tomar": "consumidor",
    "/pedidos/devolver": "consumidor",
    "/respuestas": "consumidor",
    "/raft/appendEntries": "cluster",
    "/raft/requestVote": "cluster",
    "/raft/estado": "cluster",
    "/estado": "publicador",
}

# Las cinco rutas de datos, y sólo ellas, contestan 421 cuando este nodo no es
# el master. `/health`, `/health/vivo` y `/raft/*` nunca lo hacen.
RUTAS_DE_DATOS = frozenset((
    "/pedidos", "/pedidos/tomar", "/pedidos/devolver",
    "/respuestas", "/respuestas/tomar",
))


def _ahora_iso():
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


_LOCK_BITACORA = threading.Lock()


def bitacora(operacion, codigo, detalle=None):
    """Una línea por evento, con el mismo formato que el balanceador y las réplicas.

    Que sean iguales no es estética: es lo que permite tomar un `req=` de la
    bitácora del balanceador y seguirlo por la de la cola y la de la réplica que
    lo atendió, que es la evidencia que se muestra en la demo.
    """
    linea = " | ".join([_ahora_iso(), f"{NOMBRE}@{CASA}", operacion, str(codigo), detalle or "-"])
    try:
        with _LOCK_BITACORA:
            os.makedirs(DIRECTORIO_LOGS, exist_ok=True)
            with open(ARCHIVO_BITACORA, "a", encoding="utf-8") as f:
                f.write(linea + "\n")
    except OSError as e:
        print(f"[bitacora] no se pudo escribir: {e}", flush=True)
    print(linea, flush=True)


def recuperador():
    """El hilo que hace que un pedido no se pierda cuando muere quien lo tomó.

    Es el único componente que puede notarlo: el worker está en la réplica y una
    réplica que se murió no avisa. Sin este hilo, un pedido tomado por una
    réplica que se cae se queda en vuelo para siempre y el cliente espera hasta
    su propio timeout sin que nadie sepa por qué.
    """
    while True:
        try:
            reasignados, fallados, purgadas = SISTEMA.recuperar()
            for pedido, estado, detalle in fallados:
                bitacora("recuperar", estado, f"req={pedido.id} {pedido.operacion} {detalle}")
            if reasignados:
                bitacora("recuperar", "REASIGNADO", f"n={reasignados} vuelven al frente de la cola")
            if purgadas:
                bitacora("purgar", "OK", f"n={purgadas} respuestas que nadie recolectó")
        except Exception as e:  # un bug acá no puede matar el hilo que salva pedidos
            print(f"[recuperador] {type(e).__name__}: {e}", flush=True)
        time.sleep(INTERVALO_RECUPERADOR)


def acotar_espera(valor):
    try:
        return max(0.0, min(float(valor), ESPERA_MAXIMA))
    except (TypeError, ValueError):
        return ESPERA_MAXIMA


class Manejador(BaseHTTPRequestHandler):
    server_version = "cola/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass  # el registro lo lleva la bitácora, con el formato del contrato

    # -- utilidades --

    def responder(self, codigo, cuerpo):
        # Un 204 va SIN cuerpo, y no es un detalle de estilo. Los clientes HTTP
        # saben que un 204 no lo tiene y no lo leen del socket: los bytes que
        # igual escribamos quedan ahí y el cliente los toma como la línea de
        # estado de la respuesta siguiente. Sobre una conexión keep-alive eso se
        # ve como `BadStatusLine: {}HTTP/1.1 204` en una request que estaba bien,
        # y es lo que rompía al recolector después del primer long-poll vacío.
        if codigo == 204:
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        crudo = json.dumps(cuerpo, ensure_ascii=False).encode()
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(crudo)))
        self.end_headers()
        self.wfile.write(crudo)

    def cuerpo_json(self):
        largo = int(self.headers.get("Content-Length") or 0)
        if not largo:
            return {}
        try:
            datos = json.loads(self.rfile.read(largo).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None
        return datos if isinstance(datos, dict) else None

    def autorizado(self, clase):
        """Check the token class this route needs, not merely "a" token.

        A consumer token that could publish would let a worker inject pedidos;
        a data token that could reach `/raft/*` would let it join the cluster.
        """
        esperado = {"publicador": TOKEN_PUBLICADOR,
                    "consumidor": TOKEN_CONSUMIDOR,
                    "cluster": TOKEN_CLUSTER}[clase]
        return not esperado or self.headers.get("X-Cola-Token") == esperado

    def soy_master(self):
        return RAFT.rol == "master"

    def proponer(self, operacion, payload, limite):
        """Agrega la entrada y espera la mayoría. False si ya contestó por su cuenta.

        Los tres desenlaces que no son "comprometido" tienen respuesta propia, y
        ninguno deja al que llamó esperando: destituido contesta 421 con el
        master nuevo, y vencido contesta 204, que en estas rutas ya significa
        "ahora no hay nada" y no obliga a cambiar nada del contrato.
        """
        resultado = MOTOR.proponer_y_esperar(operacion, payload, limite)
        if resultado == COMPROMETIDO:
            return True
        if resultado == DESTITUIDO:
            self.redirigir()
        else:
            self.responder(204, {})
        return False

    def redirigir(self):
        """421 Misdirected Request: literally "you asked a server that cannot
        answer this". Distinct from the two `409`s on /respuestas, so a client
        branching on the status alone cannot confuse a redirect with a discard.
        """
        self.responder(421, {"error": "no-soy-master",
                             "master": RAFT.master_conocido})

    def ruta(self):
        return self.path.split("?")[0].rstrip("/") or "/"

    # -- pedidos --

    def publicar_pedido(self, cuerpo):
        """Lo llama el balanceador. 202 aceptado, 503 si la cola está llena."""
        operacion = cuerpo.get("operacion")
        destinatario = cuerpo.get("destinatario")
        if not operacion or not destinatario:
            return self.responder(400, {"error": "faltan operacion y/o destinatario"})
        parametros = cuerpo.get("parametros")
        if parametros is not None and not isinstance(parametros, dict):
            return self.responder(400, {"error": "parametros tiene que ser un objeto"})
        try:
            presupuesto = min(float(cuerpo.get("presupuestoMs", 5000)) / 1000, PRESUPUESTO_MAXIMO)
        except (TypeError, ValueError):
            return self.responder(400, {"error": "presupuestoMs tiene que ser un número"})
        if presupuesto <= 0:
            return self.responder(400, {"error": "presupuestoMs tiene que ser positivo"})

        ahora_ms = SISTEMA.reloj_ms()
        pedido = Pedido(
            id=str(cuerpo.get("id") or uuid.uuid4().hex),
            operacion=operacion,
            parametros=parametros or {},
            # El default es el seguro: ante la duda, no se reintenta. Un pedido
            # marcado idempotente por error se puede ejecutar dos veces.
            idempotente=bool(cuerpo.get("idempotente", False)),
            destinatario=destinatario,
            # Época absoluta, no monótono: el vencimiento se replica, y un
            # valor monótono sólo significa algo dentro de este proceso.
            vence_en_ms=ahora_ms + int(presupuesto * 1000),
            encolado_en_ms=ahora_ms,
            cliente=cuerpo.get("cliente"),
            reloj_ms=SISTEMA.reloj_ms,
        )
        # La cota se mira antes de agregar la entrada: llenar el log con pedidos
        # que se van a rechazar es replicar basura.
        estado = SISTEMA.pedidos.estado()
        if estado["esperando"] >= estado["cota"]:
            bitacora("POST /pedidos", 503,
                     f"req={pedido.id} cola llena ({estado['esperando']}/{estado['cota']})")
            return self.responder(503, {"error": "cola llena", "esperando": estado["esperando"],
                                        "cota": estado["cota"]})

        payload = {"id": pedido.id, "operacion": operacion,
                   "parametros": pedido.parametros, "idempotente": pedido.idempotente,
                   "destinatario": destinatario, "cliente": pedido.cliente,
                   "venceEnMs": pedido.vence_en_ms, "encoladoEnMs": pedido.encolado_en_ms}
        # El 202 sale DESPUÉS de la mayoría: es la promesa de que ese pedido
        # sobrevive a la caída de un nodo, y antes del commit no se puede hacer.
        if not self.proponer("encolar", payload, time.monotonic() + ESPERA_COMMIT):
            return
        bitacora("POST /pedidos", 202, f"req={pedido.id} {operacion} para={destinatario}")
        self.responder(202, {"id": pedido.id, "encolado": True})

    def tomar_pedido(self, cuerpo):
        """Lo llama el worker de una réplica. 200 con el pedido, 204 si no hubo nada.

        El `consumidor` es el `host:puerto` gRPC de la réplica, el mismo string
        que el balanceador usa como `destino` en el pool. No es capricho: es lo
        que hace que `atendidosPorConsumidor` y `enVueloPorConsumidor` se puedan
        cruzar con /health del balanceador sin traducir nada.
        """
        consumidor = cuerpo.get("consumidor")
        if not consumidor:
            return self.responder(400, {"error": "falta consumidor"})

        limite = time.monotonic() + acotar_espera(cuerpo.get("espera", ESPERA_MAXIMA))
        SISTEMA.pedidos.vistos[consumidor] = time.monotonic()

        primera = True
        while primera or time.monotonic() < limite:
            # Se mira siempre al menos una vez: `espera: 0` significa "fijate y
            # contestame ya", no "no te fijes".
            primera = False
            ahora_ms = SISTEMA.reloj_ms()
            clase, dato = SISTEMA.inspeccionar_frente(ahora_ms, MOTOR.ids_propuestos())

            if clase == "vacio":
                SISTEMA.esperar_pedidos(min(limite - time.monotonic(), 0.5))
                continue

            if clase == "vencidos":
                # El frente está muerto pero todavía no se comprometió su
                # vencimiento. Entregarlo sería gastarle una réplica a algo que
                # nadie va a leer, así que se espera a que el `expirar` commitee.
                resultado = MOTOR.barrer_vencidos(espera_s=max(0.05, limite - time.monotonic()))
                if resultado == DESTITUIDO:
                    return self.redirigir()
                continue

            pedido = SISTEMA.obtener_pedido(dato)
            if pedido is None:
                continue                      # se lo llevaron entre medio
            payload = {"id": dato, "consumidor": consumidor,
                       "reservadoHastaMs": min(ahora_ms + int(RESERVA * 1000),
                                               pedido.vence_en_ms)}
            # El presupuesto del long-poll dice cuánto esperar a que APAREZCA
            # trabajo, no cuánto esperar a que se confirme la reserva. Una vez
            # propuesta, hay que esperarla: abandonarla no la cancela —se
            # commitea igual y deja el pedido reservado para un worker que ya
            # recibió 204, perdido hasta que venza la reserva.
            resultado = MOTOR.proponer_y_esperar(
                "tomar", payload, max(limite, time.monotonic() + ESPERA_COMMIT))
            if resultado == DESTITUIDO:
                return self.redirigir()
            if resultado != COMPROMETIDO:
                break
            entregado = SISTEMA.obtener_pedido(dato)
            if entregado is None or not SISTEMA.pedido_en_vuelo(dato):
                continue                      # perdimos la carrera, a mirar de nuevo
            bitacora("POST /pedidos/tomar", 200,
                     f"req={entregado.id} {entregado.operacion} tomado={consumidor} "
                     f"intento={len(entregado.intentos)}")
            return self.responder(200, entregado.como_json())

        self.responder(204, {})

    def devolver_pedido(self, cuerpo):
        """El worker lo suelta a propósito: se está apagando o no lo puede atender.

        Existe para el drenado de un blue/green: la réplica que se va devuelve lo
        que tenía en la mano en vez de hacer esperar a la cola los segundos de la
        reserva. Es una optimización, no una garantía: si la réplica no llega a
        devolverlo, el recuperador lo hace igual, sólo que más tarde.
        """
        id = cuerpo.get("id")
        if not id:
            return self.responder(400, {"error": "falta id"})
        if not SISTEMA.pedido_en_vuelo(id):
            return self.responder(409, {"resultado": "no-estaba-en-vuelo"})
        payload = {"id": id, "consumidor": cuerpo.get("consumidor")}
        if not self.proponer("devolver", payload, time.monotonic() + ESPERA_COMMIT):
            return
        bitacora("POST /pedidos/devolver", 200, f"req={id} lo soltó {cuerpo.get('consumidor')}")
        self.responder(200, {"resultado": "devuelto"})

    # -- respuestas --

    def publicar_respuesta(self, cuerpo):
        """Lo llama el worker cuando terminó. El destinatario lo pone la cola."""
        id = cuerpo.get("id")
        estado = cuerpo.get("estado")
        if not id or not estado:
            return self.responder(400, {"error": "faltan id y/o estado"})
        contenido = cuerpo.get("contenido")
        if contenido is not None and not isinstance(contenido, dict):
            return self.responder(400, {"error": "contenido tiene que ser un objeto"})
        # Los dos rechazos se deciden acá y no viajan por el log: una respuesta
        # que se descarta no es una mutación que haya que replicar.
        pedido = SISTEMA.obtener_pedido(id)
        aceptada, motivo = True, "entregada"
        if pedido is None:
            aceptada, motivo = False, "desconocido"
        elif SISTEMA.saturado(pedido.destinatario):
            aceptada, motivo = False, "destinatario-saturado"
        if not aceptada:
            # "desconocido" es el caso normal de una respuesta repetida: la
            # réplica lenta contestó después de que otra ya lo resolvió. No es
            # un error del que responde, así que se registra pero no se grita.
            bitacora("POST /respuestas", 409, f"req={id} descartada: {motivo}")
            return self.responder(409, {"resultado": motivo})

        payload = {"id": id, "estado": str(estado), "contenido": contenido or {},
                   "atendidoPor": cuerpo.get("atendidoPor"), "app": cuerpo.get("app")}
        if not self.proponer("responder", payload, time.monotonic() + ESPERA_COMMIT):
            return
        bitacora("POST /respuestas", 202,
                 f"req={id} {estado} de={cuerpo.get('atendidoPor') or '?'}")
        self.responder(202, {"resultado": "entregada"})

    def tomar_respuesta(self, cuerpo):
        """Lo llama el recolector del balanceador. 200 con la respuesta, 204 si no hubo."""
        destinatario = cuerpo.get("destinatario")
        if not destinatario:
            return self.responder(400, {"error": "falta destinatario"})
        limite = time.monotonic() + acotar_espera(cuerpo.get("espera", ESPERA_MAXIMA))
        primera = True
        while primera or time.monotonic() < limite:
            primera = False
            respuesta = SISTEMA.espiar_respuesta(destinatario)
            if respuesta is None:
                SISTEMA.esperar_respuestas(min(limite - time.monotonic(), 0.5))
                continue
            # Retirarla también es una mutación: si no se replicara, el nodo que
            # se promueva después la volvería a entregar.
            resultado = MOTOR.proponer_y_esperar(
                "retirar-respuesta", {"destinatario": destinatario},
                max(limite, time.monotonic() + ESPERA_COMMIT))
            if resultado == DESTITUIDO:
                return self.redirigir()
            if resultado != COMPROMETIDO:
                break
            return self.responder(200, respuesta)

        self.responder(204, {})

    # -- observación --

    def salud(self):
        """200 mientras el proceso atienda. No mira si hay workers consumiendo:
        una cola sin consumidores está sana y llenándose, que es información
        distinta y sale en /estado."""
        estado = SISTEMA.estado()
        self.responder(200, {
            "cola": "sana",
            "casa": CASA,
            "instancia": INSTANCIA,
            "contrato": CONTRATO,
            "arrancado": ARRANCADO,
            # Quién manda, para que el balanceador y los workers se muden solos.
            # `masterConocido` en null significa elección en curso: no es un
            # error, es la única respuesta honesta mientras no haya master.
            "rol": RAFT.rol,
            "termino": RAFT.termino_actual,
            "masterConocido": RAFT.master_conocido,
            "esperando": estado["pedidos"]["esperando"],
            "enVuelo": estado["pedidos"]["enVuelo"],
            "cota": estado["pedidos"]["cota"],
            "respuestasPendientes": estado["respuestas"]["pendientes"],
        })

    def vivo(self):
        """Liveness, and nothing else: 200 while the process serves HTTP.

        Deliberately blind to role, term and master. It is what the container
        HEALTHCHECK asks, and a node in the middle of an election is perfectly
        alive — restarting it there would be exactly the wrong move.
        """
        self.responder(200, {"vivo": True})

    # -- protocolo del clúster --

    def raft_append(self, cuerpo):
        cuerpo = dict(cuerpo)
        cuerpo["entradas"] = [
            Entrada(indice=e["indice"], termino=e["termino"],
                    operacion=e["operacion"], payload=e.get("payload") or {})
            for e in (cuerpo.get("entradas") or [])
        ]
        with LOCK_RAFT:
            respuesta, salientes = RAFT.recibir_append(cuerpo)
        despachar(salientes)
        self.responder(200, respuesta)

    def raft_voto(self, cuerpo):
        with LOCK_RAFT:
            respuesta, salientes = RAFT.recibir_solicitud_voto(cuerpo)
        despachar(salientes)
        self.responder(200, respuesta)

    def raft_estado(self):
        with LOCK_RAFT:
            self.responder(200, RAFT.instantanea())

    def estado(self):
        datos = SISTEMA.estado()
        datos["casa"] = CASA
        datos["arrancado"] = ARRANCADO
        self.responder(200, datos)

    # -- ruteo --

    def do_GET(self):
        ruta = self.ruta()
        if ruta == "/health":
            return self.salud()
        if ruta == "/health/vivo":
            return self.vivo()
        if ruta == "/raft/estado":
            if not self.autorizado("cluster"):
                return self.responder(403, {"error": "token inválido"})
            return self.raft_estado()
        if ruta in ("/", "/estado"):
            if not self.autorizado("publicador"):
                return self.responder(403, {"error": "token inválido"})
            return self.estado()
        self.responder(404, {"error": "no existe"})

    def do_POST(self):
        ruta = self.ruta()
        # El cuerpo se lee SIEMPRE y antes que nada, aunque la ruta no exista o
        # el token esté mal. Con keep-alive, los bytes que no se leen quedan en
        # el buffer del socket y el servidor los toma como la línea de pedido de
        # la request siguiente: la conexión queda envenenada y el próximo pedido
        # muere con un 400 que no tiene nada que ver con lo que mandó.
        cuerpo = self.cuerpo_json()
        rutas = {
            "/pedidos": self.publicar_pedido,
            "/pedidos/tomar": self.tomar_pedido,
            "/pedidos/devolver": self.devolver_pedido,
            "/respuestas": self.publicar_respuesta,
            "/respuestas/tomar": self.tomar_respuesta,
            "/raft/appendEntries": self.raft_append,
            "/raft/requestVote": self.raft_voto,
        }
        manejar = rutas.get(ruta)
        if manejar is None:
            return self.responder(404, {"error": "no existe"})
        if not self.autorizado(CLASE_DE_TOKEN[ruta]):
            bitacora(f"POST {ruta}", 403, f"ip={self.client_address[0]} token inválido")
            return self.responder(403, {"error": "token inválido"})
        if cuerpo is None:
            return self.responder(400, {"error": "cuerpo no es un objeto JSON"})
        # El redirect va ANTES del handler, así que un nodo que no manda no
        # muta nada ni cuenta nada para una mayoría antes de contestar.
        if ruta in RUTAS_DE_DATOS:
            if not self.soy_master():
                return self.redirigir()
            # 503 y no 421: este nodo SÍ es el master, sólo que todavía no sabe
            # qué heredó. Dura un round-trip de commit.
            if MOTOR is not None and MOTOR.recuperando:
                return self.responder(503, {"error": "recuperando"})
        manejar(cuerpo)


class Servidor(ThreadingHTTPServer):
    """Igual que el de la biblioteca, pero sin el traceback por conexión cortada.

    Con long-polls de 20 s abiertos todo el tiempo, un cliente que se va —un
    worker que se apaga, un balanceador que reinicia— deja siempre un
    `BrokenPipeError`. Con el traceback puesto, la salida del contenedor queda
    llena de errores que no son errores y la bitácora de verdad no se lee.
    """

    def handle_error(self, request, direccion):
        import sys as _sys
        tipo = _sys.exc_info()[0]
        if tipo is not None and issubclass(tipo, (ConnectionError, TimeoutError)):
            return
        super().handle_error(request, direccion)


def arrancar_motor():
    """Wire the engine. Always — a lone node needs it too.

    Even with no peers, the applier thread is what turns committed entries into
    queue state, and it is the only writer to `Sistema` on every node. A single
    node is a majority of one, so its proposals commit immediately; nothing
    about the data path branches on how many peers there are.
    """
    global DESPACHAR, MOTOR
    MOTOR = Motor(RAFT, token=TOKEN_CLUSTER, lock=LOCK_RAFT,
                  intervalo_ms=max(10, RAFT_HEARTBEAT_MS // 3),
                  registrar=bitacora, sistema=SISTEMA, aplicador=APLICADOR,
                  intervalo_recuperador_s=INTERVALO_RECUPERADOR)
    DESPACHAR = MOTOR.enviar
    MOTOR.arrancar()
    return MOTOR


def main():
    global ARRANCADO
    ARRANCADO = _ahora_iso()
    # El barrido de vencimientos lo corre el motor, y sólo en el master: un
    # slave que venciera por su cuenta divergiría contra su propio reloj.
    arrancar_motor()

    servidor = Servidor((BIND, PUERTO), Manejador)
    # Un hilo por conexión y todas daemon: los long-poll se quedan colgados hasta
    # 30 s y no queremos que el apagado espere a que venzan.
    servidor.daemon_threads = True
    bitacora("arranque", "OK",
             f"escucha={BIND}:{PUERTO} cotaPedidos={COTA_PEDIDOS} "
             f"cotaRespuestas={COTA_RESPUESTAS} reserva={RESERVA}s ttl={TTL_RESPUESTAS}s "
             f"token={'sí' if TOKEN else 'NO'} "
             f"instancia={INSTANCIA} pares={len(PARES)} "
             f"modo={'clúster' if PARES else 'nodo solo'}")
    if not TOKEN:
        print("[cola] sin COLA_TOKEN: cualquiera que alcance el puerto puede "
              "publicar y tomar pedidos", flush=True)
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        bitacora("apagado", "OK", "SIGINT")
        servidor.shutdown()


if __name__ == "__main__":
    main()
