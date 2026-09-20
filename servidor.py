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
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from colas import Pedido, Sistema

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
# Pendiente de la próxima vuelta: un token para el balanceador y otro para los
# workers, porque publicar y consumir no son el mismo permiso.
TOKEN = os.environ.get("COLA_TOKEN", "")


SISTEMA = Sistema(COTA_PEDIDOS, COTA_RESPUESTAS, RESERVA, TTL_RESPUESTAS)
ARRANCADO = None


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

    def autorizado(self):
        return not TOKEN or self.headers.get("X-Cola-Token") == TOKEN

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

        pedido = Pedido(
            id=str(cuerpo.get("id") or uuid.uuid4().hex),
            operacion=operacion,
            parametros=parametros or {},
            # El default es el seguro: ante la duda, no se reintenta. Un pedido
            # marcado idempotente por error se puede ejecutar dos veces.
            idempotente=bool(cuerpo.get("idempotente", False)),
            destinatario=destinatario,
            vence_en=time.monotonic() + presupuesto,
            cliente=cuerpo.get("cliente"),
        )
        if not SISTEMA.publicar_pedido(pedido):
            estado = SISTEMA.pedidos.estado()
            bitacora("POST /pedidos", 503,
                     f"req={pedido.id} cola llena ({estado['esperando']}/{estado['cota']})")
            return self.responder(503, {"error": "cola llena", "esperando": estado["esperando"],
                                        "cota": estado["cota"]})
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
        pedido = SISTEMA.tomar_pedido(consumidor, acotar_espera(cuerpo.get("espera", ESPERA_MAXIMA)))
        if pedido is None:
            return self.responder(204, {})
        bitacora("POST /pedidos/tomar", 200,
                 f"req={pedido.id} {pedido.operacion} tomado={consumidor} "
                 f"intento={len(pedido.intentos)}")
        self.responder(200, pedido.como_json())

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
        if not SISTEMA.devolver_pedido(id, cuerpo.get("consumidor")):
            return self.responder(409, {"resultado": "no-estaba-en-vuelo"})
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
        aceptada, motivo = SISTEMA.responder(id, str(estado), contenido or {},
                                             cuerpo.get("atendidoPor"), cuerpo.get("app"))
        if not aceptada:
            # "desconocido" es el caso normal de una respuesta repetida: la
            # réplica lenta contestó después de que otra ya lo resolvió. No es
            # un error del que responde, así que se registra pero no se grita.
            bitacora("POST /respuestas", 409, f"req={id} descartada: {motivo}")
            return self.responder(409, {"resultado": motivo})
        bitacora("POST /respuestas", 202,
                 f"req={id} {estado} de={cuerpo.get('atendidoPor') or '?'}")
        self.responder(202, {"resultado": "entregada"})

    def tomar_respuesta(self, cuerpo):
        """Lo llama el recolector del balanceador. 200 con la respuesta, 204 si no hubo."""
        destinatario = cuerpo.get("destinatario")
        if not destinatario:
            return self.responder(400, {"error": "falta destinatario"})
        respuesta = SISTEMA.tomar_respuesta(destinatario,
                                            acotar_espera(cuerpo.get("espera", ESPERA_MAXIMA)))
        if respuesta is None:
            return self.responder(204, {})
        self.responder(200, respuesta)

    # -- observación --

    def salud(self):
        """200 mientras el proceso atienda. No mira si hay workers consumiendo:
        una cola sin consumidores está sana y llenándose, que es información
        distinta y sale en /estado."""
        estado = SISTEMA.estado()
        self.responder(200, {
            "cola": "sana",
            "casa": CASA,
            "arrancado": ARRANCADO,
            "esperando": estado["pedidos"]["esperando"],
            "enVuelo": estado["pedidos"]["enVuelo"],
            "cota": estado["pedidos"]["cota"],
            "respuestasPendientes": estado["respuestas"]["pendientes"],
        })

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
        if ruta in ("/", "/estado"):
            if not self.autorizado():
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
        }
        manejar = rutas.get(ruta)
        if manejar is None:
            return self.responder(404, {"error": "no existe"})
        if not self.autorizado():
            bitacora(f"POST {ruta}", 403, f"ip={self.client_address[0]} token inválido")
            return self.responder(403, {"error": "token inválido"})
        if cuerpo is None:
            return self.responder(400, {"error": "cuerpo no es un objeto JSON"})
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


def main():
    global ARRANCADO
    ARRANCADO = _ahora_iso()
    threading.Thread(target=recuperador, daemon=True).start()

    servidor = Servidor((BIND, PUERTO), Manejador)
    # Un hilo por conexión y todas daemon: los long-poll se quedan colgados hasta
    # 30 s y no queremos que el apagado espere a que venzan.
    servidor.daemon_threads = True
    bitacora("arranque", "OK",
             f"escucha={BIND}:{PUERTO} cotaPedidos={COTA_PEDIDOS} "
             f"cotaRespuestas={COTA_RESPUESTAS} reserva={RESERVA}s ttl={TTL_RESPUESTAS}s "
             f"token={'sí' if TOKEN else 'NO'}")
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
