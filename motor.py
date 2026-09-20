"""The networking half of the cluster: it gives `NodoRaft` a clock and a network.

`raft.py` is deliberately inert — it returns messages instead of sending them and
takes time as an argument. Something has to close that loop, and this is it:

  * a ticker thread calling `NodoRaft.tic()` on a fixed interval, which is the
    only thing that starts an election or emits a heartbeat;
  * a small pool of sender threads that POST those messages to peers and feed
    the replies back into the state machine.

Everything that touches `NodoRaft` does so holding `lock`, because the HTTP
handlers call into it from their own request threads. The state machine is not
thread-safe by design: making it so would have meant locks inside the logic
being tested, and the tests would no longer be deterministic.
"""

import json
import threading
import time
import urllib.error
import urllib.request
from queue import Empty, Queue

TIMEOUT_PAR_S = 2.0
HILOS_DE_ENVIO = 4
LATIDO_S = 0.5              # techo de toda espera: un notify perdido cuesta 0,5 s, no un cuelgue

# Resultados de `proponer_y_esperar`.
COMPROMETIDO = "comprometido"
DESTITUIDO = "destituido"
VENCIDO = "vencido"


def ahora_ms():
    """Monotonic milliseconds. Never wall-clock: it must not jump backwards."""
    return int(time.monotonic() * 1000)


def _a_json(cuerpo):
    """Serialise a message body, turning `Entrada` dataclasses into dicts."""
    entradas = cuerpo.get("entradas")
    if entradas:
        cuerpo = dict(cuerpo)
        cuerpo["entradas"] = [
            {"indice": e.indice, "termino": e.termino,
             "operacion": e.operacion, "payload": e.payload}
            for e in entradas
        ]
    return json.dumps(cuerpo, ensure_ascii=False).encode()


RUTA_DE_TIPO = {
    "append": "/raft/appendEntries",
    "solicitud_voto": "/raft/requestVote",
}


class Motor:
    """Drives one `NodoRaft` over HTTP. One per process."""

    def __init__(self, raft, token="", lock=None, intervalo_ms=50,
                 registrar=None, sistema=None, aplicador=None,
                 intervalo_recuperador_s=0.25):
        self.raft = raft
        self.token = token
        self.lock = lock or threading.RLock()
        self.intervalo_ms = intervalo_ms
        self.registrar = registrar or (lambda *_: None)
        self.sistema = sistema
        self.aplicador = aplicador
        self.intervalo_recuperador_s = intervalo_recuperador_s

        self.salientes = Queue()
        self._activo = False
        self._hilos = []

        # Candado 1 del orden de la Decisión 8. Se toma por microsegundos, para
        # agregar una entrada y anotar quién la espera. Nunca se sostiene a
        # través de un I/O ni de un candado de cola.
        self._lock_propuesta = threading.RLock()
        self._esperando_commit = {}     # índice -> Event que el aplicador despierta
        self._propuestos = set()        # ids de pedido con entrada sin aplicar

        # El master no atiende datos hasta terminar su puesta al día: no sabe
        # todavía qué heredó, así que no puede decidir nada honestamente.
        #
        # Se lleva como "de qué mandato terminé el catch-up" y no como un
        # booleano que alguien prende al empezar. Con el booleano había una
        # ventana —ya soy master, todavía no arrancó el hilo del catch-up— en la
        # que el flag decía False y el nodo atendía sobre un log sin barrer.
        # Comparado contra el término actual, no hay instante en que mienta.
        self.recuperado_hasta_termino = 0
        self._fue_master = False

    # --------------------------------------------------------------- control
    def arrancar(self, reloj=True):
        """Arranca los hilos. `reloj=False` deja fuera el ticker.

        Sirve para un test que fija el rol a mano: si el ticker corriera, la
        próxima elección le pisaría el escenario que el test montó.
        """
        if self._activo:
            return
        self._activo = True
        self._hilos = []
        if reloj:
            self._hilos.append(threading.Thread(target=self._bucle_reloj,
                                                name="raft-tic", daemon=True))
        self._hilos += [
            threading.Thread(target=self._bucle_envio, name=f"raft-envio-{i}",
                             daemon=True)
            for i in range(HILOS_DE_ENVIO)
        ]
        if self.aplicador is not None:
            self._hilos.append(threading.Thread(target=self._bucle_aplicador,
                                                name="raft-aplicador", daemon=True))
        if self.sistema is not None:
            self._hilos.append(threading.Thread(target=self._bucle_recuperador,
                                                name="raft-recuperador", daemon=True))
        for hilo in self._hilos:
            hilo.start()

    def parar(self):
        self._activo = False

    # ----------------------------------------------------------------- loops
    def _bucle_reloj(self):
        """The only source of elections and heartbeats."""
        while self._activo:
            try:
                with self.lock:
                    salientes = self.raft.tic(ahora_ms())
                    soy_master = self.raft.rol == "master"
                self.enviar(salientes)
                if soy_master and not self._fue_master:
                    self._fue_master = True
                    threading.Thread(target=self.recuperacion_post_eleccion,
                                     name="raft-recuperacion", daemon=True).start()
                elif not soy_master:
                    self._fue_master = False
            except Exception as e:                      # noqa: BLE001
                self.registrar("raft tic", 500, str(e))
            time.sleep(self.intervalo_ms / 1000.0)

    def enviar(self, mensajes):
        """Queue outbound messages. Called by the ticker and by HTTP handlers."""
        for mensaje in mensajes or []:
            self.salientes.put(mensaje)

    def _bucle_envio(self):
        while self._activo:
            try:
                mensaje = self.salientes.get(timeout=0.2)
            except Empty:
                continue
            try:
                self._entregar(mensaje)
            except Exception as e:                      # noqa: BLE001
                # A peer being unreachable is the normal case this whole design
                # exists for, not an error worth a stack trace. The election
                # timeout is what reacts to it.
                self.registrar("raft envio", 503, f"{mensaje.destino}: {e}")

    # -------------------------------------------------------------- transport
    def _entregar(self, mensaje):
        ruta = RUTA_DE_TIPO.get(mensaje.tipo)
        if ruta is None:
            return

        cabeceras = {"Content-Type": "application/json"}
        if self.token:
            cabeceras["X-Cola-Token"] = self.token
        pedido = urllib.request.Request(
            mensaje.destino.rstrip("/") + ruta,
            data=_a_json(mensaje.cuerpo), method="POST", headers=cabeceras)

        try:
            with urllib.request.urlopen(pedido, timeout=TIMEOUT_PAR_S) as r:
                crudo = r.read()
        except urllib.error.HTTPError as e:
            # A 403 here means the cluster token is wrong on one side, and no
            # amount of retrying fixes that; say so once, loudly.
            self.registrar("raft envio", e.code, f"{mensaje.destino}{ruta}")
            return

        respuesta = json.loads(crudo) if crudo else {}
        self._recibir_respuesta(mensaje, respuesta)

    def _recibir_respuesta(self, mensaje, respuesta):
        with self.lock:
            if mensaje.tipo == "append":
                salientes = self.raft.recibir_respuesta_append(
                    mensaje.destino, respuesta)
            else:
                salientes = self.raft.recibir_respuesta_voto(
                    mensaje.destino, respuesta)
        self.enviar(salientes)

    # ------------------------------------------------------- proponer y esperar
    def _ids_de(self, operacion, payload):
        """Qué pedido nombra una entrada. Sólo `tomar` compite por uno."""
        if operacion == "tomar" and payload.get("id"):
            return {payload["id"]}
        return set()

    def ids_propuestos(self):
        """Ids con una entrada agregada y todavía sin aplicar.

        Es una optimización del caso común, no el mecanismo de corrección: dos
        `tomar` simultáneos podrían inspeccionar el mismo pedido y proponer dos
        entradas para él. Lo que garantiza la corrección es que el paso de
        aplicación es tolerante — el segundo `reservar_pedido` devuelve None y
        el handler vuelve a intentar.
        """
        with self._lock_propuesta:
            return set(self._propuestos)

    def proponer_y_esperar(self, operacion, payload, limite):
        """Agrega una entrada y espera a que la tenga la mayoría.

        `limite` es un instante de `time.monotonic()`, el presupuesto del que
        llamó. Devuelve COMPROMETIDO, DESTITUIDO o VENCIDO — nunca se cuelga, y
        nunca se llama con un candado de cola tomado (Decisión 8).
        """
        with self._lock_propuesta:
            with self.lock:
                propuesta = self.raft.proponer(operacion, payload)
                termino = self.raft.termino_actual
            if propuesta is None:
                return DESTITUIDO           # no somos master
            indice, _ = propuesta
            evento = threading.Event()
            self._esperando_commit[indice] = evento
            ids = self._ids_de(operacion, payload)
            self._propuestos |= ids

        try:
            self._empujar()
            while True:
                restante = limite - time.monotonic()
                if restante <= 0:
                    return VENCIDO
                if evento.wait(min(restante, LATIDO_S)):
                    return COMPROMETIDO
                with self.lock:
                    # Si nos destituyeron, esa entrada no se va a comprometer
                    # nunca: nadie nos va a confirmar nada más.
                    if self.raft.rol != "master" or self.raft.termino_actual != termino:
                        return DESTITUIDO
        finally:
            with self._lock_propuesta:
                self._esperando_commit.pop(indice, None)
                self._propuestos -= ids

    def _empujar(self):
        """Manda ya lo que haya, sin esperar al próximo latido."""
        with self.lock:
            salientes = self.raft._emitir_appends(ahora_ms())
        self.enviar(salientes)

    # ------------------------------------------------------------- aplicación
    def _bucle_aplicador(self):
        """El único escritor de `Sistema`, en todos los nodos.

        Que sea uno solo es lo que hace que el orden del log sea el único orden
        posible: si el master aplicara por su lado y los slaves por el suyo,
        "aplicado" querría decir cosas distintas en cada nodo.
        """
        while self._activo:
            try:
                with self.lock:
                    entradas = self.raft.entradas_a_aplicar()
                for entrada in entradas:
                    self.aplicador.aplicar(entrada)
                    with self._lock_propuesta:
                        self._propuestos -= self._ids_de(entrada.operacion,
                                                         entrada.payload or {})
                        evento = self._esperando_commit.pop(entrada.indice, None)
                    # Despertar afuera del candado 1: el que espera va a querer
                    # tomar candados de cola, y ese orden es el prohibido.
                    if evento is not None:
                        evento.set()
                if not entradas:
                    time.sleep(0.005)
            except Exception as e:                      # noqa: BLE001
                self.registrar("raft aplicador", 500, str(e))
                time.sleep(0.05)

    # ------------------------------------------------------------ mantenimiento
    def _bucle_recuperador(self):
        """El barrido de vencimientos, sólo en el master y sólo después del catch-up.

        Un slave no vence nada por su cuenta: con tres relojes distintos, tres
        nodos tomarían tres decisiones distintas en el mismo instante. Lo que
        viaja es la decisión del master, ya tomada.
        """
        while self._activo:
            time.sleep(self.intervalo_recuperador_s)
            if self.recuperando:
                continue
            with self.lock:
                if self.raft.rol != "master":
                    continue
            try:
                self.barrer_vencidos(espera_s=LATIDO_S)
            except Exception as e:                      # noqa: BLE001
                self.registrar("raft recuperador", 500, str(e))

    def barrer_vencidos(self, espera_s=LATIDO_S):
        """Decide qué vence y lo propone. Devuelve el resultado de la propuesta."""
        decision = self.sistema.decidir_expiry(self.sistema.reloj_ms())
        if not decision.get("reencolar") and not decision.get("fallar") \
                and not decision.get("purgar"):
            return None
        return self.proponer_y_esperar("expirar", decision,
                                       time.monotonic() + espera_s)

    @property
    def recuperando(self):
        """¿Este master todavía no barrió el log que heredó?

        Verdadero desde el instante mismo en que gana la elección, sin que nadie
        tenga que marcarlo: si el catch-up terminado no es el del mandato en
        curso, falta hacerlo.
        """
        with self.lock:
            if self.raft.rol != "master":
                return False
            return self.recuperado_hasta_termino != self.raft.termino_actual

    def recuperacion_post_eleccion(self, espera_s=5.0):
        """Las dos fases que corre un master recién electo, antes de atender.

        Fase 1 — la barrera del `sentinela`. Un master nuevo puede tener
        entradas de términos anteriores que no puede comprometer contando
        réplicas (Raft §5.4.2). Agrega una entrada propia que no hace nada y
        espera a que se comprometa: comprometerla compromete todo lo anterior.
        Hasta que eso pase, el nodo no sabe qué heredó.

        Fase 2 — un barrido de vencimientos. Las reservas de workers que se
        murieron durante la ventana sin líder, y los pedidos cuyo presupuesto se
        agotó mientras no había master, se resuelven acá — antes de que ningún
        worker pueda recibir uno de ellos como si estuviera vivo.
        """
        with self.lock:
            termino = self.raft.termino_actual
        if self.sistema is None:
            self._marcar_recuperado(termino)
            return True

        limite = time.monotonic() + espera_s
        if self.proponer_y_esperar("sentinela", {}, limite) != COMPROMETIDO:
            return False                    # nos destituyeron o no hubo quórum
        self.barrer_vencidos(espera_s=max(0.1, limite - time.monotonic()))
        self._marcar_recuperado(termino)
        return True

    def _marcar_recuperado(self, termino):
        """Sólo si seguimos en el mismo mandato: si cambió, el catch-up que
        corresponde es el del mandato nuevo, no éste."""
        with self.lock:
            if self.raft.termino_actual == termino:
                self.recuperado_hasta_termino = termino
