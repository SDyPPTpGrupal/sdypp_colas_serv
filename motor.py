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
                 registrar=None):
        self.raft = raft
        self.token = token
        self.lock = lock or threading.RLock()
        self.intervalo_ms = intervalo_ms
        self.registrar = registrar or (lambda *_: None)

        self.salientes = Queue()
        self._activo = False
        self._hilos = []

    # --------------------------------------------------------------- control
    def arrancar(self):
        if self._activo:
            return
        self._activo = True
        self._hilos = [threading.Thread(target=self._bucle_reloj,
                                        name="raft-tic", daemon=True)]
        self._hilos += [
            threading.Thread(target=self._bucle_envio, name=f"raft-envio-{i}",
                             daemon=True)
            for i in range(HILOS_DE_ENVIO)
        ]
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
                self.enviar(salientes)
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
