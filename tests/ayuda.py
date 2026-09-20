"""Shared test plumbing: HTTP calls and bounded waiting.

Both suites talk to a node the way the worker team's client will — plain
`urllib`, no client library — so this is the one place that shape is written.
"""

import json
import os
import time
import urllib.error
import urllib.request


def pedir_http(url, ruta, cuerpo=None, metodo="POST", token=None, timeout=5):
    """Call a node and return `(status, body)`, never raising on 4xx/5xx.

    `HTTPError` is closed explicitly: left to the garbage collector it surfaces
    as a `ResourceWarning` in the middle of an unrelated test, which is noise
    that hides the warnings worth reading.
    """
    cabeceras = {"Content-Type": "application/json"}
    if token:
        cabeceras["X-Cola-Token"] = token
    datos = json.dumps(cuerpo or {}).encode() if metodo == "POST" else None
    pedido = urllib.request.Request(url + ruta, data=datos, method=metodo,
                                    headers=cabeceras)
    try:
        with urllib.request.urlopen(pedido, timeout=timeout) as r:
            crudo = r.read()
            return r.status, json.loads(crudo) if crudo else {}
    except urllib.error.HTTPError as e:
        try:
            crudo = e.read()
            return e.code, json.loads(crudo) if crudo else {}
        finally:
            e.close()


def esperar_a(condicion, techo=15.0, paso=0.05):
    """Poll until `condicion()` is truthy, or give up after `techo` seconds.

    Not a `sleep` in disguise: the test never asserts *how long* something took,
    only that it eventually happened. The ceiling is generous on purpose — a
    tight one is how integration suites become flaky on a loaded machine.
    """
    limite = time.monotonic() + techo
    while time.monotonic() < limite:
        valor = condicion()
        if valor:
            return valor
        time.sleep(paso)
    return None


def silenciar_bitacora(mod, directorio, nombre):
    """Point a loaded `servidor` module's log at a temp dir.

    `ARCHIVO_BITACORA` is computed at import time from `DIRECTORIO_LOGS`, so
    patching only the directory leaves the path stale and every write fails
    noisily into the test output.
    """
    mod.DIRECTORIO_LOGS = directorio
    mod.ARCHIVO_BITACORA = os.path.join(directorio, f"bitacora-{nombre}.log")
