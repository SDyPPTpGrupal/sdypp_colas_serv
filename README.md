# El sistema de colas

Un proceso aparte del balanceador, con **dos colas**:

| | Quién publica | Quién consume |
| :--- | :--- | :--- |
| **pedidos** | el balanceador | los workers de las réplicas, compitiendo |
| **respuestas** | los workers | el balanceador que originó cada pedido |

Sólo biblioteca estándar. No es ascetismo: el componente que ahora está en el camino crítico
de **cada** request del servicio no agrega ninguna dependencia que pueda romperse ni ninguna
superficie que haya que parchear. La imagen no corre `pip install`.

```bash
python3 cola/servidor.py
COLA_PUERTO=8085 COLA_BIND=100.101.15.93 COLA_TOKEN=... python3 cola/servidor.py
```

---

## El clúster: una cola lógica en tres nodos

Un solo nodo es un punto único de falla, y lo que guarda no es descartable: son pedidos que el
cliente ya vio aceptados. La cola corre entonces en **3 nodos (impar)** con un **Raft-lite**
propio, en `raft.py`:

- un **master** atiende *todo* el tráfico de datos;
- los **slaves** replican su log y compiten por sucederlo;
- si el master se cae, los nodos **eligen uno nuevo solos**, y los clientes se mudan siguiendo
  un `421`.

```
COLA_PUERTO=8085 COLA_URL=http://127.0.0.1:8085 \
COLA_PARES=http://127.0.0.1:8085,http://127.0.0.1:8086,http://127.0.0.1:8087 \
COLA_TOKEN=... python3 servidor.py
```

`COLA_PARES` vacío = **nodo solo**, que es mayoría de uno: master desde el arranque, sin
elección y sin emitir nunca un `421`. No hay rama especial en el camino de datos — es la misma
aritmética de `mayoria`, con `N=1`.

### `tomar` no es una lectura

Es la sutileza que ordena todo el diseño, y la que más caro sale ignorar: `POST /pedidos/tomar`
**reserva** el pedido, o sea que lo saca del pool de disponibles. Es una mutación. Por eso no se
puede repartir entre slaves como si fueran réplicas de lectura: dos workers tomando contra dos
nodos distintos se llevan el mismo pedido, y la garantía de a-lo-sumo-una-entrega se cae apenas
la replicación tenga el menor retraso.

### El redirect: `421 Misdirected Request`

Cualquier nodo que no sea master contesta, en las **cinco rutas de datos**:

```jsonc
// ← 421
{"error": "no-soy-master", "master": "http://cola-2:8085"}   // o "master": null en elección
```

`421` y no `409` porque `/respuestas` ya usa `409` para dos cosas distintas
(`desconocido`, `destinatario-saturado`): un tercer significado sobre el mismo código hace que
un `status == 409` pelado descarte respuestas válidas en silencio. `421` significa literalmente
"le pediste a un servidor que no puede responder esto".

`GET /health`, `GET /health/vivo` y `/raft/*` **nunca** contestan `421`.

### Nada de nginx adelante del clúster

Un proxy round-robin mandaría escrituras a un slave al azar. **El balanceador y los workers le
hablan directo a cada nodo** y siguen al master ellos mismos: arrancan con la lista de los N,
preguntan por `/health` quién manda, lo cachean, y se mudan cuando les llega un `421`. Toda la
adaptación a la topología son tres cosas: una lista estática, una URL cacheada y reintento con
backoff.

### El protocolo, entre nodos

```
POST /raft/appendEntries   {termino, master, indicePrevio, terminoPrevio, entradas[], indiceCommit}
                           vacío = heartbeat. ← {termino, exito, indiceCoincidente}
                           ← {termino, exito: false, terminoConflicto, primerIndiceDelTermino}
POST /raft/requestVote     {termino, candidato, ultimoIndiceLog, ultimoTerminoLog}
                           ← {termino, votoConcedido}
GET  /raft/estado          rol, término, índice de log y de commit (diagnóstico)
```

Tres reglas sostienen la corrección, y cada una vive en un solo lugar de `raft.py`:

1. **Un voto por término, y sólo a un log al menos tan al día como el propio** (último término
   primero, índice en el desempate). Es lo que impide que un nodo atrasado gane y se lleve
   puestos pedidos ya confirmados.
2. **Una entrada se compromete con la mayoría**, y el master sólo commitea por conteo las
   entradas de *su* término; las anteriores viajan de arrastre.
3. **Fencing por término**: cualquier mensaje con un término mayor degrada al receptor a slave
   en el acto, esté en el rol que esté. Es lo que hace imposible que haya dos masters — un
   master viejo que revive se autoexcluye sin que nadie tenga que detectarlo ni echarlo.

`raft.py` no importa `threading`, `time` ni `http`: el tiempo entra como argumento de `tic()` y
los mensajes se **devuelven** en vez de enviarse. Quien cierra ese lazo es `motor.py`. No es
purismo: es lo que hace que los tests de elección sean deterministas en vez de una lotería
contra el reloj de pared.

---

## Por qué dos colas y no una

Son dos flujos con dueños distintos. En `pedidos` hay N consumidores **compitiendo por el
mismo elemento**: el primero que llega se lo lleva y nadie más lo ve. En `respuestas` cada
elemento tiene **un** destinatario y nadie más lo puede tomar. Con una sola cola, cada
consumidor tendría que filtrar lo que no es suyo y devolver el resto — que es exactamente el
protocolo de reasignación que este diseño evita.

La cola de respuestas está indexada por destinatario por la misma razón: en la Etapa 3 hay dos
balanceadores contra la misma cola, y uno no puede llevarse la respuesta que el otro está
esperando. Con una FIFO sola, el segundo se comería las del primero y los dos clientes
esperarían de más.

```mermaid
flowchart TD
    BA["Balanceador<br/>balanceador@casa-tomas"]

    subgraph CP["Cola de pedidos — FIFO, cota 100"]
        E1["esperando<br/>p3 · p4 · p5"]
        EV["en vuelo · reservados<br/>p1 → casa-A · p2 → casa-B"]
    end

    subgraph CR["Cola de respuestas — una por destinatario"]
        D1["balanceador@casa-tomas<br/>r1 · r2"]
    end

    RECUP["Recuperador · cada 0,25 s"]

    BA -->|"1 · POST /pedidos"| E1
    E1 -->|"2 · POST /pedidos/tomar"| WA["Worker casa-A"]
    E1 -->|"2 · POST /pedidos/tomar"| WB["Worker casa-B"]
    WA -.-> EV
    WB -.-> EV
    WA -->|"3 · POST /respuestas"| D1
    WB -->|"3 · POST /respuestas"| D1
    D1 -->|"4 · POST /respuestas/tomar"| BA

    RECUP -->|"reserva vencida + idempotente<br/>vuelve al frente"| E1
    RECUP -->|"reserva vencida + escritura<br/>o sin presupuesto"| D1
```

## La reserva: lo único que hace que esto sea una cola y no una lista

Un pedido que un worker toma **no se borra**: queda "en vuelo" con un vencimiento
(`COLA_RESERVA`, 2 s). Si el worker no contesta antes, el pedido vuelve al frente y otro lo
toma.

Es lo que reemplaza al `devolver_al_frente()` que antes hacía el worker cuando el gRPC le
fallaba. Con los workers adentro del balanceador, el que se comía el error devolvía el pedido;
ahora el worker está en la réplica y **una réplica que se murió no devuelve nada**. La única
que puede notarlo es la cola, y sólo por el vencimiento.

La regla de qué se reintenta es contrato:

| Qué pasó | `idempotente: true` | `idempotente: false` |
| :--- | :--- | :--- |
| Venció la reserva y queda presupuesto | vuelve al frente | **respuesta `DEADLINE_EXCEEDED`** |
| Venció la reserva y no queda presupuesto | `DEADLINE_EXCEEDED` | `DEADLINE_EXCEEDED` |
| Venció esperando turno, sin que nadie lo tome | `DEADLINE_EXCEEDED` | `DEADLINE_EXCEEDED` |

"No contestó" no dice si el pedido alcanzó a ejecutarse. Repetir un `POST /personas` puede
crear la persona dos veces: preferimos un `504` honesto a un duplicado silencioso. El default
de `idempotente` es **`false`** — ante la duda, no se reintenta.

**Gana la primera respuesta.** Si una réplica lenta contesta después de que la reserva venció
y otra ya resolvió el pedido, la segunda se rechaza con `409 {"resultado": "desconocido"}`.

---

## El contrato HTTP

Todo es `POST` con JSON y respuesta JSON. Todo pide `X-Cola-Token`, menos `GET /health` y
`GET /health/vivo`. El token **no es uno solo**: cada clase de ruta tiene el suyo (ver
[Seguridad](#seguridad)).

`tomar` es POST y no GET a propósito: saca el elemento de la cola, o sea que cambia el estado
del servidor. Un GET que muta es lo que cualquier reintento automático de un cliente HTTP
convierte en pedidos perdidos.

### Lo que usa el balanceador

<table>
<tr><th><code>POST /pedidos</code></th><th>publicar un pedido</th></tr>
</table>

```jsonc
// →
{"id": "4b7baf0d2d4a4e909ec90c6f17690125",   // uuid4 hex; si falta, lo genera la cola
 "operacion": "POST /personas",              // obligatorio
 "parametros": {"nombre": "Ada", "legajo": 1234},
 "idempotente": false,                       // default false: el seguro
 "destinatario": "balanceador@casa-tomas",   // obligatorio
 "cliente": "100.118.61.111",                // IP del que hizo la request; informativo
 "presupuestoMs": 5000}                      // techo COLA_PRESUPUESTO_MAXIMO

// ← 202 {"id": "4b7baf0d…", "encolado": true}
// ← 503 {"error": "cola llena", "esperando": 100, "cota": 100}
```

<table>
<tr><th><code>POST /respuestas/tomar</code></th><th>recolectar (long-poll)</th></tr>
</table>

```jsonc
// →  {"destinatario": "balanceador@casa-tomas", "espera": 20}
// ← 200 (ver el formato de la respuesta más abajo)
// ← 204 sin cuerpo: no había nada en esos segundos
```

### Lo que usa el worker de la réplica

<table>
<tr><th><code>POST /pedidos/tomar</code></th><th>llevarse el próximo (long-poll)</th></tr>
</table>

```jsonc
// →  {"consumidor": "100.91.134.43:8080", "espera": 20}

// ← 200
{"id": "4b7baf0d2d4a4e909ec90c6f17690125",
 "operacion": "POST /personas",
 "parametros": {"nombre": "Ada", "legajo": 1234},
 "idempotente": false,
 "cliente": "100.118.61.111",
 "quedaMs": 4870,       // presupuesto que le queda: usalo como timeout
 "intento": 1}          // 2 o más = a este pedido ya lo abandonó otra réplica

// ← 204 sin cuerpo: no había trabajo en esos segundos. Volvé a llamar.
```

**`consumidor` tiene que ser el `host:puerto` gRPC de la réplica** — el mismo string que el
balanceador usa como `destino` en su registro. No es capricho: es lo que permite que
`/health` del balanceador cruce "esta réplica está sana" con "esta réplica consumió 40
pedidos y tiene 2 en vuelo" sin traducir nada. Con otro string, la réplica figura como sana y
sin consumir.

**No viaja ningún instante absoluto.** Los relojes de cuatro casas no están sincronizados: si
el pedido llevara un `vence_en`, una máquina adelantada dos segundos descartaría pedidos
vivos. Va `quedaMs`, calculado con el reloj de la cola, que es el único que cuenta.

<table>
<tr><th><code>POST /respuestas</code></th><th>contestar</th></tr>
</table>

```jsonc
// →
{"id": "4b7baf0d2d4a4e909ec90c6f17690125",   // el mismo del pedido
 "estado": "OK",                             // nombre de código de estado gRPC
 "contenido": {"servidoPor": "python-1", "persona": {"id": 7, "nombre": "Ada", "legajo": 1234}},
 "atendidoPor": "100.91.134.43:8080",        // el mismo string que `consumidor`
 "app": "python"}

// ← 202 {"resultado": "entregada"}
// ← 409 {"resultado": "desconocido"}          ese pedido ya lo contestó otro: tirala
// ← 409 {"resultado": "destinatario-saturado"} el balanceador no está recolectando
```

**No mandes `destinatario`: lo pone la cola** con lo que guardó del pedido. El worker no tiene
por qué saber quién le pidió, y si se lo preguntáramos podría contestar apuntando a otro
balanceador y meterle una respuesta ajena.

<table>
<tr><th><code>POST /pedidos/devolver</code></th><th>soltarlo sin atenderlo</th></tr>
</table>

```jsonc
// →  {"id": "4b7baf0d…", "consumidor": "100.91.134.43:8080"}
// ← 200 {"resultado": "devuelto"}
// ← 409 {"resultado": "no-estaba-en-vuelo"}   la reserva ya venció; no hagas nada
```

Para el drenado de un blue/green: la réplica que se apaga devuelve lo que tenía en la mano en
vez de hacer esperar los 2 s de la reserva. Es una optimización, no una garantía — si no llega
a devolverlo, el recuperador lo hace igual, sólo que más tarde.

### La respuesta, como la ve el balanceador

Lo que sale de `POST /respuestas/tomar`. La cola le agrega al JSON del worker lo que sabe del
pedido:

```jsonc
{"id": "4b7baf0d2d4a4e909ec90c6f17690125",
 "operacion": "POST /personas",
 "estado": "OK",
 "contenido": {"servidoPor": "python-1", "persona": {"id": 7, "nombre": "Ada", "legajo": 1234}},
 "atendidoPor": "100.91.134.43:8080",
 "app": "python",
 "intentos": ["100.101.15.93:8090", "100.91.134.43:8080"],  // ← lo agrega la cola
 "esperaMs": 6}                                              // ← lo agrega la cola
```

`intentos` con más de un elemento es la evidencia de la reasignación, y es lo que el
balanceador escribe como `intentos=a→b` en su bitácora.

### Observación

```
GET /health    sin token. Lo consulta el HEALTHCHECK del contenedor.
               → 200 {"cola": "sana", "esperando": 3, "enVuelo": 2, "cota": 100, ...}

GET /estado    con token. Dice qué réplicas consumen y cuánto tienen en vuelo.
               → 200 {"pedidos": {...}, "respuestas": {...}, "consumidores": {...}}
```

`/health` contesta 200 mientras el proceso atienda, sin mirar si hay workers consumiendo: una
cola sin consumidores está sana y llenándose, que es información distinta y sale en `/estado`.

---

## Las operaciones

Esto es lo que tiene que saber hacer el worker. `operacion` es el mismo string que el
balanceador escribe en su bitácora, y cada una mapea 1:1 con un RPC de `contrato.proto`.

| `operacion` | `parametros` | RPC | `contenido` de la respuesta |
| :--- | :--- | :--- | :--- |
| `GET /` | `{}` | `Identidad` | `{app, lenguaje, equipo: [{nombre, apellido, legajo}], version, mensaje, host, arrancado, servidoPor}` |
| `POST /echo` | `{ping}` | `Echo` | `{pong, servidoPor, version}` |
| `GET /personas` | `{}` | `ListarPersonas` | `{servidoPor, personas: [{id, nombre, legajo}]}` |
| `POST /personas` | `{nombre, legajo}` | `CrearPersona` | `{servidoPor, persona: {id, nombre, legajo}}` |

`legajo` llega **siempre como entero**: el balanceador lo normaliza antes de encolar, porque
desde `curl` viene como string.

Cuando el RPC falla, `contenido` es `{"error": "<el detalle>"}` y `estado` es el nombre del
código gRPC que se recibió (`INVALID_ARGUMENT`, `ALREADY_EXISTS`, `NOT_FOUND`, …). El
balanceador lo traduce a HTTP con una tabla; no hace falta que el worker piense en códigos
HTTP.

**El `contenido` lo arma el worker, no el balanceador.** Antes el balanceador traducía el
protobuf campo por campo; ahora lo manda ya armado y el balanceador sólo lo mete en el sobre
del contrato. Agregar un campo a la app dejó de obligar a tocar el balanceador.

## El worker, en veinte líneas

```python
while True:
    codigo, pedido = post("/pedidos/tomar", {"consumidor": MI_DESTINO, "espera": 20})
    if codigo == 204:
        continue                       # no había trabajo; el long-poll ya esperó

    timeout = pedido["quedaMs"] / 1000
    try:
        contenido = RESOLVER[pedido["operacion"]](pedido["parametros"], timeout)
        estado = "OK"
    except grpc.RpcError as e:
        estado, contenido = e.code().name, {"error": e.details()}

    post("/respuestas", {"id": pedido["id"], "estado": estado, "contenido": contenido,
                         "atendidoPor": MI_DESTINO, "app": "python"})
```

Tres cosas que no son obvias y se pagan caras:

1. **Usar `quedaMs` como timeout.** Un worker que ignora el presupuesto sigue trabajando en
   un pedido que la cola ya reasignó: dos réplicas haciendo el mismo trabajo y, en un alta,
   dos personas.
2. **Devolver lo que se tiene en la mano al apagarse** (`SIGTERM` → `POST /pedidos/devolver`).
   Sin eso, cada blue/green le cuesta 2 s de espera a un puñado de pedidos.
3. **Tratar el `409` como normal.** Llega cuando la réplica tardó más que la reserva. No es un
   error del worker y no hay que reintentar: el pedido ya lo contestó otro.

## Variables

| | Default | |
| :--- | :--- | :--- |
| `COLA_PUERTO` | `8085` | |
| `COLA_BIND` | `127.0.0.1` | En el contenedor va `0.0.0.0`: atarse a loopback lo dejaría inalcanzable hasta para su host |
| `COLA_TOKEN` | *(vacío)* | Vacío = **sin autenticación**. Sólo para probar en loopback |
| `COLA_CASA` | `casa-tomas` | Sale en la bitácora |
| `COLA_COTA_PEDIDOS` | `100` | Pasado eso, `503` al balanceador en el acto |
| `COLA_COTA_RESPUESTAS` | `1000` | Por destinatario. Alto a propósito: rechazar una respuesta deja a un cliente colgado |
| `COLA_RESERVA` | `2` | Segundos que tiene un worker para contestar. Bastante menor que el presupuesto |
| `COLA_TTL_RESPUESTAS` | `60` | Cuánto sobrevive una respuesta que nadie recolectó |
| `COLA_ESPERA_MAXIMA` | `30` | Techo del long-poll |
| `COLA_PRESUPUESTO_MAXIMO` | `60` | Techo de `presupuestoMs` |
| `COLA_INTERVALO_RECUPERADOR` | `0.25` | Cada cuánto corre el recuperador |
| `COLA_LOGS` | `logs` | Directorio de la bitácora |
| `COLA_PARES` | *(vacío)* | URLs de los nodos, separadas por coma. Vacío = nodo solo |
| `COLA_URL` | `http://BIND:PUERTO` | Cómo lo ven los otros nodos. Es su identidad en el clúster |
| `COLA_INSTANCIA` | `cola-PUERTO@CASA` | Sale en `/health` y en la bitácora |
| `COLA_TOKEN_PUBLICADOR` | `COLA_TOKEN` | |
| `COLA_TOKEN_CONSUMIDOR` | `COLA_TOKEN` | |
| `COLA_TOKEN_CLUSTER` | `COLA_TOKEN` | |
| `RAFT_HEARTBEAT_MS` | `150` | Cada cuánto late el master |
| `RAFT_ELECCION_TIMEOUT_MS` | `600` | Silencio antes de postularse, más un jitter aleatorio |

## Seguridad

**El token no es opcional en la demo.** La cola escucha en el tailnet porque los workers están
en otras casas. Sin token, cualquiera que la alcance puede publicar pedidos falsos o —peor—
**tomarlos**, y quedarse con tráfico real de usuarios: nombres y legajos de personas.

Las tres barreras, y hacen falta las tres:

1. `ufw`: sólo entra por `tailscale0`.
2. El token que corresponda en `X-Cola-Token` (ver abajo).
3. El contenedor corre con un usuario sin privilegios (uid 1000), sin el socket de Docker y
   sin `--privileged`.

### Tres tokens, porque no son el mismo permiso

| Token | Abre | Lo tiene |
| :--- | :--- | :--- |
| `COLA_TOKEN_PUBLICADOR` | `POST /pedidos`, `POST /respuestas/tomar`, `GET /estado` | el balanceador |
| `COLA_TOKEN_CONSUMIDOR` | `POST /pedidos/tomar`, `POST /pedidos/devolver`, `POST /respuestas` | los workers |
| `COLA_TOKEN_CLUSTER` | `/raft/*` | sólo los nodos de cola |

Los tres caen a `COLA_TOKEN` cuando no se definen, así que un despliegue existente migra sin
tocarle la configuración.

El tercero no es simetría burocrática: **sin él, cualquiera que alcance el puerto puede
postularse candidato o inyectar entradas en el log**, que es la diferencia entre replicar y ser
replicado por un desconocido. Y el corte publicador/consumidor importa porque un worker
comprometido con un token único podría inyectar pedidos falsos, no sólo consumirlos.

## Pruebas

```bash
python3 -m unittest discover -s tests -v      # sólo biblioteca estándar, sin pytest
```

| | Qué prueba | Cómo |
| :--- | :--- | :--- |
| `test_raft.py` | la máquina de estados sola | reloj falso y bus en memoria; **sin `time` ni `sleep`** |
| `test_servidor_cluster.py` | la superficie HTTP: tokens, `421`, `/health` | un nodo en un puerto libre, hablado con `urllib` |
| `test_cluster_raft.py` | el cableado real: elección, failover, puesta al día | 3 nodos en proceso, cada uno con su copia del módulo |

`test_raft.py` no puede importar `time` ni llamar a `sleep`: el tiempo entra por
`avanzar(ms)` contra un reloj falso, y "un nodo lento" es un `threading.Event` que controla el
test. No es preferencia de estilo — es lo único que hace que los tests de elección no sean una
lotería contra el reloj de pared.

En los tests de integración no se afirma que algo pasó *dentro de* una ventana de tiempo: se
consulta una condición con un techo generoso. Un techo ajustado es cómo una suite se vuelve
intermitente en una máquina cargada.

## Lo que no hace, y no por olvido

- **No persiste en disco.** El log vive en memoria: la durabilidad la da la **replicación**, no
  el disco. Un pedido confirmado sobrevive a la caída de *un* nodo porque lo tienen 2 de 3, no
  porque esté escrito en algún lado. Apagar los tres a la vez tira lo que estaba esperando, y
  es una decisión consciente: es lo que hace Kafka en su camino rápido, donde `acks=all` sobre
  varias réplicas reemplaza al `fsync` por mensaje.
- **No reparte.** No hay round-robin ni pesos: el worker libre toma el próximo. El reparto
  sale solo de la velocidad de cada réplica.
- **No sabe qué es una réplica.** Un consumidor es un string. Puede ser Python, Java o `curl`.
- **No valida el contenido de los pedidos.** `operacion` y `parametros` son opacos para la
  cola: quien los entiende es el worker. Cambiar una operación no obliga a tocar este repo.
