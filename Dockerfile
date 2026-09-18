# Imagen del sistema de colas.
#
# Una sola etapa y sin `pip install`: la cola es **sólo biblioteca estándar**.
# No es casualidad ni ascetismo — es el argumento de por qué vale la pena
# escribirla en vez de traer RabbitMQ: el componente que ahora está en el camino
# crítico de cada request no agrega ninguna dependencia que pueda romperse ni
# ninguna superficie que haya que parchear.

FROM python:3.13-slim
WORKDIR /app

# Usuario sin privilegios. La cola ve el contenido de todos los pedidos del
# servicio (nombres, legajos): es de los últimos procesos a los que darle root.
#
# El uid es 1000 y no uno alto porque la bitácora se escribe en un bind mount
# del disco del host y el uid de adentro tiene que coincidir con el del dueño
# de ese directorio afuera. Con otro uid el servicio arranca igual y sólo se
# pierde el archivo —la línea sigue saliendo por stdout—, así que el problema
# no se nota hasta que hace falta la bitácora para la auditoría cruzada.
RUN useradd --create-home --uid 1000 cola

COPY colas.py servidor.py ./

RUN mkdir -p /app/logs && chown -R cola:cola /app
USER cola

# COLA_BIND=0.0.0.0 acá y 127.0.0.1 en el módulo, a propósito: corriendo a mano
# el default seguro es no escuchar en la red, pero adentro del contenedor atarse
# a loopback lo dejaría inalcanzable hasta para su propio host. Lo que lo protege
# en la máquina es `ufw` —sólo entra por `tailscale0`— más COLA_TOKEN, igual que
# a las réplicas. Publicar el puerto sin ninguna de las dos cosas es dejar las
# colas del servicio abiertas.
ENV COLA_PUERTO=8085 \
    COLA_BIND=0.0.0.0 \
    COLA_LOGS=/app/logs \
    PYTHONUNBUFFERED=1

EXPOSE 8085

# /health no pide token a propósito: lo consulta el HEALTHCHECK, que no tiene
# por qué llevar el secreto adentro, y no dice nada que no se vea desde afuera.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import os,urllib.request,sys; p=os.environ.get('COLA_PUERTO','8085'); sys.exit(0 if urllib.request.urlopen(f'http://localhost:{p}/health',timeout=2).status==200 else 1)"

STOPSIGNAL SIGTERM

CMD ["python", "servidor.py"]
