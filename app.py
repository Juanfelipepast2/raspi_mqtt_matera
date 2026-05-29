#!/usr/bin/env python3
# =============================================================================
# app.py — Dashboard IoT de la "Matera inteligente" para Raspberry Pi.
#
# Rol de la Raspberry Pi en el sistema:
#   * Se SUSCRIBE a TOPIC_SENSORS en HiveMQ Cloud y guarda la última lectura
#     que publica el ESP32 (humedad de suelo, temperatura, humedad ambiente,
#     luz y calidad de aire).
#   * Sirve un dashboard web (Flask) que muestra esos datos en vivo (SSE) y
#     ofrece botones de control.
#   * PUBLICA en TOPIC_CMD_PUMP / TOPIC_CMD_PLAY para activar la bomba o la
#     melodía de riego de forma remota; el ESP32 está suscrito y reacciona.
#
# Conexión a HiveMQ Cloud por TLS (puerto 8883) usando el almacén de CAs del
# sistema operativo (HiveMQ Cloud usa certificados Let's Encrypt, ya confiables
# en Raspberry Pi OS), con autenticación usuario/contraseña.
# =============================================================================
import json
import os
import queue
import threading
import uuid
import time

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request
import paho.mqtt.client as mqtt

load_dotenv()

# ----- Configuración (desde .env) -------------------------------------------
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID")
# Si no hay client-id explícito o está el valor por defecto "matera-raspi",
# genera un sufijo único para evitar colisiones con otros clientes.
if not MQTT_CLIENT_ID or MQTT_CLIENT_ID == "matera-raspi":
    MQTT_CLIENT_ID = f"matera-raspi-{uuid.uuid4().hex[:8]}"

# Keepalive configurable
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "120"))

TOPIC_SENSORS = os.getenv("TOPIC_SENSORS", "matera/sensors")
TOPIC_CMD_PUMP = os.getenv("TOPIC_CMD_PUMP", "matera/cmd/pump")
TOPIC_CMD_PLAY = os.getenv("TOPIC_CMD_PLAY", "matera/cmd/play")

WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))

# ----- Estado compartido -----------------------------------------------------
# latest: último snapshot de sensores recibido (dict) o None si aún no llegó.
# subscribers: colas de los clientes SSE conectados, para empujar updates.
_state_lock = threading.Lock()
latest = {"connected": False}
subscribers: set[queue.Queue] = set()


def _broadcast(payload: dict) -> None:
    """Empuja un payload (dict) a todos los clientes SSE conectados."""
    msg = json.dumps(payload)
    with _state_lock:
        dead = []
        for q in subscribers:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            subscribers.discard(q)


# ----- Cliente MQTT (paho 2.x, callbacks VERSION2) ---------------------------
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print(f"[mqtt] conectado a {MQTT_HOST}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSORS)
        print(f"[mqtt] suscrito a {TOPIC_SENSORS}")
    else:
        print(f"[mqtt] conexión rechazada: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties=None):
    print(f"[mqtt] desconectado ({reason_code}); paho reintentará solo")
    with _state_lock:
        latest["connected"] = False
    _broadcast(dict(latest))


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        print(f"[mqtt] payload no-JSON en {msg.topic}: {msg.payload!r}")
        return
    data["connected"] = True
    with _state_lock:
        latest.clear()
        latest.update(data)
        snapshot = dict(latest)
    _broadcast(snapshot)


def on_log(client, userdata, level, buf):
    # logs útiles de paho para diagnóstico
    print(f"[mqtt][log] {buf}")


def build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    try:
        client.tls_set()  # TLS con el almacén de CAs del sistema (Let's Encrypt)
    except Exception as e:
        print(f"[mqtt] tls_set error: {e}")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_log = on_log
    # Configurar backoff de reintentos de reconexión
    try:
        client.reconnect_delay_set(1, 120)
    except Exception:
        # versiones antiguas de paho aceptan (min_delay, max_delay)
        try:
            client.reconnect_delay_set(min_delay=1, max_delay=120)
        except Exception:
            pass
    # Imprime el client-id y usa un bucle de conexión con backoff
    print(f"[mqtt] usando client_id={MQTT_CLIENT_ID}, keepalive={MQTT_KEEPALIVE}")
    backoff = 1
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
            break
        except Exception as e:
            print(f"[mqtt] connect exception: {e}; reintentando en {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)
    client.loop_start()  # hilo de red en segundo plano
    return client


mqtt_client = build_mqtt_client()

# ----- Servidor web (Flask) --------------------------------------------------
app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html", refresh_ms=2000)


@app.route("/api/data")
def api_data():
    """Última lectura conocida (para el primer render o polling de respaldo)."""
    with _state_lock:
        return jsonify(dict(latest))


@app.route("/api/stream")
def api_stream():
    """Stream SSE: empuja cada nueva lectura a los navegadores conectados."""

    def gen():
        q: queue.Queue = queue.Queue(maxsize=10)
        with _state_lock:
            subscribers.add(q)
            current = json.dumps(dict(latest))
        try:
            yield f"data: {current}\n\n"  # estado inicial inmediato
            while True:
                msg = q.get()
                yield f"data: {msg}\n\n"
        finally:
            with _state_lock:
                subscribers.discard(q)

    return Response(gen(), mimetype="text/event-stream")


@app.route("/api/pump", methods=["POST"])
def api_pump():
    """Activa ('1') o detiene ('0') la bomba publicando en TOPIC_CMD_PUMP."""
    on = bool((request.get_json(silent=True) or {}).get("on", True))
    payload = "1" if on else "0"
    mqtt_client.publish(TOPIC_CMD_PUMP, payload, qos=1)
    print(f"[mqtt] pub {TOPIC_CMD_PUMP} -> {payload}")
    return jsonify({"ok": True, "pump": on})


@app.route("/api/play", methods=["POST"])
def api_play():
    """Dispara la melodía de riego publicando '1' en TOPIC_CMD_PLAY."""
    mqtt_client.publish(TOPIC_CMD_PLAY, "1", qos=1)
    print(f"[mqtt] pub {TOPIC_CMD_PLAY} -> 1")
    return jsonify({"ok": True})


if __name__ == "__main__":
    # threaded=True es necesario para atender varias conexiones SSE a la vez.
    app.run(host=WEB_HOST, port=WEB_PORT, threaded=True)
