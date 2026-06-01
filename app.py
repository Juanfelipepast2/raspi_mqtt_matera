#!/usr/bin/env python3
# =============================================================================
# app.py — Dashboard IoT de la "Matera inteligente" para Raspberry Pi.
#
# Rol de la Raspberry Pi en el sistema:
#   * Se SUSCRIBE a TOPIC_SENSORS (lecturas en vivo) y a TOPIC_STATE (catálogo
#     de canciones, volumen, umbral, patrón/color de los NeoPixel, etc.) que
#     publica el ESP32 en HiveMQ Cloud.
#   * Guarda un HISTÓRICO de las lecturas en SQLite para dibujar gráficas con
#     filtro temporal (últimos 5 min, 1 h, 1 día, todo).
#   * Sirve un dashboard web (Flask) que muestra los datos en vivo (SSE), las
#     gráficas y todos los controles.
#   * PUBLICA en TOPIC_CMD_* para mandar comandos al ESP32 (regar, reproducir
#     una canción por índice, parar el audio, volumen, canción de riego, umbral
#     de riego y control de los NeoPixel: brillo, patrón y color).
#
# Conexión a HiveMQ Cloud por TLS (8883) con el almacén de CAs del sistema.
# =============================================================================
import json
import os
import queue
import sqlite3
import threading
import time
import uuid

import certifi
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

MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "120"))

# Topics — deben coincidir EXACTAMENTE con los de src/core/config.h del ESP32.
TOPIC_SENSORS = os.getenv("TOPIC_SENSORS", "matera/sensors")
TOPIC_STATE = os.getenv("TOPIC_STATE", "matera/state")
TOPIC_CMD_PUMP = os.getenv("TOPIC_CMD_PUMP", "matera/cmd/pump")
TOPIC_CMD_PLAY = os.getenv("TOPIC_CMD_PLAY", "matera/cmd/play")
TOPIC_CMD_STOP = os.getenv("TOPIC_CMD_STOP", "matera/cmd/stop")
TOPIC_CMD_VOLUME = os.getenv("TOPIC_CMD_VOLUME", "matera/cmd/volume")
TOPIC_CMD_THRESHOLD = os.getenv("TOPIC_CMD_THRESHOLD", "matera/cmd/threshold")
TOPIC_CMD_IRR_INTERVAL = os.getenv("TOPIC_CMD_IRR_INTERVAL", "matera/cmd/irr_interval")
TOPIC_CMD_NEO_BRIGHT = os.getenv("TOPIC_CMD_NEO_BRIGHT", "matera/cmd/neo/bright")
TOPIC_CMD_NEO_PATTERN = os.getenv("TOPIC_CMD_NEO_PATTERN", "matera/cmd/neo/pattern")
TOPIC_CMD_NEO_COLOR = os.getenv("TOPIC_CMD_NEO_COLOR", "matera/cmd/neo/color")

WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))

# Ruta de la base de datos del histórico (junto al app.py por defecto).
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), "history.db"))
# Métricas que se guardan e historizan.
METRICS = ("soil", "temp", "hum", "lux", "ppm")

# ----- Estado compartido -----------------------------------------------------
# latest: último snapshot de sensores recibido (dict) o {"connected": False}.
# state:  último catálogo/estado recibido del ESP (canciones, volumen, etc.).
# subscribers: colas de los clientes SSE conectados, para empujar updates.
_state_lock = threading.Lock()
latest = {"connected": False}
state: dict = {}
subscribers: "set[queue.Queue]" = set()

# ----- Histórico en SQLite ---------------------------------------------------
_db_lock = threading.Lock()
_db = sqlite3.connect(DB_PATH, check_same_thread=False)
_db.execute(
    "CREATE TABLE IF NOT EXISTS readings ("
    " ts REAL PRIMARY KEY, soil REAL, temp REAL, hum REAL, lux REAL, ppm REAL)"
)
_db.execute("CREATE INDEX IF NOT EXISTS idx_ts ON readings(ts)")
_db.execute(
    "CREATE TABLE IF NOT EXISTS irrigation_log (id INTEGER PRIMARY KEY, ts REAL NOT NULL)"
)
_db.execute("CREATE INDEX IF NOT EXISTS idx_irr_ts ON irrigation_log(ts)")
_db.commit()

# Último estado de irrigación conocido para detectar transiciones ON→OFF.
_last_irrigating: bool = False


def _store_reading(ts: float, data: dict) -> None:
    """Inserta una fila de lecturas en el histórico (ts en segundos epoch)."""
    row = [ts] + [_num(data.get(m)) for m in METRICS]
    with _db_lock:
        try:
            _db.execute(
                "INSERT OR REPLACE INTO readings (ts, soil, temp, hum, lux, ppm)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                row,
            )
            _db.commit()
        except sqlite3.Error as e:
            print(f"[db] error guardando lectura: {e}")


def _log_irrigation(ts: float) -> None:
    """Registra un evento de inicio de riego en irrigation_log."""
    with _db_lock:
        try:
            _db.execute("INSERT INTO irrigation_log (ts) VALUES (?)", (ts,))
            _db.commit()
        except sqlite3.Error as e:
            print(f"[db] error guardando evento de riego: {e}")


def _num(v):
    """Convierte a float o None (para guardar NULL ante valores ausentes)."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# Rangos de tiempo soportados por las gráficas (segundos hacia atrás; None=todo).
RANGES = {
    "5m": 5 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "6h": 6 * 60 * 60,
    "24h": 24 * 60 * 60,
    "all": None,
}
MAX_POINTS = 600  # tope de puntos devueltos por gráfica (se submuestrea si hace falta)


def _query_history(range_key: str) -> dict:
    """Devuelve el histórico en el rango pedido como columnas paralelas.

    Estructura: {"t": [...ms...], "soil": [...], "temp": [...], ...}. Si hay más
    de MAX_POINTS filas, submuestrea de forma uniforme para no saturar el
    navegador.
    """
    span = RANGES.get(range_key, RANGES["1h"])
    params: tuple = ()
    where = ""
    if span is not None:
        where = "WHERE ts >= ?"
        params = (time.time() - span,)
    with _db_lock:
        cur = _db.execute(f"SELECT COUNT(*) FROM readings {where}", params)
        total = cur.fetchone()[0]
        # Submuestreo: si hay demasiadas filas, tomamos 1 de cada `stride`.
        stride = max(1, (total + MAX_POINTS - 1) // MAX_POINTS) if total else 1
        cur = _db.execute(
            f"SELECT ts, soil, temp, hum, lux, ppm FROM readings {where}"
            " ORDER BY ts ASC",
            params,
        )
        rows = cur.fetchall()

    out: dict = {"t": []}
    for m in METRICS:
        out[m] = []
    for i, r in enumerate(rows):
        if i % stride != 0 and i != len(rows) - 1:
            continue
        out["t"].append(int(r[0] * 1000))  # epoch ms para el eje de tiempo
        for j, m in enumerate(METRICS):
            out[m].append(r[j + 1])
    return out


def _broadcast(payload: dict) -> None:
    """Empuja un payload (dict con 'event' y 'data') a los clientes SSE."""
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
        client.subscribe(TOPIC_STATE)
        print(f"[mqtt] suscrito a {TOPIC_SENSORS} y {TOPIC_STATE}")
    else:
        print(f"[mqtt] conexión rechazada: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties=None):
    print(f"[mqtt] desconectado ({reason_code}); paho reintentará solo")
    with _state_lock:
        latest["connected"] = False
    _broadcast({"event": "sensors", "data": dict(latest)})


def on_message(client, userdata, msg):
    global _last_irrigating
    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        print(f"[mqtt] payload no-JSON en {msg.topic}: {msg.payload!r}")
        return

    if msg.topic == TOPIC_STATE:
        with _state_lock:
            state.clear()
            state.update(data)
            snapshot = dict(state)
        _broadcast({"event": "state", "data": snapshot})
        return

    # TOPIC_SENSORS (lecturas).
    data["connected"] = True

    # Detectar inicio de ciclo de riego (transición False → True).
    irrigating_now = bool(data.get("irrigating", False))
    if irrigating_now and not _last_irrigating:
        _log_irrigation(time.time())
        print("[irrigation] evento registrado en BD")
    _last_irrigating = irrigating_now

    with _state_lock:
        latest.clear()
        latest.update(data)
        snapshot = dict(latest)
    _store_reading(time.time(), data)
    _broadcast({"event": "sensors", "data": snapshot})


def on_log(client, userdata, level, buf):
    print(f"[mqtt][log] {buf}")


def build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    # certifi provee su propio bundle de CAs actualizado (incluye ISRG Root X1
    # de Let's Encrypt que usa HiveMQ Cloud). Más fiable que el almacén del
    # sistema en Raspberry Pi OS, que puede no tenerlo si no está actualizado.
    client.tls_set(ca_certs=certifi.where())
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_log = on_log
    try:
        client.reconnect_delay_set(1, 120)
    except Exception:
        try:
            client.reconnect_delay_set(min_delay=1, max_delay=120)
        except Exception:
            pass
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
    client.loop_start()
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


@app.route("/api/state")
def api_state():
    """Último catálogo/estado del ESP (canciones, volumen, umbral, neo, ...)."""
    with _state_lock:
        return jsonify(dict(state))


@app.route("/api/history")
def api_history():
    """Histórico de sensores para las gráficas. Query: ?range=5m|30m|1h|6h|24h|all"""
    range_key = request.args.get("range", "1h")
    if range_key not in RANGES:
        range_key = "1h"
    return jsonify(_query_history(range_key))


@app.route("/api/irrigation_log")
def api_irrigation_log():
    """Eventos de inicio de riego en el rango pedido. Devuelve lista de ts en ms."""
    range_key = request.args.get("range", "24h")
    span = RANGES.get(range_key, RANGES["24h"])
    params: tuple = ()
    where = ""
    if span is not None:
        where = "WHERE ts >= ?"
        params = (time.time() - span,)
    with _db_lock:
        cur = _db.execute(
            f"SELECT ts FROM irrigation_log {where} ORDER BY ts ASC", params
        )
        rows = cur.fetchall()
    return jsonify({"events": [int(r[0] * 1000) for r in rows]})


@app.route("/api/stream")
def api_stream():
    """Stream SSE: empuja lecturas ('sensors') y estado ('state') al navegador."""

    def gen():
        q: queue.Queue = queue.Queue(maxsize=50)
        with _state_lock:
            subscribers.add(q)
            init_sensors = json.dumps({"event": "sensors", "data": dict(latest)})
            init_state = json.dumps({"event": "state", "data": dict(state)})
        try:
            yield f"data: {init_sensors}\n\n"
            yield f"data: {init_state}\n\n"
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    # Heartbeat: evita que proxies (Cloudflare, nginx) corten
                    # la conexión por inactividad (~100 s de timeout por defecto).
                    yield ": keepalive\n\n"
        finally:
            with _state_lock:
                subscribers.discard(q)

    return Response(
        gen(),
        mimetype="text/event-stream",
        headers={
            "X-Accel-Buffering": "no",   # desactiva buffering en Cloudflare/nginx
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


# ----- Endpoints de comandos (publican en MQTT) ------------------------------
def _publish(topic: str, payload: str) -> None:
    mqtt_client.publish(topic, payload, qos=1)
    print(f"[mqtt] pub {topic} -> {payload}")


@app.route("/api/pump", methods=["POST"])
def api_pump():
    """Regar ahora ('1') o detener el ciclo ('0'), igual que el botón del ESP."""
    on = bool((request.get_json(silent=True) or {}).get("on", True))
    _publish(TOPIC_CMD_PUMP, "1" if on else "0")
    return jsonify({"ok": True, "pump": on})


@app.route("/api/play", methods=["POST"])
def api_play():
    """Reproduce la canción con el índice indicado en la SD del ESP."""
    idx = int((request.get_json(silent=True) or {}).get("index", 0))
    _publish(TOPIC_CMD_PLAY, str(idx))
    return jsonify({"ok": True, "index": idx})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    """Detiene la reproducción de audio en el ESP."""
    _publish(TOPIC_CMD_STOP, "1")
    return jsonify({"ok": True})


@app.route("/api/volume", methods=["POST"])
def api_volume():
    """Fija el nivel de volumen (0..volumeMax) del ESP."""
    level = int((request.get_json(silent=True) or {}).get("level", 0))
    _publish(TOPIC_CMD_VOLUME, str(level))
    return jsonify({"ok": True, "level": level})


@app.route("/api/threshold", methods=["POST"])
def api_threshold():
    """Cambia el umbral de humedad que dispara el riego automático."""
    value = float((request.get_json(silent=True) or {}).get("value", 0))
    _publish(TOPIC_CMD_THRESHOLD, f"{value:.1f}")
    return jsonify({"ok": True, "value": value})


@app.route("/api/irr_interval", methods=["POST"])
def api_irr_interval():
    """Cambia el intervalo de tiempo entre revisiones de riego (en horas)."""
    value = float((request.get_json(silent=True) or {}).get("value", 24))
    _publish(TOPIC_CMD_IRR_INTERVAL, f"{value:.1f}")
    return jsonify({"ok": True, "value": value})


@app.route("/api/neo", methods=["POST"])
def api_neo():
    """Control de los NeoPixel: brillo (0..255), patrón y color (#RRGGBB)."""
    body = request.get_json(silent=True) or {}
    if "brightness" in body:
        b = max(0, min(255, int(body["brightness"])))
        _publish(TOPIC_CMD_NEO_BRIGHT, str(b))
    if "pattern" in body:
        _publish(TOPIC_CMD_NEO_PATTERN, str(body["pattern"]))
    if "color" in body:
        _publish(TOPIC_CMD_NEO_COLOR, str(body["color"]).lstrip("#"))
    return jsonify({"ok": True})


if __name__ == "__main__":
    # threaded=True es necesario para atender varias conexiones SSE a la vez.
    app.run(host=WEB_HOST, port=WEB_PORT, threaded=True)
