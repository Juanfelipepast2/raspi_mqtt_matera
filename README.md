# Dashboard IoT — Raspberry Pi (Matera inteligente)

Este es el lado **Raspberry Pi** del proyecto. Se conecta al broker **HiveMQ
Cloud** (TLS) y cumple dos funciones:

1. **Se suscribe** al topic de sensores (`matera/sensors`) que publica el ESP32
   y muestra los datos en vivo en un dashboard web (humedad de suelo,
   temperatura, humedad ambiente, luz y calidad de aire).
2. **Publica comandos** en `matera/cmd/pump` y `matera/cmd/play` para activar la
   bomba de agua o la melodía de riego de forma remota. El ESP32 está suscrito a
   esos topics y reacciona, así que podés regar la mata **desde cualquier parte
   del mundo**.

```
   ESP32  ──publica matera/sensors──▶  HiveMQ Cloud  ──▶  Raspberry Pi (dashboard)
   ESP32  ◀──suscrito a matera/cmd/*──  HiveMQ Cloud  ◀──  Raspberry Pi (botones)
```

## Arquitectura del servidor

- **Flask** sirve el dashboard y una pequeña API REST.
- **paho-mqtt** mantiene la conexión con HiveMQ en un hilo de fondo.
- **SSE (Server-Sent Events)** empuja cada nueva lectura al navegador en vivo
  (sin recargar ni hacer polling).

## Requisitos

- Raspberry Pi con Python 3.9+ (probado en Raspberry Pi OS).
- Un cluster gratuito de **HiveMQ Cloud** con un usuario MQTT creado en
  *Access Management*. Usá **las mismas credenciales y topics** que configuraste
  en el ESP32 (`src/core/config.h`).

## Instalación

```bash
cd intelligent_pot/raspi

# 1. Entorno virtual + dependencias
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configuración (credenciales de HiveMQ)
cp .env.example .env
nano .env            # completá MQTT_HOST, MQTT_USER, MQTT_PASS

# 3. Ejecutar
python app.py
```

Abrí el dashboard en `http://<IP-de-la-raspi>:8000`.

> El `client-id` de la Raspi (`MQTT_CLIENT_ID`) debe ser **distinto** al del
> ESP32: HiveMQ desconecta a un cliente si otro se conecta con el mismo id.

## Arranque automático (opcional)

Para que el dashboard se levante solo al encender la Raspberry Pi, instalá el
servicio systemd incluido (revisá las rutas y el usuario dentro del archivo):

```bash
sudo cp matera-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now matera-dashboard
journalctl -u matera-dashboard -f     # logs en vivo
```

## API REST

| Método | Ruta          | Descripción                                              |
|--------|---------------|----------------------------------------------------------|
| GET    | `/`           | Dashboard web                                            |
| GET    | `/api/data`   | Última lectura conocida (JSON)                           |
| GET    | `/api/stream` | Stream SSE con cada nueva lectura                        |
| POST   | `/api/pump`   | Body `{"on": true}` → regar, `{"on": false}` → detener   |
| POST   | `/api/play`   | Dispara la melodía de riego                              |

## Formato de los datos

El ESP32 publica en `matera/sensors` un JSON como:

```json
{"soil":42.5,"temp":24.3,"hum":58.1,"lux":820,"ppm":450,"irrigating":false,"ts":123456}
```

- `soil` — humedad del suelo (%)
- `temp` — temperatura (°C, DHT11)
- `hum`  — humedad ambiente (%, DHT11)
- `lux`  — luz (BH1750)
- `ppm`  — calidad de aire / CO₂-eq (MQ-135)
- `irrigating` — `true` si la bomba está regando en este momento
- `ts`   — millis() del ESP32 al momento de la lectura
