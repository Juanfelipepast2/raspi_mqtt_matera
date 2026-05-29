# Dashboard IoT — Raspberry Pi (Matera inteligente)

Este es el lado **Raspberry Pi** del proyecto. Se conecta al broker **HiveMQ
Cloud** (TLS) y cumple dos funciones:

1. **Se suscribe** a los topics que publica el ESP32:
   - `matera/sensors` — lecturas en vivo (humedad de suelo, temperatura,
     humedad ambiente, luz y calidad de aire) que se muestran en tarjetas y se
     **historizan en SQLite** para dibujar gráficas con filtro temporal.
   - `matera/state` — catálogo/estado **retenido** (lista de canciones de la SD,
     canción de riego, volumen, umbral de riego, brillo/patrón/color de los
     NeoPixel y la lista de patrones disponibles). Pobla los controles del
     dashboard y refleja cualquier cambio al instante.
2. **Publica comandos** en `matera/cmd/#` para controlar el ESP32 de forma
   remota: regar ahora / detener bomba, cambiar el umbral de riego, reproducir
   una canción por índice, parar el audio, ajustar el volumen, fijar la canción
   de riego y controlar los NeoPixel (brillo, patrón y color). El ESP32 está
   suscrito al comodín `matera/cmd/#` y reacciona, así que podés gobernar la mata
   **desde cualquier parte del mundo**.

```
   ESP32  ──publica matera/sensors + matera/state──▶  HiveMQ  ──▶  Raspberry Pi (dashboard + gráficas)
   ESP32  ◀────────── suscrito a matera/cmd/# ───────  HiveMQ  ◀──  Raspberry Pi (controles)
```

## Funciones del dashboard

- **Valores en tiempo real** de los 5 sensores + estado de riego (igual que el
  display del ESP).
- **Gráficas** por sensor (Chart.js, servido localmente desde `static/`) con
  filtro temporal: últimos 5 min, 30 min, 1 h, 6 h, 1 día o todo el histórico.
- **Riego** con las mismas políticas que el ESP: botón *Regar ahora* (pulso de
  bomba de 3–4 s) que se **bloquea hasta que termina el ciclo**, botón de parada
  y campo para cambiar el **umbral** de riego automático.
- **Canciones**: lista las pistas de la microSD del ESP; reproducir por índice,
  parar, fijar la canción de riego y control de **volumen**.
- **NeoPixel**: brillo, selección de **patrón** (rainbow, solid, off, breathe,
  comet) y selector de **color** para los patrones de color sólido.

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
cd raspi_mqtt_matera

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

| Método | Ruta            | Descripción                                                        |
|--------|-----------------|--------------------------------------------------------------------|
| GET    | `/`             | Dashboard web                                                      |
| GET    | `/api/data`     | Última lectura de sensores (JSON)                                  |
| GET    | `/api/state`    | Último catálogo/estado del ESP (canciones, volumen, umbral, neo)   |
| GET    | `/api/history`  | Histórico para gráficas. `?range=5m\|30m\|1h\|6h\|24h\|all`         |
| GET    | `/api/stream`   | Stream SSE: eventos `sensors` y `state`                            |
| POST   | `/api/pump`     | `{"on": true}` → regar ahora, `{"on": false}` → detener            |
| POST   | `/api/play`     | `{"index": N}` → reproducir la canción N de la SD                  |
| POST   | `/api/stop`     | Detener el audio                                                   |
| POST   | `/api/volume`   | `{"level": 0..volumeMax}` → fijar el volumen                       |
| POST   | `/api/wsong`    | `{"index": N}` → fijar la canción de riego                         |
| POST   | `/api/threshold`| `{"value": 35}` → cambiar el umbral de riego (%)                   |
| POST   | `/api/neo`      | `{"brightness": 0..255, "pattern": "solid", "color": "#33aa55"}`   |

Cada endpoint POST publica en el topic `matera/cmd/...` correspondiente; el
ESP32 (suscrito a `matera/cmd/#`) ejecuta el comando.

## Formato de los datos

El ESP32 publica en `matera/sensors` un JSON como:

```json
{"soil":42.5,"temp":24.3,"hum":58.1,"lux":820,"ppm":450,"irrigating":false,"ts":123456}
```

- `soil` — humedad del suelo (%) · `temp` — temperatura (°C) · `hum` — humedad
  ambiente (%) · `lux` — luz (BH1750) · `ppm` — calidad de aire (MQ-135) ·
  `irrigating` — `true` si está regando · `ts` — millis() del ESP32.

Y en `matera/state` (RETENIDO) un JSON como:

```json
{"songs":["lluvia.mp3","jardin.mp3"],"wateringSong":1,"volume":3,"volumeMax":4,
 "threshold":35.0,"neoBright":40,"neoBright2":40,"neoPattern":"solid",
 "neoColor":"FF7800","patterns":["rainbow","solid","off","breathe","comet"]}
```

> El histórico se guarda en `history.db` (SQLite, ignorado por git). La opción
> *“Todo el tiempo”* abarca desde la primera lectura almacenada en esa base.
