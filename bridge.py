#!/usr/bin/env python3
import asyncio
import logging
import os
import time
import aiohttp

HUB_URL = os.getenv("HUB_URL", "http://192.168.1.128:5171/api")
TRACCAR_URL = os.getenv("TRACCAR_URL", "http://192.168.1.128:5055")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2.0"))
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "30"))

DEFAULT_LAT = float(os.getenv("DEFAULT_LAT", "-15.7736"))
DEFAULT_LON = float(os.getenv("DEFAULT_LON", "128.7386"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Decimal PID mapping: (Attribute Name, Multiplier)
PID_MAP = {
    # Mode 01 PIDs
    260: ("engineLoad", 1),       # 0x104 (%)
    261: ("coolantTemp", 1),      # 0x105 (°C)
    266: ("fuelPressure", 1),     # 0x10A (kPa)
    267: ("intakePressure", 1),   # 0x10B (MAP kPa)
    268: ("rpm", 1),              # 0x10C (RPM)
    269: ("obdSpeed", 1),         # 0x10D (km/h)
    270: ("timingAdvance", 1),    # 0x10E (deg)
    271: ("intakeTemp", 1),       # 0x10F (°C)
    272: ("maf", 1),              # 0x110 (g/s)
    273: ("throttle", 1),         # 0x111 (%)
    287: ("runtime", 1),          # 0x11F (s)
    303: ("fuelLevel", 1),        # 0x12F (%)
    322: ("ecuVoltage", 1),       # 0x142 (V)
    348: ("oilTemp", 1),          # 0x15C (°C)
    # Freematics Custom Sensors
    10:  ("lat", 1),              # 0x0A Latitude
    11:  ("lon", 1),              # 0x0B Longitude
    12:  ("alt", 1),              # 0x0C Altitude
    13:  ("speed", 1),            # 0x0D Speed
    36:  ("battery", 0.01),       # 0x24 Battery voltage (0.01V -> V)
    129: ("rssi", 1),             # 0x81 Signal strength (dBm)
    130: ("devTemp", 0.1),        # 0x82 CPU temp (0.1°C -> °C)
}

device_last_devtick = {}
device_locations = {}

async def push_to_traccar(session, sem, params):
    async with sem:
        try:
            async with session.get(TRACCAR_URL, params=params, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                return resp.status == 200
        except Exception as e:
            logging.error(f"Failed to connect to Traccar: {e}")
            return False

async def process_channel(session, sem, ch_summary):
    device_id = ch_summary.get("devid", ch_summary.get("id"))
    if not device_id:
        return

    # 1. Fetch live telemetry from /api/get/<device_id>
    telemetry = {}
    try:
        async with session.get(f"{HUB_URL}/get/{device_id}", timeout=aiohttp.ClientTimeout(total=2)) as resp:
            if resp.status == 200:
                telemetry = await resp.json()
    except Exception as e:
        logging.debug(f"Error fetching /api/get/{device_id}: {e}")

    stats = telemetry.get("stats", {})
    devtick = stats.get("devtick", ch_summary.get("devtick"))

    # Skip push if the hardware tick has not changed (device in standby)
    if devtick is not None and device_last_devtick.get(device_id) == devtick:
        return

    params = {
        "id": device_id,
        "timestamp": int(time.time()),
        "speed": 0,
        "altitude": 0,
    }

    # 2. Parse the [[pid, value, age], ...] data array
    raw_data = telemetry.get("data", [])
    for entry in raw_data:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        pid, val = entry[0], entry[1]

        if pid in PID_MAP:
            name, scale = PID_MAP[pid]
            if scale != 1 and isinstance(val, (int, float)):
                params[name] = round(val * scale, 2)
            else:
                params[name] = val
        else:
            params[f"pid_{pid}"] = val

    # GPS coordinates handling
    lat, lon = params.get("lat"), params.get("lon")
    if lat is not None and lon is not None:
        device_locations[device_id] = (lat, lon)
        fix_type = "live GPS"
    elif device_id in device_locations:
        params["lat"], params["lon"] = device_locations[device_id]
        fix_type = "last-known GPS"
    else:
        params["lat"], params["lon"] = DEFAULT_LAT, DEFAULT_LON
        fix_type = "default fallback (Kununurra)"

    success = await push_to_traccar(session, sem, params)
    if success:
        if devtick is not None:
            device_last_devtick[device_id] = devtick
        logging.info(f"[{device_id}] Pushed to Traccar: {fix_type} | Attributes: {list(params.keys())}")

async def main():
    logging.info(f"Starting Bridge -> Hub: {HUB_URL} | Traccar: {TRACCAR_URL}")
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    conn = aiohttp.TCPConnector(limit=100, limit_per_host=50)

    async with aiohttp.ClientSession(connector=conn) as session:
        while True:
            start_loop = time.monotonic()
            try:
                async with session.get(f"{HUB_URL}/channels", timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        payload = await resp.json()
                        channels = payload.get("channels", []) if isinstance(payload, dict) else payload
                        tasks = [process_channel(session, sem, ch) for ch in channels if isinstance(ch, dict)]
                        if tasks:
                            await asyncio.gather(*tasks)
            except Exception as e:
                logging.warning(f"Error reading Hub channels: {e}")

            elapsed = time.monotonic() - start_loop
            await asyncio.sleep(max(0.1, POLL_INTERVAL - elapsed))

if __name__ == "__main__":
    asyncio.run(main())
