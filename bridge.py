#!/usr/bin/env python3
import asyncio
import logging
import os
import time
import aiohttp

### ----------------- CONFIGURATION FROM ENV -----------------
HUB_URL = os.getenv("HUB_URL", "http://192.168.1.128:5171/api")[cite: 8]
TRACCAR_URL = os.getenv("TRACCAR_URL", "http://192.168.1.128:5055")[cite: 8]
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2.0"))[cite: 8]
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "30"))[cite: 8]

# Default fallback coordinates (Kununurra, WA)
DEFAULT_LAT = float(os.getenv("DEFAULT_LAT", "-15.7736"))
DEFAULT_LON = float(os.getenv("DEFAULT_LON", "128.7386"))
### ----------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")[cite: 8]

# Decimal PID mapping: (Attribute Name, Multiplier)
PID_MAP = {
    # Standard OBD-II Mode 01
    260: ("engineLoad", 1),       # 0x104 (%)
    261: ("coolantTemp", 1),      # 0x105 (°C)[cite: 8, 17]
    266: ("fuelPressure", 1),     # 0x10A (kPa)
    267: ("intakePressure", 1),   # 0x10B (MAP kPa)[cite: 17]
    268: ("rpm", 1),              # 0x10C (RPM)[cite: 8, 17]
    269: ("obdSpeed", 1),         # 0x10D (km/h)[cite: 8, 17]
    270: ("timingAdvance", 1),    # 0x10E (deg)[cite: 8, 17]
    271: ("intakeTemp", 1),       # 0x10F (°C)[cite: 8, 17]
    272: ("maf", 1),              # 0x110 (g/s)[cite: 17]
    273: ("throttle", 1),         # 0x111 (%)[cite: 8, 17]
    287: ("runtime", 1),          # 0x11F (s)[cite: 17]
    289: ("distanceMil", 1),      # 0x121 (km)[cite: 17]
    303: ("fuelLevel", 1),        # 0x12F (%)[cite: 8, 17]
    305: ("distanceCleared", 1),  # 0x131 (km)[cite: 17]
    307: ("barometer", 1),        # 0x133 (kPa)[cite: 17]
    322: ("ecuVoltage", 1),       # 0x142 (V)[cite: 17]
    323: ("absoluteLoad", 1),     # 0x143 (%)[cite: 17]
    348: ("oilTemp", 1),          # 0x15C (°C)[cite: 17]
    350: ("fuelRate", 1),         # 0x15E (L/h)[cite: 17]

    # Freematics Hardware & GPS Sensors
    10:  ("lat", 1),              # 0x0A Latitude[cite: 17]
    11:  ("lon", 1),              # 0x0B Longitude[cite: 17]
    12:  ("altitude", 1),         # 0x0C Altitude (m)[cite: 17]
    13:  ("raw_speed", 1),        # 0x0D GPS Speed (km/h)[cite: 17]
    36:  ("battery", 0.01),       # 0x24 Battery voltage (0.01V -> V)[cite: 8, 17]
    129: ("rssi", 1),             # 0x81 Signal strength (dBm)[cite: 17]
    130: ("devTemp", 0.1),        # 0x82 CPU temp (0.1°C -> °C)[cite: 8, 17]
}

device_last_devtick = {}
device_locations = {}

def clean_value(val):
    """Strip trailing checksum characters (e.g. '*3A') and convert to numeric if possible."""
    if isinstance(val, str):
        if "*" in val:
            val = val.split("*")[0]
        try:
            return float(val) if "." in val else int(val)
        except ValueError:
            return val
    return val

async def push_to_traccar(session, sem, params):
    """Send OsmAnd formatted GET request to Traccar."""
    async with sem:[cite: 8]
        try:
            async with session.get(TRACCAR_URL, params=params, timeout=aiohttp.ClientTimeout(total=2)) as resp:[cite: 8]
                return resp.status == 200[cite: 8]
        except Exception as e:
            logging.error(f"Failed to connect to Traccar: {e}")
            return False

async def process_channel(session, sem, ch_summary):
    device_id = ch_summary.get("devid", ch_summary.get("id"))
    if not device_id:
        return

    # 1. Query live PID data from /api/get/<device_id>
    telemetry = {}
    try:
        async with session.get(f"{HUB_URL}/get/{device_id}", timeout=aiohttp.ClientTimeout(total=2)) as resp:
            if resp.status == 200:
                telemetry = await resp.json()
    except Exception as e:
        logging.debug(f"Error fetching /api/get/{device_id}: {e}")

    stats = telemetry.get("stats", {})
    devtick = stats.get("devtick", ch_summary.get("devtick"))

    # Skip push if the hardware tick has not changed (device in standby / sleeping)
    if devtick is not None and device_last_devtick.get(device_id) == devtick:
        return

    params = {
        "id": device_id,
        "timestamp": int(time.time()),[cite: 8]
    }

    # 2. Parse the [[pid, value, age], ...] data array
    raw_data = telemetry.get("data", [])
    for entry in raw_data:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        pid = entry[0]
        val = clean_value(entry[1])

        if pid in PID_MAP:
            name, scale = PID_MAP[pid]
            if scale != 1 and isinstance(val, (int, float)):
                params[name] = round(val * scale, 2)
            else:
                params[name] = val
        else:
            params[f"pid_{pid}"] = val

    # 3. Native Altitude Mapping
    if "alt" in params and "altitude" not in params:
        params["altitude"] = clean_value(params.pop("alt"))

    # 4. Speed Conversion: Traccar OsmAnd protocol expects speed in KNOTS
    # 1 knot = 1.852 km/h (dividing by 1.852 ensures Traccar's UI converts back to exact km/h)
    speed_kmh = params.get("obdSpeed", params.get("raw_speed", 0))
    try:
        params["speed"] = round(float(speed_kmh) / 1.852, 2)
    except (ValueError, TypeError):
        params["speed"] = 0
    params.pop("raw_speed", None)

    # 5. GPS Coordinates & Kununurra Fallback
    lat = params.get("lat")
    lon = params.get("lon")

    if lat is not None and lon is not None:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            device_locations[device_id] = (lat_f, lon_f)
            params["lat"] = lat_f
            params["lon"] = lon_f
            fix_type = "live GPS"
        except (ValueError, TypeError):
            lat = lon = None

    if lat is None or lon is None:
        if device_id in device_locations:
            params["lat"], params["lon"] = device_locations[device_id]
            fix_type = "last-known GPS"
        else:
            params["lat"], params["lon"] = DEFAULT_LAT, DEFAULT_LON
            fix_type = "default fallback (Kununurra)"

    # 6. Automatic Ignition Detection
    rpm = params.get("rpm", 0)
    battery = params.get("battery", 0)
    try:
        is_ign_on = (float(rpm) > 300) or (float(battery) > 13.2)
    except (ValueError, TypeError):
        is_ign_on = False
    params["ignition"] = "true" if is_ign_on else "false"

    # 7. Push to Traccar
    success = await push_to_traccar(session, sem, params)
    if success:
        if devtick is not None:
            device_last_devtick[device_id] = devtick
        logging.info(
            f"[{device_id}] Pushed to Traccar ({fix_type}) | "
            f"Speed: {speed_kmh} km/h | Ignition: {params['ignition']} | "
            f"Batt: {params.get('battery')}V | Coolant: {params.get('coolantTemp')}°C"
        )

async def main():
    logging.info(f"Starting Bridge -> Hub: {HUB_URL} | Traccar: {TRACCAR_URL}")[cite: 8]
    logging.info(f"Default fallback location: {DEFAULT_LAT}, {DEFAULT_LON}")
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)[cite: 8]
    conn = aiohttp.TCPConnector(limit=100, limit_per_host=50)[cite: 8]

    async with aiohttp.ClientSession(connector=conn) as session:[cite: 8]
        while True:[cite: 8]
            start_loop = time.monotonic()[cite: 8]
            try:
                async with session.get(f"{HUB_URL}/channels", timeout=aiohttp.ClientTimeout(total=3)) as resp:[cite: 8]
                    if resp.status == 200:[cite: 8]
                        payload = await resp.json()[cite: 8]
                        channels = payload.get("channels", []) if isinstance(payload, dict) else payload
                        tasks = [process_channel(session, sem, ch) for ch in channels if isinstance(ch, dict)]
                        if tasks:[cite: 8]
                            await asyncio.gather(*tasks)[cite: 8]
            except Exception as e:
                logging.warning(f"Error reading Hub channels: {e}")[cite: 8]

            elapsed = time.monotonic() - start_loop[cite: 8]
            await asyncio.sleep(max(0.1, POLL_INTERVAL - elapsed))[cite: 8]

if __name__ == "__main__":
    asyncio.run(main())[cite: 8]
