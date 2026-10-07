#!/usr/bin/env python3
import asyncio
import logging
import os
import time
import aiohttp

### ----------------- CONFIGURATION FROM ENV -----------------
HUB_URL = os.getenv("HUB_URL", "http://192.168.1.128:5171/api")
TRACCAR_URL = os.getenv("TRACCAR_URL", "http://192.168.1.128:5055")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2.0"))
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "30"))

# Default fallback coordinates (Kununurra, WA)
DEFAULT_LAT = float(os.getenv("DEFAULT_LAT", "-15.7736"))
DEFAULT_LON = float(os.getenv("DEFAULT_LON", "128.7386"))
### ----------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PID_MAP = {
    0x10C: "rpm",
    0x10D: "obdSpeed",
    0x104: "engineLoad",
    0x105: "coolantTemp",
    0x10F: "intakeTemp",
    0x111: "throttle",
    0x12F: "fuelLevel",
    0x10E: "timingAdvance",
    0x24:  "battery",
    0x82:  "devTemp",
}

device_timestamps = {}
device_locations = {}  # Stores last known GPS fix: {device_id: (lat, lon)}

async def push_to_traccar(session, sem, params):
    async with sem:
        try:
            async with session.get(TRACCAR_URL, params=params, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                return resp.status == 200
        except Exception as e:
            logging.error(f"Failed to connect to Traccar: {e}")
            return False

async def process_device(session, sem, device_id, dev_data):
    if not isinstance(dev_data, dict):
        return

    current_time = dev_data.get("time", int(time.time()))
    if device_timestamps.get(device_id) == current_time:
        return

    # Evaluate GPS fix and fallback logic
    lat = dev_data.get("lat")
    lon = dev_data.get("lon")

    if lat is not None and lon is not None:
        # Live satellite lock acquired -> update cached location
        device_locations[device_id] = (lat, lon)
        fix_type = "live GPS"
    elif device_id in device_locations:
        # Fall back to previously recorded position
        lat, lon = device_locations[device_id]
        fix_type = "last-known GPS"
    else:
        # Never locked -> default to Kununurra, WA
        lat, lon = DEFAULT_LAT, DEFAULT_LON
        fix_type = "default fallback (Kununurra)"

    params = {
        "id": device_id,
        "lat": lat,
        "lon": lon,
        "timestamp": current_time,
        "speed": dev_data.get("speed", 0),
        "altitude": dev_data.get("alt", 0),
    }

    stats = dev_data.get("stats", {}) or dev_data.get("pids", {})
    for pid_key, val in stats.items():
        try:
            pid_int = int(pid_key, 16) if isinstance(pid_key, str) and pid_key.startswith("0x") else int(pid_key)
            if pid_int in PID_MAP:
                params[PID_MAP[pid_int]] = val
            else:
                params[f"pid_{hex(pid_int)}"] = val
        except (ValueError, TypeError):
            continue

    success = await push_to_traccar(session, sem, params)
    if success:
        device_timestamps[device_id] = current_time
        logging.info(f"[{device_id}] Pushed to Traccar using {fix_type} ({lat}, {lon})")
    else:
        logging.warning(f"[{device_id}] Traccar rejected update (HTTP non-200)")

async def main():
    logging.info(f"Starting Bridge -> Hub: {HUB_URL} | Traccar: {TRACCAR_URL}")
    logging.info(f"Default fallback location: {DEFAULT_LAT}, {DEFAULT_LON}")
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    conn = aiohttp.TCPConnector(limit=100, limit_per_host=50)

    async with aiohttp.ClientSession(connector=conn) as session:
        while True:
            start_loop = time.monotonic()
            all_devices = {}
            try:
                async with session.get(f"{HUB_URL}/channels", timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        all_devices = await resp.json()
            except Exception as e:
                logging.warning(f"Error reading Hub channels: {e}")

            if isinstance(all_devices, dict):
                tasks = [
                    process_device(session, sem, dev_id, data)
                    for dev_id, data in all_devices.items()
                ]
                if tasks:
                    await asyncio.gather(*tasks)

            elapsed = time.monotonic() - start_loop
            sleep_time = max(0.1, POLL_INTERVAL - elapsed)
            await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    asyncio.run(main())
