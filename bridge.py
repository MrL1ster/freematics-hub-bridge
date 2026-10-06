#!/usr/bin/env python3
import asyncio
import logging
import os
import time
import aiohttp

# ----------------- CONFIGURATION FROM ENV -----------------
HUB_URL = os.getenv("HUB_URL", "http://192.168.1.128:8080/hub/api")
TRACCAR_URL = os.getenv("TRACCAR_URL", "http://192.168.1.128:5055")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2.0"))
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "30"))
# ----------------------------------------------------------

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

async def push_to_traccar(session, sem, params):
    async with sem:
        try:
            async with session.get(TRACCAR_URL, params=params, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                return resp.status == 200
        except Exception:
            return False

async def process_device(session, sem, device_id, dev_data):
    if not isinstance(dev_data, dict):
        return

    lat = dev_data.get("lat")
    lon = dev_data.get("lon")
    if lat is None or lon is None:
        return

    current_time = dev_data.get("time", int(time.time()))
    if device_timestamps.get(device_id) == current_time:
        return

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

async def main():
    logging.info(f"Starting Bridge -> Hub: {HUB_URL} | Traccar: {TRACCAR_URL}")
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
