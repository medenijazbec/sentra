"""
Lightweight headless sampler for sentra.

This replaces the Streamlit-driven sampling loop so we can keep writing
host/GPU data into SQLite for the new C# API + React UI.
"""

from __future__ import annotations

import time
import traceback

from config import config
from collector import logger


def main() -> None:
    prev_disk = None
    prev_net = None
    last_tick = time.time()

    while True:
        start = time.time()
        interval_s = max(start - last_tick, 0.001)

        try:
            snapshot, gpus, containers, prev_disk, prev_net = logger.collect_and_store(
                prev_disk=prev_disk,
                prev_net=prev_net,
                interval_s=interval_s,
            )
        except Exception:
            traceback.print_exc()

        last_tick = start
        elapsed = time.time() - start
        sleep_for = max(config.SAMPLE_INTERVAL - elapsed, 0.5)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
