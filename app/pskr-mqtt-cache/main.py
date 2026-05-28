"""
main.py — Entry point for pskr-mqtt-cache.
Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>
Starts three concurrent components:
  1. MQTT subscriber thread — receives spots and writes to SQLite
  2. Background pruner thread — removes spots older than max_age_hours
  3. FastAPI/uvicorn HTTP server — serves /spots and /status queries
Usage:
    python -m pskr_mqtt_cache [--config /path/to/config.yaml]
    # or
    python main.py [--config /path/to/config.yaml]
"""
import sys
import signal
# Ensure zombie processes are reaped automatically.
# Uvicorn uses os.fork() internally and without this handler
# child processes accumulate as zombies.
signal.signal(signal.SIGCHLD, signal.SIG_DFL)
import logging
import argparse
import uvicorn
from .config import load as load_config
from .database import SpotDatabase
from .subscriber import SpotSubscriber
from .pruner import Pruner
from . import api


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(description="PSKReporter MQTT spot cache service")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(cfg.logging.level)
    log = logging.getLogger("pskr_mqtt_cache.main")
    log.info("Starting pskr-mqtt-cache")

    # ── Database ──────────────────────────────────────────────────────────────
    db = SpotDatabase(cfg.database)

    # ── Wire database and config into the API module ──────────────────────────
    api._db  = db
    api._cfg = cfg.api

    # ── MQTT Subscriber ───────────────────────────────────────────────────────
    subscriber = SpotSubscriber(cfg.mqtt, db)
    api._subscriber = subscriber
    subscriber.start()

    # ── Pruner ────────────────────────────────────────────────────────────────
    # Pass subscriber so pruner can coordinate flush pausing to avoid
    # write lock starvation on busy systems.
    pruner = Pruner(db, cfg.database, subscriber)
    pruner.start()

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    def shutdown(signum, frame):
        log.info("Shutting down (signal %d) …", signum)
        subscriber.stop()
        pruner.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # ── HTTP Server (blocks until killed) ─────────────────────────────────────
    log.info("Starting API on %s:%d", cfg.api.host, cfg.api.port)
    uvicorn.run(
        api.app,
        host=cfg.api.host,
        port=cfg.api.port,
        log_level=cfg.logging.level.lower(),
        access_log=False,   # Suppress per-request logs — too noisy at scale
    )


if __name__ == "__main__":
    main()
