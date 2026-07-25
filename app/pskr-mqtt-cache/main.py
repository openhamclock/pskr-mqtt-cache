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
import logging
import argparse

import anyio.to_thread
import uvicorn

from .config import load as load_config
from .database import SpotDatabase
from .subscriber import SpotSubscriber
from .pruner import Pruner
from .checkpointer import Checkpointer
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
    pruner = Pruner(db, cfg.database)
    pruner.start()

    # ── Checkpointer ──────────────────────────────────────────────────────────
    # Runs independently of the pruner on a short cadence so the WAL never
    # gets a chance to build a large backlog between prune cycles.
    checkpointer = Checkpointer(db, cfg.database)
    checkpointer.start()

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    def shutdown(signum, frame):
        log.info("Shutting down (signal %d) …", signum)
        subscriber.stop()
        pruner.stop()
        checkpointer.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # ── Thread pool cap ───────────────────────────────────────────────────────
    # get_spots() is a sync `def`, so Starlette dispatches every call onto its
    # anyio worker thread pool. That pool's default ceiling is high, so a burst
    # of concurrent /spots requests spawns threads without meaningful limit.
    # Combined with the old thread-local SQLite connections, each new thread
    # also leaked a connection — and its mmap/cache reservation — permanently.
    # This is the mechanism behind the observed 10 -> 36 thread and
    # 23.8g -> 75.8g VIRT growth inside one minute.
    # Cap the pool so concurrency is bounded and excess requests queue instead.
    async def _apply_thread_limit():
        anyio.to_thread.current_default_thread_limiter().total_tokens = \
            cfg.api.thread_pool_limit

    @api.app.on_event("startup")
    async def _on_startup():
        await _apply_thread_limit()
        log.info("Sync thread pool capped at %d", cfg.api.thread_pool_limit)

    # ── HTTP Server (blocks until killed) ─────────────────────────────────────
    # NOTE: uvicorn is intentionally still single-process here. Raising
    # workers > 1 would fork the MQTT subscriber and pruner too, producing
    # duplicate firehose subscriptions and multiple writers against one SQLite
    # file. Scaling the API horizontally requires splitting ingest into its own
    # process first — do that before reaching for workers > 1.
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
