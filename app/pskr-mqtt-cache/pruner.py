"""
pruner.py — Background thread that periodically prunes expired spots.

Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>

"""

import time
import logging
import threading

from .database import SpotDatabase
from .config import DatabaseConfig

log = logging.getLogger(__name__)


class Pruner:
    def __init__(self, db: SpotDatabase, cfg: DatabaseConfig):
        self.db       = db
        self.interval = cfg.prune_interval_minutes * 60
        # incremental_vacuum() is disk I/O on top of prune()'s own batched
        # deletes + WAL checkpoint. Doing it every cycle is what causes a
        # periodic CPU/IRQ sawtooth at worldwide spot volume. Instead only
        # vacuum every Nth prune — freed pages are still reused by SQLite
        # between vacuums, so this only delays reclaiming disk space, it
        # doesn't change correctness or unbounded growth.
        self.vacuum_every_n = max(1, cfg.vacuum_every_n_prunes)
        self._prune_count = 0
        self._running = False
        self._stop_event = threading.Event()
        self._thread  = None

    def _prune_and_maybe_vacuum(self):
        self.db.prune()
        self._prune_count += 1
        if self._prune_count % self.vacuum_every_n == 0:
            self.db.incremental_vacuum()

    def _run(self):
        log.info("Pruner started (interval=%ds, vacuum every %d prunes)",
                  self.interval, self.vacuum_every_n)
        # Prune immediately on startup to clean stale data from volume
        self._prune_and_maybe_vacuum()
        while self._running:
            if self._stop_event.wait(self.interval):
                break
            if self._running:
                self._prune_and_maybe_vacuum()

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._run, name="pruner", daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._stop_event.set()
