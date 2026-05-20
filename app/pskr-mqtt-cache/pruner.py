"""
pruner.py — Background thread that periodically prunes expired spots.

Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>

Coordinates with SpotSubscriber to pause the flush thread during pruning
so the pruner can acquire the SQLite write lock without being starved.
"""

import time
import logging
import threading

from .database import SpotDatabase
from .config import DatabaseConfig

log = logging.getLogger(__name__)


class Pruner:
    def __init__(self, db: SpotDatabase, cfg: DatabaseConfig, subscriber=None):
        self.db         = db
        self.subscriber = subscriber   # SpotSubscriber reference for flush coordination
        self.interval   = cfg.prune_interval_minutes * 60
        self._running   = False
        self._stop_event = threading.Event()
        self._thread    = None

    def _run(self):
        log.info("Pruner started (interval=%ds)", self.interval)
        # Prune immediately on startup to clean stale data from volume.
        # No need to pause flush thread here — it hasn't built up a backlog yet.
        self.db.prune()
        self.db.incremental_vacuum()
        while self._running:
            if self._stop_event.wait(self.interval):
                break
            if self._running:
                # Pause the flush thread so pruner can acquire the write lock.
                # Without this, the flush thread's BEGIN IMMEDIATE every 15s
                # starves the pruner indefinitely.
                if self.subscriber:
                    self.subscriber.pause_for_prune()
                    time.sleep(0.2)   # let any in-flight flush commit finish
                try:
                    self.db.prune()
                    self.db.incremental_vacuum()
                finally:
                    # Always resume even if prune raised an exception
                    if self.subscriber:
                        self.subscriber.resume_after_prune()

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._run, name="pruner", daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._stop_event.set()
