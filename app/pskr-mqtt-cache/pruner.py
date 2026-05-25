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
        first_run = True
        while self._running:
            if self._running:
                paused = False
                try:
                    # Skip pause on startup — flush thread has no backlog yet
                    # and pausing risks OOM if startup prune takes a long time
                    if self.subscriber and not first_run:
                        self.subscriber.pause_for_prune()
                        paused = True
                        time.sleep(0.5)
                    self.db.prune()
                    #self.db.incremental_vacuum()
                except Exception as exc:
                    log.error("Pruner error: %s", exc)
                finally:
                    if paused and self.subscriber:
                        try:
                            self.subscriber.resume_after_prune()
                        except Exception as exc:
                            log.error("Failed to resume flush thread: %s", exc)
                first_run = False
            if self._stop_event.wait(self.interval):
                break

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._run, name="pruner", daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._stop_event.set()
