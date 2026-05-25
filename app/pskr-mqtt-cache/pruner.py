"""
pruner.py — Background thread that periodically prunes expired spots.

Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>

Coordinates with SpotSubscriber to pause the flush thread during pruning
so the pruner can acquire the SQLite write lock without being starved.

When the flush thread timeout fires (MAX_PAUSE_SECONDS exceeded), the pruner
detects this via abort_check() and stops batching immediately, releasing the
write lock so inserts can resume. Remaining old spots are cleaned up on the
next scheduled prune cycle.
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

    def _abort_prune(self) -> bool:
        """Return True if the pruner should stop batching.

        Fires when the flush thread timeout has cleared _paused_for_prune —
        meaning inserts need to resume and the pruner should release the
        write lock. Remaining old spots are cleaned on the next cycle.
        """
        return (self.subscriber is not None and
                not self.subscriber._paused_for_prune.is_set())

    def _run(self):
        log.info("Pruner started (interval=%ds)", self.interval)
        first_run = True
        while self._running:
            if self._running:
                paused = False
                try:
                    # Skip pause on startup — flush thread has no backlog yet
                    # and pausing risks OOM if startup prune takes a long time.
                    # abort_check is also skipped on first_run since the pause
                    # flag is not set — no timeout can fire.
                    if self.subscriber and not first_run:
                        self.subscriber.pause_for_prune()
                        paused = True
                        time.sleep(0.5)   # let any in-flight flush commit finish

                    # Pass abort_check only for scheduled prune cycles (not startup).
                    # On startup the flush thread is not paused so abort_check
                    # would immediately abort — not what we want.
                    abort = self._abort_prune if (self.subscriber and not first_run) else None
                    self.db.prune(abort_check=abort)

                except Exception as exc:
                    log.error("Pruner error: %s", exc)
                finally:
                    # Always resume flush thread even if prune raised an exception
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
