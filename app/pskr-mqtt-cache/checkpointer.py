"""
checkpointer.py — Background thread that checkpoints the WAL on a short,
fixed cadence, independent of the (much slower) prune/vacuum cycle.

Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>

Why this exists: at worldwide spot volume, letting the WAL grow untouched
for a full prune_interval_minutes (10-15 min) let it accumulate to over a
gigabyte before anything drained it — then the prune cycle's checkpoint
had to move that entire backlog in one call, showing up as a periodic
disk I/O / CPU spike on the host. Checkpointing PASSIVE-ly every ~60s
keeps the WAL consistently small, so there's never a large backlog for
prune()'s own checkpoint to catch up on.
"""

import logging
import threading

from .database import SpotDatabase
from .config import DatabaseConfig

log = logging.getLogger(__name__)


class Checkpointer:
    def __init__(self, db: SpotDatabase, cfg: DatabaseConfig):
        self.db       = db
        self.interval = cfg.checkpoint_interval_seconds
        self._running = False
        self._stop_event = threading.Event()
        self._thread  = None

    def _run(self):
        log.info("Checkpointer started (interval=%ds)", self.interval)
        while self._running:
            if self._stop_event.wait(self.interval):
                break
            if self._running:
                self.db.checkpoint()

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._run, name="checkpointer", daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._stop_event.set()
