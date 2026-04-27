"""
database.py — SQLite database layer for pskr-mqtt-cache.

Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>

Schema mirrors the PSKReporter receptionReport fields needed by HamClock.
WAL mode allows concurrent reads from the FastAPI layer while the MQTT
subscriber is continuously writing.
"""

import time
import logging
import sqlite3
import threading
from pathlib import Path
from contextlib import contextmanager

from .config import DatabaseConfig

log = logging.getLogger(__name__)




class SpotDatabase:
    def __init__(self, cfg: DatabaseConfig):
        self.path = cfg.path
        self.max_age_sec = cfg.max_age_hours * 3600
        self.prune_interval_sec = cfg.prune_interval_minutes * 60
        self.cache_size_kb = cfg.cache_size_mb * 1024

        # Ensure parent directory exists
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

        # Thread-local connections — each thread gets its own SQLite connection
        # This is the correct pattern for SQLite with multiple threads
        self._local = threading.local()

        # Initialize schema on startup
        with self._conn() as db:
            self._init_schema(db)

        log.info("Database initialized: %s", self.path)

    def _connect(self) -> sqlite3.Connection:
        """Create a new SQLite connection with optimal settings."""
        db = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        db.row_factory = sqlite3.Row

        # WAL: concurrent readers don't block on writer
        db.execute("PRAGMA journal_mode=WAL")

        # NORMAL sync is safe with WAL and much faster than FULL
        db.execute("PRAGMA synchronous=NORMAL")

        db.execute("PRAGMA temp_store=MEMORY")
        db.execute(f"PRAGMA cache_size=-{self.cache_size_kb}")

        # Allow readers to proceed even during writes
        db.execute("PRAGMA read_uncommitted=0")

        # Force the WAL to truncate to 4MB after a successful checkpoint
        db.execute("PRAGMA journal_size_limit = 4194304")

        return db

    @contextmanager
    def _conn(self):
        """Thread-local connection context manager."""
        if not hasattr(self._local, "db") or self._local.db is None:
            self._local.db = self._connect()
        try:
            yield self._local.db
        except Exception:
            self._local.db.rollback()
            raise

    def _init_schema(self, db: sqlite3.Connection):
        db.execute("""
            CREATE TABLE IF NOT EXISTS spots (
                sq      INTEGER,                -- PSKReporter sequence number (may be absent)
                t       INTEGER NOT NULL,       -- t_tx (normalized transmission start time)
                s_grid  TEXT    NOT NULL DEFAULT '',
                s_call  TEXT    NOT NULL DEFAULT '',
                r_grid  TEXT    NOT NULL DEFAULT '',
                r_call  TEXT    NOT NULL DEFAULT '',
                mode    TEXT    NOT NULL DEFAULT '',
                freq    INTEGER NOT NULL DEFAULT 0,
                snr     INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (t, s_call, r_call, freq)
            )
        """)

        # Indexes for the two HamClock query patterns
        db.execute("CREATE INDEX IF NOT EXISTS idx_r_grid ON spots(r_grid)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_s_grid ON spots(s_grid)")

        # Index on t for pruning and maxage filtering
        db.execute("CREATE INDEX IF NOT EXISTS idx_t ON spots(t)")

        # Indexes for callsign queries
        db.execute("CREATE INDEX IF NOT EXISTS idx_r_call ON spots(r_call)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_s_call ON spots(s_call)")

        db.commit()

        # Enable incremental auto_vacuum so freed pages can be reclaimed
        # without a full VACUUM. Doesn't seem to take before table creation
        # but with a manual VACUUM it will take.
        db.execute("PRAGMA auto_vacuum = INCREMENTAL")
        db.execute("VACUUM")
        db.commit()

    def insert_spot(self, spot: dict) -> bool:
        """
        Insert a single spot. Returns True if inserted, False if duplicate.
        Uses INSERT OR IGNORE so duplicates (same t/s_call/r_call/freq) are dropped.
        sq is optional — not all MQTT messages include it.
        """
        try:
            # Use t (decode time) — consistent with CSI behavior
            # Fall back to t_tx if t is absent
            t = spot.get("t") or spot.get("t_tx")
            if t is None:
                return False   # timestamp is mandatory — skip silently

            sq   = spot.get("sq")
            freq = spot.get("f")
            snr  = spot.get("rp")
            sl   = spot.get("sl") or ""
            rl   = spot.get("rl") or ""

            # Normalize mode and callsigns — prevents dedup failures from case/whitespace
            mode = (spot.get("md") or "").strip().upper()
            sc   = (spot.get("sc") or "").strip().upper()
            rc   = (spot.get("rc") or "").strip().upper()

            with self._conn() as db:
                cur = db.execute("""
                    INSERT OR IGNORE INTO spots
                        (sq, t, s_grid, s_call, r_grid, r_call, mode, freq, snr)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    int(sq)   if sq   is not None else None,
                    int(t),
                    sl[:6].upper(),
                    sc,
                    rl[:6].upper(),
                    rc,
                    mode,
                    int(freq) if freq is not None else 0,
                    int(snr)  if snr  is not None else 0,
                ))
                db.commit()
                return cur.rowcount > 0
        except Exception as exc:
            log.error("Insert error: %s  spot=%s", exc, spot)
            return False

    def insert_batch(self, spots: list[dict]) -> int:
        """
        Bulk insert a list of spot dicts. Returns number of rows inserted.
        More efficient than individual inserts for batch backfill.
        """
        if not spots:
            return 0
        rows = []
        for spot in spots:
            try:
                sq   = spot.get("sq")
                t    = spot.get("t") or spot.get("t_tx")
                if t is None:
                    continue
                freq = spot.get("f")
                snr  = spot.get("rp")
                sl   = spot.get("sl") or ""
                rl   = spot.get("rl") or ""

                # Normalize mode and callsigns
                mode = (spot.get("md") or "").strip().upper()
                sc   = (spot.get("sc") or "").strip().upper()
                rc   = (spot.get("rc") or "").strip().upper()

                rows.append((
                    int(sq)   if sq   is not None else None,
                    int(t),
                    sl[:6].upper(),
                    sc,
                    rl[:6].upper(),
                    rc,
                    mode,
                    int(freq) if freq is not None else 0,
                    int(snr)  if snr  is not None else 0,
                ))
            except (KeyError, ValueError, TypeError):
                continue

        try:
            with self._conn() as db:
                cur = db.executemany("""
                    INSERT OR IGNORE INTO spots
                        (sq, t, s_grid, s_call, r_grid, r_call, mode, freq, snr)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)
                db.commit()
                return cur.rowcount
        except Exception as exc:
            log.error("Batch insert error: %s", exc)
            return 0

    def prune(self) -> int:
        """Delete spots older than max_age_sec in batches to avoid long write locks."""
        cutoff = int(time.time()) - self.max_age_sec
        total = 0
        batch_size = 10000
        try:
            while True:
                with self._conn() as db:
                    cur = db.execute(
                        "DELETE FROM spots WHERE t < ? ORDER BY t ASC LIMIT ?",
                        (cutoff, batch_size)
                    )
                    db.commit()
                    count = cur.rowcount
                    total += count
                    if count < batch_size:
                        break
                # Brief pause between batches to yield to MQTT writer
                time.sleep(0.05)
            if total:
                log.info("Pruned %d spots older than %dh", total, self.max_age_sec // 3600)
                # Force a checkpoint to move all that deleted space back to the DB
                with self._conn() as db:
                    # Use FULL checkpoint mode. PASSIVE is a no-op if another connection
                    # is writing, which is likely. FULL waits for writers to finish,
                    # ensuring the checkpoint runs. This is critical for moving deleted
                    # pages from the WAL to the main DB freelist so that
                    # incremental_vacuum can reclaim the space. It does not block readers.
                    # We'll use NORMAL to be less aggressive but still make some happen
                    res = db.execute("PRAGMA wal_checkpoint(NORMAL)").fetchone()
                    if res and res[2] > 0:
                        log.info("Checkpointed %d pages from WAL to main database.", res[2])
                    else:
                        log.info("WAL checkpoint ran, but no pages were moved (busy=%s, log=%s, checkpointed=%s).", res[0], res[1], res[2])
            return total
        except Exception as exc:
            log.error("Prune error: %s", exc)
            return 0

    def query_spots(self, bygrid: str = "", ofgrid: str = "",
                    bycall: str = "", ofcall: str = "",
                    maxage: int = 900) -> list[sqlite3.Row]:
        """
        Query spots by grid prefix, callsign, and maxage.

        ofgrid  — sender grid prefix (s_grid LIKE 'XX00%')
        bygrid  — receiver grid prefix (r_grid LIKE 'XX00%')
        ofcall  — sender callsign exact match (s_call = ?)
        bycall  — receiver callsign exact match (r_call = ?)
        maxage  — seconds back from now

        Returns list of tuples: (t, s_grid, s_call, r_grid, r_call, mode, freq, snr)
        """
        cutoff = int(time.time()) - maxage

        sql = """
            SELECT t, s_grid, s_call, r_grid, r_call, mode, freq, snr
            FROM spots
            WHERE t >= ?
        """
        params = [cutoff]

        if ofgrid:
            sql += " AND s_grid LIKE ?"
            params.append(ofgrid.upper() + "%")

        if bygrid:
            sql += " AND r_grid LIKE ?"
            params.append(bygrid.upper() + "%")

        if ofcall:
            sql += " AND s_call = ?"
            params.append(ofcall.upper())

        if bycall:
            sql += " AND r_call = ?"
            params.append(bycall.upper())

        sql += " ORDER BY t DESC"

        try:
            with self._conn() as db:
                cur = db.execute(sql, params)
                return cur.fetchall()
        except Exception as exc:
            log.error("Query error: %s", exc)
            return []

    def incremental_vacuum(self, pages: int = 0) -> None:
        """Reclaim up to `pages` freed pages from the database file.
        Called after pruning to gradually shrink the file without downtime."""
        try:
            with self._conn() as db:
                before = db.execute("PRAGMA freelist_count;").fetchone()[0]
                if before == 0:
                    return

                # incremental_vacuum must be run outside a transaction.
                # Setting isolation_level to None enables autocommit mode.
                original_isolation_level = db.isolation_level
                db.isolation_level = None
                try:
                    # When pages is 0 (the default), it would vacuum all
                    # free pages. But we know it's just going to create more
                    # so let's just vacuum half.
                    if pages == 0:
                        some_pages = before // 2 # less aggressively clean
                        vacuum_sql = f"PRAGMA incremental_vacuum({some_pages})"
                    else:
                        vacuum_sql = f"PRAGMA incremental_vacuum({pages})"

                    # We must consume all results for the pragma to run to completion.
                    # fetchone() may cause it to stop after processing a small number
                    # of pages. fetchall() ensures the entire freelist is processed.
                    db.execute(vacuum_sql + ";").fetchall()
                finally:
                    db.isolation_level = original_isolation_level

                after = db.execute("PRAGMA freelist_count;").fetchone()[0]
                pages_recovered = before - after
                if pages_recovered > 0:
                    page_size = db.execute("PRAGMA page_size").fetchone()[0]
                    kb_recovered = (pages_recovered * page_size) / 1024
                    log.info(f"Cleaned {pages_recovered} pages (~{kb_recovered:.0f} KB); Remaining: {after}")

        except Exception as exc:
            log.error("Incremental vacuum error: %s", exc)

    def count(self) -> int:
        try:
            with self._conn() as db:
                return db.execute("SELECT COUNT(*) FROM spots").fetchone()[0]
        except Exception:
            return 0

    def oldest_newest(self) -> tuple[int | None, int | None]:
        """Return (oldest_t, newest_t) for status reporting."""
        try:
            with self._conn() as db:
                row = db.execute("SELECT MIN(t), MAX(t) FROM spots").fetchone()
                return (row[0], row[1]) if row else (None, None)
        except Exception:
            return None, None
