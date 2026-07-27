"""
database.py — SQLite database layer for pskr-mqtt-cache.

Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>

Schema mirrors the PSKReporter receptionReport fields needed by HamClock.
WAL mode allows concurrent reads from the FastAPI layer while the MQTT
subscriber is continuously writing.

Connection model
----------------
Reads come from a FIXED-SIZE pool of connections (read_pool_size). Writes all
go through ONE dedicated connection guarded by a lock.

This replaces the previous thread-local scheme, where every thread that ever
touched the DB lazily opened its own connection and kept it forever. Because
the API's sync endpoints run on Starlette's thread pool, connection count grew
with request concurrency — and each connection carried its own mmap_size and
cache_size reservation, so thread growth showed up as multi-gigabyte VIRT
growth and runaway CPU. A bounded pool makes connection count independent of
load.
"""

import time
import queue
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
        self.mmap_size_bytes = cfg.mmap_size_mb * 1024 * 1024
        self.read_pool_size = max(1, cfg.read_pool_size)

        # Ensure parent directory exists
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

        # ── Single dedicated writer ───────────────────────────────────────────
        # All INSERT/DELETE/checkpoint/vacuum traffic serializes through this
        # one connection. SQLite only permits one writer at a time anyway, so
        # a pool of writers would just contend on the same lock.
        self._write_lock = threading.Lock()
        self._write_db = self._connect()

        # Initialize schema on startup
        self._init_schema(self._write_db)

        # ── Bounded read pool ─────────────────────────────────────────────────
        # LIFO so hot connections stay hot and idle ones stay cold.
        self._read_pool: queue.LifoQueue = queue.LifoQueue(maxsize=self.read_pool_size)
        for _ in range(self.read_pool_size):
            self._read_pool.put(self._connect())

        log.info("Database initialized: %s (read_pool=%d, mmap=%dMB, cache=%dMB)",
                 self.path, self.read_pool_size,
                 self.mmap_size_bytes // (1024 * 1024), self.cache_size_kb // 1024)

    def _connect(self) -> sqlite3.Connection:
        """Create a new SQLite connection with optimal settings."""
        db = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        db.row_factory = sqlite3.Row

        # WAL: concurrent readers don't block on writer
        db.execute("PRAGMA journal_mode=WAL")

        # NORMAL sync is safe with WAL and much faster than FULL
        db.execute("PRAGMA synchronous=NORMAL")

        # CRITICAL: This allows LIKE 'ABC%' to use indexes.
        # Since we store data in UPPER case, this makes grid queries
        # O(log N) instead of O(N).
        db.execute("PRAGMA case_sensitive_like = ON")

        db.execute("PRAGMA temp_store=MEMORY")

        # NOTE: cache_size and mmap_size are PER CONNECTION, not per process.
        # They are now sized on the assumption that read_pool_size + 1
        # connections exist simultaneously. Multiply before raising either.
        db.execute(f"PRAGMA cache_size=-{self.cache_size_kb}")

        # Allow readers to proceed even during writes
        db.execute("PRAGMA read_uncommitted=0")

        # Force the WAL to truncate to 4MB after a successful checkpoint
        db.execute("PRAGMA journal_size_limit = 4194304")

        # Memory-mapped I/O reduces CPU spent on I/O, but each connection
        # reserves this much address space. Keep it modest now that the pool
        # is fixed-size; total reservation is (read_pool_size + 1) * mmap_size.
        db.execute(f"PRAGMA mmap_size={self.mmap_size_bytes}")

        return db

    @contextmanager
    def _read_conn(self):
        """Borrow a connection from the bounded read pool, always return it."""
        db = self._read_pool.get()
        try:
            yield db
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
            raise
        finally:
            self._read_pool.put(db)

    @contextmanager
    def _write_conn(self):
        """Exclusive access to the single writer connection."""
        with self._write_lock:
            try:
                yield self._write_db
            except Exception:
                self._write_db.rollback()
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

        # Composite indexes: filter by grid/call AND time in a single pass.
        # These are significantly more efficient for the HamClock query pattern.
        db.execute("CREATE INDEX IF NOT EXISTS idx_r_grid_t ON spots(r_grid, t)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_s_grid_t ON spots(s_grid, t)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_r_call_t ON spots(r_call, t)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_s_call_t ON spots(s_call, t)")

        # Standalone index for the Pruner
        db.execute("CREATE INDEX IF NOT EXISTS idx_t ON spots(t)")
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

            with self._write_conn() as db:
                db.execute("BEGIN IMMEDIATE")
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
            with self._write_conn() as db:
                db.execute("BEGIN IMMEDIATE")
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

    def checkpoint(self) -> int:
        """
        Run a PASSIVE WAL checkpoint. Safe to call frequently and from
        multiple call sites — PASSIVE never blocks the MQTT writer, it
        just moves whatever WAL frames it currently can.

        Calling this often (e.g. every 60s) instead of only after a
        15-minute prune cycle keeps the WAL small continuously, instead
        of letting it accumulate for the full interval and then having
        to move a large backlog in a single pass.
        """
        try:
            with self._write_conn() as db:
                res = db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                if res and res[2] > 0:  # res[2] is the number of pages checkpointed
                    log.info("Checkpointed %d pages from WAL to main database.", res[2])
                    return res[2]
                return 0
        except Exception as exc:
            log.error("Checkpoint error: %s", exc)
            return 0

    def prune(self) -> int:
        """Delete spots older than max_age_sec in batches to avoid long write locks."""
        cutoff = int(time.time()) - self.max_age_sec
        total = 0
        batch_size = 10000
        try:
            while True:
                with self._write_conn() as db:
                    db.execute("BEGIN IMMEDIATE")
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
                # This is now just a top-up — the Checkpointer thread already
                # keeps the WAL small on its own short cadence, so this call
                # should normally have little left to do.
                self.checkpoint()
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
            with self._read_conn() as db:
                cur = db.execute(sql, params)
                return cur.fetchall()
        except Exception as exc:
            log.error("Query error: %s", exc)
            return []

    def incremental_vacuum(self, pages: int = 0) -> None:
        """Reclaim up to `pages` freed pages from the database file.
        Called after pruning to gradually shrink the file without downtime."""
        try:
            with self._write_conn() as db:
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
            with self._read_conn() as db:
                return db.execute("SELECT COUNT(*) FROM spots").fetchone()[0]
        except Exception:
            return 0

    def oldest_newest(self) -> tuple[int | None, int | None]:
        """Return (oldest_t, newest_t) for status reporting."""
        try:
            with self._read_conn() as db:
                row = db.execute("SELECT MIN(t), MAX(t) FROM spots").fetchone()
                return (row[0], row[1]) if row else (None, None)
        except Exception:
            return None, None
