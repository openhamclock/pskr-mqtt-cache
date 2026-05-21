"""
subscriber.py — MQTT subscriber for pskr-mqtt-cache.

Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>

Connects to mqtt.pskreporter.info, subscribes to the full spot firehose,
parses JSON payloads, and inserts spots into the SQLite database.

Spots are accumulated in an in-memory batch and flushed to SQLite every
FLUSH_INTERVAL seconds or when the batch reaches FLUSH_SIZE spots —
whichever comes first. This dramatically reduces disk IO compared to
committing every spot individually.

Runs in its own thread. Reconnects automatically on disconnect.
"""

import orjson
import time
import logging
import threading

import uuid
import paho.mqtt.client as mqtt

from .config import MQTTConfig
from .database import SpotDatabase

log = logging.getLogger(__name__)

FLUSH_INTERVAL = 15    # seconds between batch flushes
FLUSH_SIZE     = 5000  # flush early if batch reaches this size
MAX_PAUSE_SECONDS = 30 # Maximum time to pause flush (seconds)


class SpotSubscriber:
    def __init__(self, cfg: MQTTConfig, db: SpotDatabase):
        self.cfg = cfg
        self.db  = db

        self._connected   = False
        self._running     = False
        self._thread      = None

        # In-memory batch — MQTT callback appends here, flush thread drains it
        self._batch       = []
        self._batch_lock  = threading.Lock()
        self._flush_lock  = threading.Lock()
        self._flush_thread = None
        self._stop_event  = threading.Event()

        # Pruner coordination — set by Pruner to pause flush during prune
        self._paused_for_prune = threading.Event()
        self._pause_start = None

        self._client      = None

        # Stats
        self.spots_received  = 0
        self.spots_inserted  = 0
        self.last_spot_time  = None
        self.connect_time    = None

    # ── MQTT Callbacks ────────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self._connected   = True
            self.connect_time = time.time()
            log.info("Connected to MQTT broker %s:%d", self.cfg.host, self.cfg.port)
            client.subscribe(self.cfg.topic)
            log.info("Subscribed to topic: %s", self.cfg.topic)
        else:
            log.error("MQTT connect failed, rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc, properties=None, reasoncode=None):
        self._connected = False
        rc_val = int(rc) if rc is not None and isinstance(rc, int) else 0
        if rc_val != 0 or rc is None:
            log.warning("MQTT disconnected unexpectedly — will reconnect. (rc=%s)", rc)
        else:
            log.info("MQTT disconnected cleanly.")

    def _on_message(self, client, userdata, msg):
        try:
            spot = orjson.loads(msg.payload)
        except (orjson.JSONDecodeError, UnicodeDecodeError) as exc:
            log.debug("Bad payload: %s", exc)
            return

        self.spots_received += 1
        self.last_spot_time  = time.time()

        # Skip spots missing both grids — HamClock requires at least one
        if not spot.get("sl") and not spot.get("rl"):
            return

        # Append to batch — lock is brief (list append is O(1))
        with self._batch_lock:
            self._batch.append(spot)
            batch_size = len(self._batch)

        # Flush early if batch is large enough
        if batch_size >= FLUSH_SIZE and not self._paused_for_prune.is_set():
            self._flush()


        # Periodic stats log
        if self.spots_received % 10000 == 0:
            log.info("Stats: received=%d inserted=%d",
                     self.spots_received, self.spots_inserted)

    # ── Batch Flush ───────────────────────────────────────────────────────────

    def _flush(self):
        """Drain the batch and write to SQLite."""
        # The flush_lock ensures that the timer thread and the buffer-full
        # logic don't attempt to write to the database simultaneously.
        with self._flush_lock:
            with self._batch_lock:
                if not self._batch:
                    return
                batch = self._batch
                self._batch = []

            inserted = self.db.insert_batch(batch)
            self.spots_inserted += inserted

    def _flush_loop(self):
        while self._running:
            if self._stop_event.wait(FLUSH_INTERVAL):
                break
            if self._running:
                # Auto-resume if pruner has been holding pause too long
                if self._paused_for_prune.is_set():
                    pause_duration = time.time() - (self._pause_start or time.time())
                    if pause_duration > MAX_PAUSE_SECONDS:
                        log.warning("Pruner pause timeout (%ds) — forcing flush resume",
                                    int(pause_duration))
                        self._paused_for_prune.clear()
                        self._pause_start = None
                if not self._paused_for_prune.is_set():
                    self._flush()  
        

    # ── Pruner Coordination ───────────────────────────────────────────────────

    def pause_for_prune(self):
        """Signal flush thread to pause while pruner acquires write lock."""
        self._paused_for_prune.set()
        self._pause_start = time.time()    # record when pause started
        log.info("Flush thread paused for pruner.")

    def resume_after_prune(self):
        """Resume flush thread after pruner completes."""
        self._paused_for_prune.clear()
        self._pause_start = None           # clear the timer
        log.info("Flush thread resumed after pruner.")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def _run(self):
        """Main subscriber loop — runs in its own thread."""
        while self._running:
            client_id = f"{self.cfg.client_id}-{uuid.uuid4().hex[:8]}"
            client = mqtt.Client(
                client_id=client_id,
                protocol=mqtt.MQTTv311,
            )
            client.on_connect    = self._on_connect
            client.on_disconnect = self._on_disconnect
            client.on_message    = self._on_message

            if self.cfg.tls:
                client.tls_set()

            self._client = client
            try:
                log.info("Connecting to %s:%d …", self.cfg.host, self.cfg.port)
                client.connect(self.cfg.host, self.cfg.port, self.cfg.keepalive)
                client.loop_forever()
            except Exception as exc:
                log.error("MQTT error: %s", exc)

            if self._running:
                log.info("Reconnecting in %ds …", self.cfg.reconnect_delay)
                if self._stop_event.wait(self.cfg.reconnect_delay):
                    break

        log.info("Subscriber stopped.")

    def start(self):
        """Start the subscriber and flush threads."""
        self._running = True

        self._flush_thread = threading.Thread(
            target=self._flush_loop, name="batch-flush", daemon=True)
        self._flush_thread.start()

        self._thread = threading.Thread(
            target=self._run, name="mqtt-subscriber", daemon=True)
        self._thread.start()

        log.info("MQTT subscriber thread started.")

    def stop(self):
        """Signal threads to stop."""
        self._running = False
        self._stop_event.set()
        if self._client:
            try:
                self._client.disconnect()
            except Exception:
                pass
        log.info("MQTT subscriber stopping …")

    @property
    def is_connected(self) -> bool:
        return self._connected

    def stats(self) -> dict:
        return {
            "connected":       self._connected,
            "connect_time":    self.connect_time,
            "spots_received":  self.spots_received,
            "spots_inserted":  self.spots_inserted,
            "last_spot_time":  self.last_spot_time,
        }
