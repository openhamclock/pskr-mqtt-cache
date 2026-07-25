"""
config.py — YAML configuration loader for pskr-mqtt-cache.

Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>

"""

import sys
import logging
from pathlib import Path
from dataclasses import dataclass, field

import yaml

log = logging.getLogger(__name__)


@dataclass
class MQTTConfig:
    host: str = "mqtt.pskreporter.info"
    port: int = 1883
    tls: bool = False
    topic: str = "pskr/filter/v2/#"
    client_id: str = "pskr-mqtt-cache"
    keepalive: int = 60
    reconnect_delay: int = 5


@dataclass
class DatabaseConfig:
    path: str = "/var/lib/pskr-mqtt-cache/spots.db"
    max_age_hours: int = 7
    prune_interval_minutes: int = 15
    cache_size_mb: int = 64
    # cache_size and mmap_size are PER SQLITE CONNECTION. Total reservation is
    # (read_pool_size + 1) * each. The old code opened an unbounded number of
    # connections (one per thread, forever), so a 2GB mmap_size meant VIRT grew
    # by 2GB for every new API thread. Keep the pool small and mmap modest.
    read_pool_size: int = 6
    mmap_size_mb: int = 256
    # incremental_vacuum() is real disk I/O (page shuffling) on top of the
    # prune's own batched deletes + WAL checkpoint. Running it on every
    # single prune cycle is what produces the periodic CPU/IRQ sawtooth on
    # a worldwide-scoped cache. Only vacuum every Nth prune cycle instead —
    # the freelist just gets reused between vacuums, so this costs nothing
    # correctness-wise, only delays how quickly disk space is reclaimed.
    vacuum_every_n_prunes: int = 4
    # Checkpoint the WAL on this short, independent cadence (seconds) so
    # it never has 10-15 minutes to accumulate a large backlog between
    # prune cycles. See checkpointer.py.
    checkpoint_interval_seconds: int = 60


@dataclass
class APIConfig:
    host: str = "0.0.0.0"
    port: int = 5000
    api_key: str = ""
    # Upper bound on Starlette's sync-endpoint thread pool. Left at the
    # framework default, a burst of concurrent /spots calls spawns threads
    # without meaningful limit — and each new thread used to open its own
    # SQLite connection that was never released.
    thread_pool_limit: int = 16
    # Seconds to cache a /spots response for an identical query. The MQTT
    # subscriber only flushes to SQLite every FLUSH_INTERVAL (15s), so a TTL
    # at or below that costs no freshness at all.
    spots_cache_ttl_seconds: int = 10
    # Max distinct query keys held in the /spots cache (LRU eviction).
    spots_cache_max_entries: int = 2048


@dataclass
class LoggingConfig:
    level: str = "INFO"


@dataclass
class Config:
    mqtt: MQTTConfig = field(default_factory=MQTTConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    api: APIConfig = field(default_factory=APIConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def load(path: str | None = None) -> Config:
    """
    Load config from YAML file. Falls back to defaults if file not found.
    Looks for config.yaml in the current directory if no path given.
    """
    search_paths = [path] if path else [
        "config.yaml",
        "/etc/pskr-mqtt-cache/config.yaml",
    ]

    raw = {}
    for p in search_paths:
        if p and Path(p).exists():
            log.info("Loading config from %s", p)
            with open(p) as fh:
                raw = yaml.safe_load(fh) or {}
            break
    else:
        log.warning("No config.yaml found — using defaults.")

    cfg = Config()

    if "mqtt" in raw:
        m = raw["mqtt"]
        cfg.mqtt = MQTTConfig(
            host=m.get("host", cfg.mqtt.host),
            port=int(m.get("port", cfg.mqtt.port)),
            tls=bool(m.get("tls", cfg.mqtt.tls)),
            topic=m.get("topic", cfg.mqtt.topic),
            client_id=m.get("client_id", cfg.mqtt.client_id),
            keepalive=int(m.get("keepalive", cfg.mqtt.keepalive)),
            reconnect_delay=int(m.get("reconnect_delay", cfg.mqtt.reconnect_delay)),
        )

    if "database" in raw:
        d = raw["database"]
        cfg.database = DatabaseConfig(
            path=d.get("path", cfg.database.path),
            max_age_hours=int(d.get("max_age_hours", cfg.database.max_age_hours)),
            prune_interval_minutes=int(d.get("prune_interval_minutes", cfg.database.prune_interval_minutes)),
            cache_size_mb=int(d.get("cache_size_mb", cfg.database.cache_size_mb)),
            read_pool_size=int(d.get("read_pool_size", cfg.database.read_pool_size)),
            mmap_size_mb=int(d.get("mmap_size_mb", cfg.database.mmap_size_mb)),
            vacuum_every_n_prunes=int(d.get("vacuum_every_n_prunes", cfg.database.vacuum_every_n_prunes)),
            checkpoint_interval_seconds=int(d.get("checkpoint_interval_seconds", cfg.database.checkpoint_interval_seconds)),
        )

    if "api" in raw:
        a = raw["api"]
        cfg.api = APIConfig(
            host=a.get("host", cfg.api.host),
            port=int(a.get("port", cfg.api.port)),
            api_key=str(a.get("api_key", cfg.api.api_key)),
            thread_pool_limit=int(a.get("thread_pool_limit", cfg.api.thread_pool_limit)),
            spots_cache_ttl_seconds=int(a.get("spots_cache_ttl_seconds", cfg.api.spots_cache_ttl_seconds)),
            spots_cache_max_entries=int(a.get("spots_cache_max_entries", cfg.api.spots_cache_max_entries)),
        )

    if "logging" in raw:
        cfg.logging = LoggingConfig(
            level=raw["logging"].get("level", cfg.logging.level).upper()
        )

    return cfg
