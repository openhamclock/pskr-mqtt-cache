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
    flush_max_pause: int = 60
    filter_grid: str = ".{4,}"  #grids are minimum 4 characters
    filter_call: str = ".{3,}"  #calls are minimum 3 characters


@dataclass
class DatabaseConfig:
    path: str = "/var/lib/pskr-mqtt-cache/spots.db"
    max_age_hours: int = 7
    prune_interval_minutes: int = 15
    cache_size_mb: int = 64
    insert_lock_timeout: int = 10
    mmap_size_mb: int = 2048

@dataclass
class APIConfig:
    host: str = "0.0.0.0"
    port: int = 5000
    api_key: str = ""


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
            flush_max_pause=int(m.get("flush_max_pause", cfg.mqtt.flush_max_pause)),
            filter_grid=str(m.get("filter_grid", cfg.mqtt.filter_grid)),
            filter_call=str(m.get("filter_call", cfg.mqtt.filter_call)),            
        )

    if "database" in raw:
        d = raw["database"]
        cfg.database = DatabaseConfig(
            path=d.get("path", cfg.database.path),
            max_age_hours=int(d.get("max_age_hours", cfg.database.max_age_hours)),
            prune_interval_minutes=int(d.get("prune_interval_minutes", cfg.database.prune_interval_minutes)),
            cache_size_mb=int(d.get("cache_size_mb", cfg.database.cache_size_mb)),
            insert_lock_timeout=int(d.get("insert_lock_timeout", cfg.database.insert_lock_timeout)),
            mmap_size_mb=int(d.get("mmap_size_mb", cfg.database.mmap_size_mb)),
        )

    if "api" in raw:
        a = raw["api"]
        cfg.api = APIConfig(
            host=a.get("host", cfg.api.host),
            port=int(a.get("port", cfg.api.port)),
            api_key=str(a.get("api_key", cfg.api.api_key)),
        )

    if "logging" in raw:
        cfg.logging = LoggingConfig(
            level=raw["logging"].get("level", cfg.logging.level).upper()
        )

    return cfg
