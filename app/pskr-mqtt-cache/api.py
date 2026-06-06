"""
api.py — FastAPI HTTP API for pskr-mqtt-cache.

Copyright (C) 2026 Open HamClock Backend (OHB) Contributors
License: GNU Affero General Public License v3.0 (AGPLv3)
See LICENSE file or <https://www.gnu.org/licenses/agpl-3.0.html>

Endpoints:

  GET /spots
    Query parameters:
      bygrid  — sender Maidenhead grid prefix (e.g. EL97 or EL97ab)
      ofgrid  — receiver Maidenhead grid prefix
      maxage  — seconds back from now (default 900, max 86400)
    Returns: plain text CSV, one spot per line, same format as HamClock expects:
      flowStartSeconds,senderLocator,senderCallsign,receiverLocator,receiverCallsign,mode,frequency,sNR

  GET /status
    Returns JSON with service health, DB stats, and MQTT connection state.

  GET /docs
    Auto-generated FastAPI interactive docs (Swagger UI).
"""

import re
import time
import logging
from typing import Optional

from fastapi import FastAPI, Query, HTTPException, Request, Depends
from fastapi.responses import PlainTextResponse, JSONResponse

from .config import APIConfig
from .database import SpotDatabase

log = logging.getLogger(__name__)

# Module-level references set by main.py after construction
_db: SpotDatabase | None = None
_cfg: APIConfig | None   = None
_subscriber              = None   # SpotSubscriber — imported lazily to avoid circular

# Cache for expensive DB stats
_cached_db_stats = {"total": 0, "oldest": None, "newest": None, "timestamp": 0}
_CACHE_TTL_SEC   = 5 # Cache for 5 seconds

GRID_RE = re.compile(r'^[A-Ra-r]{2}[0-9]{2}([A-Xa-x]{2})?$')

app = FastAPI(
    title="pskr-mqtt-cache",
    description="PSKReporter MQTT spot cache — serves HamClock-compatible spot queries from a local SQLite database.",
    version="1.0.0",
    license_info={"name": "AGPLv3", "url": "https://www.gnu.org/licenses/agpl-3.0.html"},
)


def get_db() -> SpotDatabase:
    if _db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return _db


def check_api_key(request: Request):
    """Optional API key check. Disabled if api_key is empty in config."""
    if not _cfg or not _cfg.api_key:
        return
    key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if key != _cfg.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def valid_grid(g: str) -> bool:
    return bool(GRID_RE.match(g))


CALL_RE = re.compile(r'^[A-Z0-9/]{3,15}$')

def valid_call(c: str) -> bool:
    return bool(CALL_RE.match(c))


@app.get(
    "/spots",
    response_class=PlainTextResponse,
    summary="Query spots by grid prefix, callsign, and age",
    description="""
Returns spots in HamClock wire format (CSV, one spot per line):

    flowStartSeconds,senderLocator,senderCallsign,receiverLocator,receiverCallsign,mode,frequency,sNR

At least one of `bygrid`, `ofgrid`, `bycall`, or `ofcall` must be provided.
    """,
)
@app.get("/ham/HamClock/fetchPSKReporter.pl", response_class=PlainTextResponse, include_in_schema=False)
def get_spots(
    request: Request,
    bygrid: Optional[str] = Query(default=None, description="Sender grid prefix (e.g. EL97 or EL97ab)"),
    ofgrid: Optional[str] = Query(default=None, description="Receiver grid prefix (e.g. EL97 or EL97ab)"),
    bycall: Optional[str] = Query(default=None, description="Sender callsign (exact match, e.g. W4BLD)"),
    ofcall: Optional[str] = Query(default=None, description="Receiver callsign (exact match, e.g. KO4AQF)"),
    maxage: int           = Query(default=900, ge=60, le=86400, description="Seconds back from now (60–86400)"),
    db: SpotDatabase      = Depends(get_db),
    _auth                 = Depends(check_api_key),
):
    if not bygrid and not ofgrid and not bycall and not ofcall:
        raise HTTPException(status_code=400, detail="At least one of bygrid, ofgrid, bycall, or ofcall is required")

    if bygrid and not valid_grid(bygrid):
        raise HTTPException(status_code=400, detail=f"Invalid bygrid locator: {bygrid}")

    if ofgrid and not valid_grid(ofgrid):
        raise HTTPException(status_code=400, detail=f"Invalid ofgrid locator: {ofgrid}")

    if bycall:
        bycall = bycall.upper()
        if not valid_call(bycall):
            raise HTTPException(status_code=400, detail=f"Invalid bycall: {bycall}")

    if ofcall:
        ofcall = ofcall.upper()
        if not valid_call(ofcall):
            raise HTTPException(status_code=400, detail=f"Invalid ofcall: {ofcall}")

    # Mapping: based on fetchPSKReporter.pl, 'by' is sender, 'of' is receiver.
    rows = db.query_spots(
        bygrid=bygrid or "",
        ofgrid=ofgrid or "",
        bycall=bycall or "",
        ofcall=ofcall or "",
        maxage=maxage,
    )
    
    # Optimization: Use positional indexing instead of column names.
    # sqlite3.Row access by name is slow in tight loops.
    # Order: t[0], s_grid[1], s_call[2], r_grid[3], r_call[4], mode[5], freq[6], snr[7]
    lines = []
    for r in rows:
        lines.append(f"{r[0]},{r[1]},{r[2][:10]},{r[3]},{r[4][:10]},{r[5]},{r[6]},{r[7]}")

    return PlainTextResponse("\n".join(lines) + "\n" if lines else "")


@app.get(
    "/status",
    summary="Service health and statistics",
)
def get_status(
    db: SpotDatabase = Depends(get_db),
    _auth            = Depends(check_api_key),
):
    global _cached_db_stats

    # Use cached stats if recent enough
    if (time.time() - _cached_db_stats["timestamp"]) > _CACHE_TTL_SEC:
        _cached_db_stats["total"]   = db.count()
        _cached_db_stats["oldest"], _cached_db_stats["newest"] = db.oldest_newest()
        _cached_db_stats["timestamp"] = time.time()

    total, oldest, newest = _cached_db_stats["total"], _cached_db_stats["oldest"], _cached_db_stats["newest"]

    status = {
        "status":       "ok",
        "db_spots":     total,
        "db_oldest_t":  oldest,
        "db_newest_t":  newest,
        "db_window_hours": round((newest - oldest) / 3600, 2) if oldest and newest else 0,
        "mqtt": {},
    }

    if _subscriber is not None:
        s = _subscriber.stats()
        status["mqtt"] = {
            "connected":      s["connected"],
            "spots_received": s["spots_received"],
            "spots_inserted": s["spots_inserted"],
            "last_spot_age_sec": round(time.time() - s["last_spot_time"], 1)
                                  if s["last_spot_time"] else None,
        }

    return JSONResponse(status)
