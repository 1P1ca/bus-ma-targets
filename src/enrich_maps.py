"""Stage 5: Google Maps enrichment.

For each operator:
  1. Geocode HQ address -> lat/lng, place_id, formatted_address.
  2. Fetch a Static Maps satellite tile (z=19, 640x640) centred on HQ.
  3. Run a cheap OpenCV / PIL heuristic on the tile to estimate paved
     (parking) area vs building roof area in m², and a rough bus-capacity
     figure (parking m² / 45 m² per bus stall).

The heuristic is intentionally cheap (±30%). For the top-N candidates we'll
call a vision agent separately — that lives in `agent_enrich.py`.

Idempotent: skips operators that already have lat/lng AND a facility row
with a non-null parking estimate.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from db import DB_PATH, connect, init_db

ROOT = Path(__file__).resolve().parent.parent
SAT_DIR = ROOT / "data" / "satellite"
SAT_DIR.mkdir(parents=True, exist_ok=True)


def _load_env() -> dict:
    env: dict[str, str] = {}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    # process env overrides file
    for k in list(env):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


ENV = _load_env()
GMAPS_KEY = ENV.get("GMAPS_API_KEY")


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

def _http_get_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "bus_ma-enrich/1.0"})
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def geocode(address: str, city: str = None, postal: str = None,
            province: str = "QC", country: str = "CA") -> Optional[dict]:
    """Return {lat, lng, place_id, formatted_address} or None."""
    if not GMAPS_KEY:
        raise RuntimeError("GMAPS_API_KEY missing in .env")
    parts = [p for p in (address, city, postal, province, country) if p]
    q = ", ".join(parts)
    url = "https://maps.googleapis.com/maps/api/geocode/json?" + urlencode({
        "address": q,
        "region": "ca",
        "components": "country:CA|administrative_area:QC",
        "key": GMAPS_KEY,
    })
    data = _http_get_json(url)
    if data.get("status") != "OK" or not data.get("results"):
        err = data.get("error_message") or data.get("status", "unknown")
        # bubble a concrete reason on the first call so config issues are visible
        if data.get("status") in {"REQUEST_DENIED", "OVER_QUERY_LIMIT",
                                  "INVALID_REQUEST"}:
            raise RuntimeError(
                f"Google Geocoding API error: {data.get('status')} — {err}"
            )
        return None
    r0 = data["results"][0]
    loc = r0["geometry"]["location"]
    return {
        "lat": loc["lat"],
        "lng": loc["lng"],
        "place_id": r0.get("place_id"),
        "formatted_address": r0.get("formatted_address"),
        "location_type": r0["geometry"].get("location_type"),
    }


# ---------------------------------------------------------------------------
# Static Maps tile
# ---------------------------------------------------------------------------

def fetch_satellite_tile(lat: float, lng: float, out_path: Path,
                         zoom: int = 19, size: str = "640x640",
                         scale: int = 2) -> Path:
    """Download a satellite Static Maps PNG. Returns the path written."""
    if not GMAPS_KEY:
        raise RuntimeError("GMAPS_API_KEY missing in .env")
    url = "https://maps.googleapis.com/maps/api/staticmap?" + urlencode({
        "center": f"{lat},{lng}",
        "zoom": zoom,
        "size": size,
        "scale": scale,
        "maptype": "satellite",
        "key": GMAPS_KEY,
    })
    req = Request(url, headers={"User-Agent": "bus_ma-enrich/1.0"})
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with urlopen(req, timeout=30) as resp, out_path.open("wb") as f:
        f.write(resp.read())
    return out_path


# ---------------------------------------------------------------------------
# Heuristic parking / roof estimate
# ---------------------------------------------------------------------------
#
# Strategy without OpenCV: rely on PIL + numpy.
#   - Convert tile -> HSV.
#   - Asphalt/paved: low saturation, low-to-mid value, neutral hue.
#   - Roofs: a mix of darker neutrals (flat commercial roofs) and bright
#     metal. Hard to separate from asphalt without context, so we report a
#     combined "impervious" area and flag confidence as low.
#   - Grass/trees: high-saturation green.
#
# At z=19 with scale=2, resolution at latitude 46° is ~0.15 m/pixel.
# 640*2 = 1280 px tile => ~190 m wide. Good for typical depot footprints.

METERS_PER_PIXEL_AT_LAT = lambda lat_deg, zoom=19, scale=2: (
    156543.03392 * __import__("math").cos(__import__("math").radians(lat_deg))
    / (2 ** zoom) / scale
)


def estimate_areas_from_tile(image_path: Path, lat: float,
                             zoom: int = 19, scale: int = 2) -> Optional[dict]:
    """Cheap PIL+numpy impervious-surface estimate. Returns dict or None."""
    try:
        from PIL import Image
        import numpy as np
    except ImportError:
        return None

    img = Image.open(image_path).convert("RGB")
    arr = np.asarray(img).astype("float32") / 255.0
    r, g, b = arr[..., 0], arr[..., 1], arr[..., 2]

    # HSV-ish channels
    mx = arr.max(axis=-1)
    mn = arr.min(axis=-1)
    v = mx
    s = np.where(mx == 0, 0, (mx - mn) / np.where(mx == 0, 1, mx))

    # vegetation: green dominance
    veg = (g > r + 0.05) & (g > b + 0.05) & (s > 0.15)
    # paved / roof (impervious): low saturation, mid brightness
    paved = (~veg) & (s < 0.2) & (v > 0.15) & (v < 0.9)
    # bright roofs (metal)
    bright_roof = (~veg) & (v > 0.85)

    mpp = METERS_PER_PIXEL_AT_LAT(lat, zoom=zoom, scale=scale)
    px_area_m2 = mpp * mpp

    impervious_px = int(paved.sum() + bright_roof.sum())
    impervious_m2 = impervious_px * px_area_m2
    bright_m2 = int(bright_roof.sum()) * px_area_m2
    paved_m2 = int(paved.sum()) * px_area_m2

    # Rough bus capacity: 45 m² per articulated bus stall (12m x 3.75m),
    # assume 60% of impervious area excluding bright roofs is parking.
    parking_m2_est = max(0.0, 0.60 * paved_m2)
    bus_capacity_est = int(parking_m2_est / 45)

    return {
        "parking_area_m2": round(parking_m2_est, 1),
        "building_area_m2": round(bright_m2, 1),
        "parcel_area_m2": round(impervious_m2 + (arr.shape[0] * arr.shape[1] * px_area_m2 * 0), 1),
        "bus_capacity_est": bus_capacity_est,
        "meters_per_pixel": round(mpp, 4),
        "confidence": 0.3,
        "estimation_method": "pil-hsv-heuristic",
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def enrich_one(conn: sqlite3.Connection, op: dict, zoom: int = 19,
               skip_consolidators: bool = True) -> dict:
    """Geocode + fetch tile + estimate, persist to DB. Returns summary."""
    summary = {"operator_id": op["id"], "name": op["name"], "ok": False}

    # Skip consolidator subsidiaries to save API calls (they're excluded
    # from Top-Targets anyway).
    if skip_consolidators and op.get("group_id"):
        g = conn.execute(
            "SELECT is_excluded FROM groups WHERE id = ?", (op["group_id"],)
        ).fetchone()
        if g and g["is_excluded"]:
            summary["skipped"] = "consolidator"
            return summary

    # 1. geocode if needed
    if op.get("lat") is None or op.get("lng") is None:
        g = geocode(op.get("address"), op.get("city"), op.get("postal"))
        if not g:
            summary["error"] = "geocode_failed"
            return summary
        conn.execute(
            "UPDATE operators SET lat = ?, lng = ?, place_id = ?, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (g["lat"], g["lng"], g.get("place_id"), op["id"]),
        )
        conn.commit()
        op["lat"], op["lng"], op["place_id"] = g["lat"], g["lng"], g.get("place_id")
        summary["geocoded"] = True

    # 2. fetch satellite tile if needed
    tile_path = SAT_DIR / f"op_{op['id']}_z{zoom}.png"
    if not tile_path.exists():
        fetch_satellite_tile(op["lat"], op["lng"], tile_path, zoom=zoom)
        summary["tile_fetched"] = True

    # 3. estimate areas
    existing = conn.execute(
        "SELECT id FROM facility WHERE operator_id = ? AND estimation_method = 'pil-hsv-heuristic'",
        (op["id"],),
    ).fetchone()
    if existing:
        summary["skipped_estimate"] = True
    else:
        est = estimate_areas_from_tile(tile_path, op["lat"], zoom=zoom)
        if est:
            conn.execute(
                "INSERT INTO facility (operator_id, parcel_area_m2, parking_area_m2, "
                "building_area_m2, bus_capacity_est, satellite_image_path, zoom, "
                "estimation_method, confidence) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (op["id"], est["parcel_area_m2"], est["parking_area_m2"],
                 est["building_area_m2"], est["bus_capacity_est"],
                 str(tile_path.relative_to(ROOT)), zoom,
                 est["estimation_method"], est["confidence"]),
            )
            conn.commit()
            summary["estimate"] = est

    summary["ok"] = True
    return summary


def run(limit: int = None, skip_consolidators: bool = True,
        db_path: Path = DB_PATH) -> list[dict]:
    init_db(db_path)
    conn = connect(db_path)
    try:
        ops = [dict(r) for r in conn.execute(
            "SELECT * FROM operators ORDER BY id"
        ).fetchall()]
        if limit:
            ops = ops[:limit]
        out = []
        for i, op in enumerate(ops, 1):
            try:
                s = enrich_one(conn, op, skip_consolidators=skip_consolidators)
            except Exception as e:
                s = {"operator_id": op["id"], "name": op["name"],
                     "ok": False, "error": f"{type(e).__name__}: {e}"}
            out.append(s)
            if i % 10 == 0:
                print(f"  [{i}/{len(ops)}] {op['name'][:50]} -> "
                      f"{'OK' if s.get('ok') else s.get('error') or s.get('skipped')}")
            # polite rate-limit for Google APIs
            time.sleep(0.05)
        return out
    finally:
        conn.close()


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else None
    results = run(limit=n)
    ok = sum(1 for r in results if r.get("ok"))
    skipped = sum(1 for r in results if r.get("skipped"))
    err = sum(1 for r in results if r.get("error"))
    print(f"\nDone: {ok} ok, {skipped} skipped, {err} errored")
