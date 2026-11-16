"""SQLite persistence for the edge gateway.

Tables:
  upstreams  - registered upstream targets (seeded from env, upserted on boot)
  routes     - single-row table holding the effective route and its version
  instances  - one row per live gateway process, kept fresh by a heartbeat;
               this is the lease that tells parallel instances apart from
               dead ones
  inflight   - one row per admitted proxied request, owned by the admitting
               instance (instance_id). Rows are removed only by their owner
               (finish) or, for rows whose owner is provably gone (heartbeat
               lease expired / never registered), by any live instance's
               reaper. A starting instance never wipes rows owned by live
               peers, so a rolling restart keeps the shared drain view intact.
  probes     - audit log of candidate probes; a commit requires a fresh,
               successful probe recorded against the same route version
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

DB_PATH = Path(os.environ.get("GATEWAY_DB", "/data/gateway.db"))
PROBE_TTL_SECONDS = float(os.environ.get("PROBE_TTL_SECONDS", "60"))
# Instance liveness lease: a peer is only considered dead (and its in-flight
# rows reclaimable) once its heartbeat is older than the TTL. The TTL is a
# multiple of the heartbeat interval so a briefly stalled live instance is
# never mistaken for a dead one.
HEARTBEAT_INTERVAL_SECONDS = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "1.0"))
INSTANCE_LEASE_TTL_SECONDS = float(os.environ.get("INSTANCE_LEASE_TTL_SECONDS", "3.0"))
REAPER_INTERVAL_SECONDS = float(os.environ.get("REAPER_INTERVAL_SECONDS", "1.0"))

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None
_instance_id: str | None = None
_started_at: float = 0.0
_reclaimed_inflight = 0  # rows reclaimed by this instance since boot
_stop = threading.Event()
_bg_threads: list[threading.Thread] = []


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS upstreams (
            name TEXT PRIMARY KEY,
            url  TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS routes (
            id             INTEGER PRIMARY KEY CHECK (id = 1),
            current_target TEXT NOT NULL,
            version        INTEGER NOT NULL,
            updated_at     REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS instances (
            id             TEXT PRIMARY KEY,
            started_at     REAL NOT NULL,
            last_heartbeat REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS inflight (
            id          TEXT PRIMARY KEY,
            instance_id TEXT,
            target      TEXT NOT NULL,
            path        TEXT NOT NULL,
            started_at  REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS probes (
            id           TEXT PRIMARY KEY,
            target       TEXT NOT NULL,
            base_version INTEGER NOT NULL,
            ok           INTEGER NOT NULL,
            detail       TEXT,
            created_at   REAL NOT NULL
        );
        """
    )
    # Migration for databases created before instance ownership existed.
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(inflight)")}
    if "instance_id" not in cols:
        conn.execute("ALTER TABLE inflight ADD COLUMN instance_id TEXT")


def init_db(upstreams: dict[str, str], initial_target: str) -> dict:
    """Create tables, upsert the upstream registry, seed the route once,
    register this instance, and reclaim in-flight rows whose owner is gone.

    Rows owned by *live* peer instances (fresh heartbeat) are left untouched:
    a second gateway starting during a rolling restart must not erase the
    first gateway's in-flight requests from the shared drain view."""
    global _conn, _instance_id, _started_at
    if not upstreams:
        raise RuntimeError("no upstreams configured")
    if initial_target not in upstreams:
        raise RuntimeError(f"initial target {initial_target!r} not in upstreams")
    _conn = _connect()
    _instance_id = uuid.uuid4().hex
    _started_at = time.time()
    with _lock, _conn:
        _ensure_schema(_conn)
        for name, url in upstreams.items():
            _conn.execute(
                "INSERT INTO upstreams (name, url) VALUES (?, ?) "
                "ON CONFLICT(name) DO UPDATE SET url = excluded.url",
                (name, url),
            )
        # Parallel instances may boot concurrently; exactly one seeds the row.
        cur = _conn.execute(
            "INSERT OR IGNORE INTO routes (id, current_target, version, updated_at) "
            "VALUES (1, ?, 0, ?)",
            (initial_target, _started_at),
        )
        seeded = cur.rowcount == 1
        _heartbeat_now()
    cleared = reclaim_dead()
    _start_background()
    return {
        "instance_id": _instance_id,
        "seeded_route": seeded,
        "cleared_inflight": cleared,
    }


def _db() -> sqlite3.Connection:
    assert _conn is not None, "db not initialised"
    return _conn


def _heartbeat_now() -> None:
    """Upsert so the row is recreated even if a reaper removed it while this
    instance was briefly stalled past its lease."""
    _db().execute(
        "INSERT INTO instances (id, started_at, last_heartbeat) VALUES (?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET last_heartbeat = excluded.last_heartbeat",
        (_instance_id, _started_at, time.time()),
    )


def reclaim_dead() -> int:
    """Reclaim in-flight rows whose owning instance is gone: heartbeat lease
    expired, never registered, or pre-ownership legacy rows (NULL owner).

    The DELETE is scoped by owner and evaluated in one statement, so it
    interleaves safely with peers' admit()/finish(): rows of live instances
    never match, and a row already finished by its owner simply matches
    nothing. Reclaiming is deliberately conservative - a stalled-but-alive
    peer keeps its rows until the lease expires."""
    global _reclaimed_inflight
    cutoff = time.time() - INSTANCE_LEASE_TTL_SECONDS
    with _lock, _db() as conn:
        cur = conn.execute(
            "DELETE FROM inflight WHERE instance_id IS NULL OR instance_id NOT IN "
            "(SELECT id FROM instances WHERE last_heartbeat >= ?)",
            (cutoff,),
        )
        reclaimed = cur.rowcount
        conn.execute("DELETE FROM instances WHERE last_heartbeat < ?", (cutoff,))
    if reclaimed:
        _reclaimed_inflight += reclaimed
    return reclaimed


def _heartbeat_loop() -> None:
    while not _stop.wait(HEARTBEAT_INTERVAL_SECONDS):
        try:
            with _lock, _db() as conn:
                _heartbeat_now()
        except sqlite3.Error:
            pass  # retry next tick; the lease TTL covers transient failures


def _reaper_loop() -> None:
    while not _stop.wait(REAPER_INTERVAL_SECONDS):
        try:
            reclaim_dead()
        except sqlite3.Error:
            pass


def _start_background() -> None:
    for target in (_heartbeat_loop, _reaper_loop):
        t = threading.Thread(target=target, daemon=True)
        t.start()
        _bg_threads.append(t)


def shutdown() -> None:
    """Graceful exit: stop the background loops, then drop our own in-flight
    rows (this process can no longer complete them) and deregister. A killed
    instance never runs this; its rows are reclaimed by the survivors once
    its heartbeat lease expires."""
    _stop.set()
    for t in _bg_threads:
        t.join(timeout=2.0)
    if _conn is None or _instance_id is None:
        return
    try:
        with _lock, _db() as conn:
            conn.execute("DELETE FROM inflight WHERE instance_id = ?", (_instance_id,))
            conn.execute("DELETE FROM instances WHERE id = ?", (_instance_id,))
    except sqlite3.Error:
        pass


def get_route() -> dict:
    with _lock:
        row = _db().execute(
            "SELECT current_target, version, updated_at FROM routes WHERE id = 1"
        ).fetchone()
    return dict(row)


def upstream_url(name: str) -> str | None:
    with _lock:
        row = _db().execute(
            "SELECT url FROM upstreams WHERE name = ?", (name,)
        ).fetchone()
    return row["url"] if row else None


def admit(path: str) -> dict:
    """Atomically snapshot the effective route and register the request as
    in-flight under this instance's ownership. The request keeps this target
    even if a switch commits while it is running, so it always completes
    against the original upstream."""
    rid = uuid.uuid4().hex
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        conn.execute(
            "INSERT INTO inflight (id, instance_id, target, path, started_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (rid, _instance_id, route["current_target"], path, time.time()),
        )
    return {"id": rid, "target": route["current_target"], "version": route["version"]}


def finish(request_id: str) -> None:
    with _lock, _db() as conn:
        conn.execute("DELETE FROM inflight WHERE id = ?", (request_id,))


def record_probe(target: str, base_version: int, ok: bool, detail: dict) -> dict:
    """A probe never mutates the route; it only records an auditable result."""
    pid = uuid.uuid4().hex
    with _lock, _db() as conn:
        conn.execute(
            "INSERT INTO probes (id, target, base_version, ok, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (pid, target, base_version, 1 if ok else 0, json.dumps(detail), time.time()),
        )
    return {"id": pid}


def commit_switch(target: str, base_version: int, probe_id: str) -> dict:
    """Compare-and-swap the route on its version. A commit carrying a stale
    version (e.g. a late probe from an interleaved switch attempt) loses and
    cannot overwrite the already-effective route."""
    with _lock, _db() as conn:
        route = conn.execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        if route["version"] != base_version:
            return {
                "ok": False,
                "error": "version_conflict",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if route["current_target"] == target:
            return {
                "ok": False,
                "error": "target_is_current",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if upstream_url(target) is None:
            return {"ok": False, "error": "unknown_target"}
        probe = conn.execute(
            "SELECT * FROM probes WHERE id = ?", (probe_id,)
        ).fetchone()
        if (
            probe is None
            or not probe["ok"]
            or probe["target"] != target
            or probe["base_version"] != base_version
        ):
            return {"ok": False, "error": "probe_invalid"}
        if time.time() - probe["created_at"] > PROBE_TTL_SECONDS:
            return {"ok": False, "error": "probe_stale"}
        cur = conn.execute(
            "UPDATE routes SET current_target = ?, version = version + 1, updated_at = ? "
            "WHERE id = 1 AND version = ?",
            (target, time.time(), base_version),
        )
        if cur.rowcount != 1:  # guarded by _lock; belt and braces
            return {"ok": False, "error": "version_conflict"}
        return {"ok": True, "current_target": target, "version": base_version + 1}


def get_state() -> dict:
    with _lock:
        route = dict(_db().execute("SELECT * FROM routes WHERE id = 1").fetchone())
        upstreams = [
            dict(r) for r in _db().execute("SELECT name, url FROM upstreams ORDER BY name")
        ]
        inflight = [
            dict(r) for r in _db().execute("SELECT * FROM inflight ORDER BY started_at")
        ]
        instances = [
            dict(r)
            for r in _db().execute("SELECT * FROM instances ORDER BY started_at")
        ]
        probes = [
            dict(r)
            for r in _db().execute(
                "SELECT * FROM probes ORDER BY created_at DESC LIMIT 10"
            )
        ]
    now = time.time()
    counts: dict[str, int] = {}
    per_instance: dict[str, int] = {}
    for row in inflight:
        counts[row["target"]] = counts.get(row["target"], 0) + 1
        owner = row["instance_id"] or ""
        per_instance[owner] = per_instance.get(owner, 0) + 1
    for u in upstreams:
        u["inflight"] = counts.get(u["name"], 0)
        u["is_current"] = u["name"] == route["current_target"]
        # A non-current target may only be taken offline once its in-flight
        # requests - across ALL live instances sharing this database - have
        # fully drained.
        u["can_offline"] = (not u["is_current"]) and u["inflight"] == 0
    for row in inflight:
        row["age_ms"] = int((now - row["started_at"]) * 1000)
    for inst in instances:
        inst["alive"] = now - inst["last_heartbeat"] <= INSTANCE_LEASE_TTL_SECONDS
        inst["is_self"] = inst["id"] == _instance_id
        inst["inflight"] = per_instance.get(inst["id"], 0)
        inst["heartbeat_age_ms"] = int((now - inst["last_heartbeat"]) * 1000)
    for p in probes:
        p["detail"] = json.loads(p["detail"]) if p["detail"] else {}
    return {
        "now": now,
        "route": route,
        "upstreams": upstreams,
        "inflight": inflight,
        "instances": instances,
        "reclaimed_inflight": _reclaimed_inflight,
        "lease_ttl_seconds": INSTANCE_LEASE_TTL_SECONDS,
        "recent_probes": probes,
    }
