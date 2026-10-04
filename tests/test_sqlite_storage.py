"""Tests for :mod:`aiohttp_session.sqlite_storage`.

These tests never open a network socket: the session middleware is
exercised in-process against mocked :class:`aiohttp.web.Request`
objects and real :class:`aiohttp.web.Response` objects, while the
storage persists to a temporary SQLite file.  No Redis or Docker is
required.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from collections.abc import Awaitable, Callable
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web
from aiohttp.http import HttpVersion
from aiohttp.test_utils import make_mocked_request

from aiohttp_session import Session, get_session, session_middleware
from aiohttp_session.sqlite_storage import SQLiteStorage

COOKIE = "AIOHTTP_SESSION"
Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


# --------------------------------------------------------------------- #
# In-process request helpers (no sockets / test server needed)
# --------------------------------------------------------------------- #


def make_request(cookie: str | None = None) -> web.Request:
    headers: Any = None
    if cookie is not None:
        headers = {"Cookie": f"{COOKIE}={cookie}"}
    return make_mocked_request(
        "GET",
        "/",
        headers=headers,
        version=HttpVersion(major=1, minor=1),
    )


async def invoke(
    storage: SQLiteStorage, handler: Handler, cookie: str | None = None
) -> tuple[web.Request, web.StreamResponse]:
    """Run one request through the real session middleware."""
    request = make_request(cookie)
    middleware = session_middleware(storage)
    response = await middleware(request, handler)
    return request, response


def response_cookie(response: web.StreamResponse) -> SimpleCookie[str]:
    return response.cookies  # type: ignore[attr-defined]


def set_session(request: web.Request) -> Session:
    return request[  # type: ignore[no-any-return]
        "aiohttp_session"
    ]


def connect(db_path: Path | str) -> sqlite3.Connection:
    return sqlite3.connect(str(db_path))


def table_names(db_path: Path | str) -> set[str]:
    conn = connect(db_path)
    try:
        return {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()


def read_row(
    db_path: Path | str, key: str
) -> tuple[str, str, int | None] | None:
    conn = connect(db_path)
    try:
        return conn.execute(
            "SELECT session_key, data, expires_at FROM aiohttp_sessions "
            "WHERE session_key = ?",
            (key,),
        ).fetchone()
    finally:
        conn.close()


def seed_session(
    db_path: Path | str,
    key: str,
    data: dict[str, Any] | None,
    *,
    expires_at: int | None,
    raw: str | None = None,
) -> None:
    if raw is not None:
        payload = raw
    elif data is None:
        payload = json.dumps({})
    else:
        payload = json.dumps({"created": int(time.time()), "session": data})
    conn = connect(db_path)
    try:
        conn.execute(
            "INSERT INTO aiohttp_sessions (session_key, data, expires_at) "
            "VALUES (?, ?, ?)",
            (key, payload, expires_at),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "sessions.db"


@pytest.fixture
async def storage(db_path: Path):
    store = SQLiteStorage(db_path)
    await store._start()
    yield store
    await store.close()


# --------------------------------------------------------------------- #
# Initialization / schema
# --------------------------------------------------------------------- #


async def test_schema_is_created_lazily(db_path: Path) -> None:
    store = SQLiteStorage(db_path)
    await store._start()
    assert db_path.exists()
    assert table_names(db_path) == {"aiohttp_sessions"}
    await store.close()


async def test_two_instances_sharing_file_initialize_concurrently(
    db_path: Path,
) -> None:
    store_a = SQLiteStorage(db_path)
    store_b = SQLiteStorage(db_path)
    await asyncio.gather(store_a._start(), store_b._start())
    assert table_names(db_path) == {"aiohttp_sessions"}
    await store_b.close()
    await store_a.close()


async def test_init_error_is_surfaced_and_close_does_not_leak(
    tmp_path: Path,
) -> None:
    bad_path = tmp_path / "missing-dir" / "sessions.db"
    store = SQLiteStorage(bad_path)
    with pytest.raises(sqlite3.OperationalError):
        await store._start()
    with pytest.raises(sqlite3.OperationalError):
        await store.cleanup_expired()
    await store.close()  # never raises, worker is stopped
    assert store._worker is not None and store._worker.closed


async def test_construction_before_event_loop_runs(tmp_path: Path) -> None:
    # No running loop here at construction time; startup is lazy.
    store = SQLiteStorage(tmp_path / "late.db")
    assert await store.cleanup_expired() == 0
    await store.close()


# --------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------- #


async def test_close_is_idempotent(storage: SQLiteStorage) -> None:
    await storage.close()
    await storage.close()


async def test_operations_fail_after_close(storage: SQLiteStorage) -> None:
    # The fixture has started the worker; closing must then reject use.
    await storage.close()
    with pytest.raises(RuntimeError):
        await storage.cleanup_expired()
    with pytest.raises(RuntimeError):
        await storage._run(lambda: None)


async def test_close_without_startup_is_a_noop(db_path: Path) -> None:
    store = SQLiteStorage(db_path)
    await store.close()
    assert store._worker is None


async def test_context_manager_closes_storage(db_path: Path) -> None:
    async with SQLiteStorage(db_path) as store:
        assert await store.cleanup_expired() == 0
    assert store._worker is not None and store._worker.closed
    with pytest.raises(RuntimeError):
        await store.cleanup_expired()


async def test_work_runs_on_dedicated_worker_thread(
    storage: SQLiteStorage,
) -> None:
    loop_thread = threading.get_ident()
    seen: set[int] = set()

    def record() -> int:
        seen.add(threading.get_ident())
        return 0

    await storage._run(record)
    await storage._run(record)
    assert seen == {storage._worker_ident}
    assert loop_thread not in seen


# --------------------------------------------------------------------- #
# Loading sessions
# --------------------------------------------------------------------- #


async def test_new_session_when_no_cookie(storage: SQLiteStorage) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        assert isinstance(session, Session)
        assert session.new
        assert session.identity is None
        return web.Response(body=b"OK")

    _request, response = await invoke(storage, handler)
    assert response.status == 200
    assert COOKIE not in response_cookie(response)


async def test_loads_existing_session(
    storage: SQLiteStorage, db_path: Path
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        assert not session.new
        assert session.identity == "known-key"
        assert dict(session) == {"a": 1, "b": 12}
        return web.Response(body=b"OK")

    seed_session(db_path, "known-key", {"a": 1, "b": 12}, expires_at=None)
    _request, response = await invoke(storage, handler, cookie="known-key")
    assert response.status == 200


async def test_unknown_identity_is_treated_as_new(
    storage: SQLiteStorage,
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        assert session.new
        assert session.identity is None
        return web.Response(body=b"OK")

    _request, response = await invoke(storage, handler, cookie="missing-key")
    assert response.status == 200
    assert COOKIE not in response_cookie(response)


async def test_expired_identity_is_treated_as_new(
    storage: SQLiteStorage, db_path: Path
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        assert session.new
        assert session.identity is None
        return web.Response(body=b"OK")

    seed_session(db_path, "stale-key", {"a": 1}, expires_at=int(time.time()) - 1)
    _request, response = await invoke(storage, handler, cookie="stale-key")
    assert response.status == 200
    assert COOKIE not in response_cookie(response)
    # A plain load does not delete the expired row.
    assert read_row(db_path, "stale-key") is not None


async def test_unparsable_payload_is_empty_existing_session(
    storage: SQLiteStorage, db_path: Path
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        assert not session.new
        assert session.identity == "bad-key"
        assert dict(session) == {}
        return web.Response(body=b"OK")

    seed_session(db_path, "bad-key", None, expires_at=None, raw="not-json")
    _request, response = await invoke(storage, handler, cookie="bad-key")
    assert response.status == 200


# --------------------------------------------------------------------- #
# Saving / cookies
# --------------------------------------------------------------------- #


async def test_new_session_carries_identity_cookie_and_persists_row(
    storage: SQLiteStorage, db_path: Path
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["a"] = 1
        session["b"] = 2
        return web.Response(body=b"OK")

    _request, response = await invoke(storage, handler)
    assert response.status == 200

    morsel = response_cookie(response)[COOKIE]
    key = morsel.value
    assert key
    row = read_row(db_path, key)
    assert row is not None
    stored_key, data_str, expires_at = row
    assert stored_key == key
    assert json.loads(data_str)["session"] == {"a": 1, "b": 2}
    assert expires_at is None  # storage-level max_age unset -> no expiry
    assert morsel["httponly"]
    assert morsel["path"] == "/"


async def test_two_new_sessions_get_distinct_secure_identities(
    storage: SQLiteStorage,
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        (await get_session(request))["x"] = 1
        return web.Response()

    _r, resp_a = await invoke(storage, handler)
    _r, resp_b = await invoke(storage, handler)
    key_a = response_cookie(resp_a)[COOKIE].value
    key_b = response_cookie(resp_b)[COOKIE].value
    assert key_a != key_b
    assert len(key_a) >= 32 and len(key_b) >= 32


async def test_absolute_expiration_is_stored_with_max_age(
    db_path: Path,
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        (await get_session(request))["a"] = 1
        return web.Response()

    store = SQLiteStorage(db_path, max_age=10)
    try:
        before = int(time.time())
        _request, response = await invoke(store, handler)
        after = int(time.time())
    finally:
        await store.close()

    key = response_cookie(response)[COOKIE].value
    row = read_row(db_path, key)
    assert row is not None
    expires_at = row[2]
    assert expires_at is not None
    assert before + 10 <= expires_at <= after + 10
    assert response_cookie(response)[COOKIE]["max-age"] == "10"


async def test_session_level_max_age_overrides_storage_default(
    db_path: Path,
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session.max_age = 7
        session["a"] = 1
        return web.Response()

    store = SQLiteStorage(db_path, max_age=1000)
    try:
        before = int(time.time())
        _request, response = await invoke(store, handler)
        after = int(time.time())
    finally:
        await store.close()

    key = response_cookie(response)[COOKIE].value
    expires_at = read_row(db_path, key)[2]  # type: ignore[index]
    assert expires_at is not None
    assert before + 7 <= expires_at <= after + 7


async def test_invalidation_clears_cookie_and_row(
    storage: SQLiteStorage, db_path: Path
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session.invalidate()
        return web.Response(body=b"OK")

    seed_session(db_path, "known-key", {"a": 1}, expires_at=None)
    _request, response = await invoke(storage, handler, cookie="known-key")
    assert response.status == 200
    morsel = response_cookie(response)[COOKIE]
    assert morsel.value == ""
    assert morsel["expires"] == "Thu, 01 Jan 1970 00:00:00 GMT"
    assert morsel["max-age"] == "0"
    assert read_row(db_path, "known-key") is None


async def test_custom_key_factory_is_used(db_path: Path) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        (await get_session(request))["key"] = "value"
        return web.Response()

    store = SQLiteStorage(db_path, key_factory=lambda: "fixed-key")
    try:
        _request, response = await invoke(store, handler)
    finally:
        await store.close()

    assert response_cookie(response)[COOKIE].value == "fixed-key"
    row = read_row(db_path, "fixed-key")
    assert row is not None
    assert json.loads(row[1])["session"] == {"key": "value"}


async def test_session_fixation_rotates_identity_after_logout(
    storage: SQLiteStorage, db_path: Path
) -> None:
    async def login(request: web.Request) -> web.StreamResponse:
        (await get_session(request))["k"] = "v"
        return web.Response()

    async def logout(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session.invalidate()
        return web.Response()

    _request, resp_login = await invoke(storage, login)
    evil = response_cookie(resp_login)[COOKIE].value
    _request, resp_logout = await invoke(storage, logout, cookie=evil)
    assert response_cookie(resp_logout)[COOKIE].value == ""
    # The attacker-fixed identity is gone server-side, so presenting it
    # again mints a brand new session identity.
    assert read_row(db_path, evil) is None
    _request, resp_again = await invoke(storage, login, cookie=evil)
    new_key = response_cookie(resp_again)[COOKIE].value
    assert new_key != evil


# --------------------------------------------------------------------- #
# Persistence across storage instances (the file is the shared state)
# --------------------------------------------------------------------- #


async def test_session_persists_between_two_storage_instances(
    db_path: Path,
) -> None:
    async def write_handler(request: web.Request) -> web.StreamResponse:
        (await get_session(request))["a"] = 1
        return web.Response()

    async def read_handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        assert dict(session) == {"a": 1}
        return web.Response(body=b"OK")

    store_a = SQLiteStorage(db_path)
    _request, response = await invoke(store_a, write_handler)
    key = response_cookie(response)[COOKIE].value
    await store_a.close()

    store_b = SQLiteStorage(db_path)
    try:
        request_b, response_b = await invoke(
            store_b, read_handler, cookie=key
        )
        assert response_b.status == 200
        assert not set_session(request_b).new
    finally:
        await store_b.close()


# --------------------------------------------------------------------- #
# Bounded expiry cleanup
# --------------------------------------------------------------------- #


async def _stored_keys(storage: SQLiteStorage) -> set[str]:
    return await storage._run(
        lambda: {
            row[0]
            for row in storage._conn().execute(
                "SELECT session_key FROM aiohttp_sessions"
            )
        }
    )


async def test_cleanup_removes_only_expired_rows(
    storage: SQLiteStorage,
) -> None:
    now = int(time.time())
    await storage._run(storage._upsert_row, "exp1", "{}", now - 10)
    await storage._run(storage._upsert_row, "exp2", "{}", now - 1)
    await storage._run(storage._upsert_row, "fresh", "{}", now + 100)
    await storage._run(storage._upsert_row, "eternal", "{}", None)

    deleted = await storage.cleanup_expired()
    assert deleted == 2
    assert await _stored_keys(storage) == {"fresh", "eternal"}
    assert await storage.cleanup_expired() == 0


async def test_cleanup_is_batched_by_limit(storage: SQLiteStorage) -> None:
    now = int(time.time())
    for i in range(5):
        await storage._run(storage._upsert_row, f"exp{i}", "{}", now - 1)

    assert await storage.cleanup_expired(limit=2) == 2
    assert await storage.cleanup_expired(limit=2) == 2
    assert await storage.cleanup_expired(limit=2) == 1
    assert await storage.cleanup_expired(limit=2) == 0


@pytest.mark.parametrize("bad_limit", [0, -1, True, "10", 1.5])
async def test_cleanup_rejects_invalid_limit(
    storage: SQLiteStorage, bad_limit: Any
) -> None:
    with pytest.raises(ValueError):
        await storage.cleanup_expired(bad_limit)  # type: ignore[arg-type]


async def test_refreshed_row_is_not_deleted_by_stale_cleanup(
    db_path: Path,
) -> None:
    """A refresh committed before cleanup deletes must survive.

    The cleanup statement is paused inside its snapshot scan with a SQL
    function. A second connection refreshes the row (new, future
    expiration) before the delete executes. SQLite serializes the two
    writers, so the committed new version remains.
    """
    init_conn = connect(db_path)
    try:
        init_conn.executescript(
            """
            CREATE TABLE aiohttp_sessions (
                session_key TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                expires_at INTEGER
            );
            CREATE INDEX idx_aiohttp_sessions_expires_at
                ON aiohttp_sessions (expires_at);
            """
        )
        init_conn.execute(
            "INSERT INTO aiohttp_sessions VALUES ('r1', ?, 0)",
            (json.dumps({"created": 1, "session": {"v": "old"}}),),
        )
        init_conn.commit()
    finally:
        init_conn.close()

    started = threading.Event()
    release = threading.Event()
    outcome: dict[str, Any] = {}

    def stale_cleanup() -> None:
        conn = connect(db_path)
        conn.execute("PRAGMA busy_timeout = 30000")

        def gate() -> int:
            started.set()
            # Hold the stale snapshot scan briefly without blocking the
            # test for the full busy timeout: let the refresh commit,
            # then resume so the delete is re-evaluated.
            release.wait(2)
            return 1

        conn.create_function("gate", 0, gate)
        try:
            conn.execute(
                "DELETE FROM aiohttp_sessions WHERE rowid IN ("
                "  SELECT rowid FROM aiohttp_sessions "
                "  WHERE expires_at IS NOT NULL AND expires_at <= ? "
                "    AND gate() = 1"
                ")",
                (int(time.time()),),
            )
            conn.commit()
        except Exception as exc:  # pragma: no cover - asserted below
            outcome["error"] = exc
        finally:
            conn.close()

    thread = threading.Thread(target=stale_cleanup)
    thread.start()
    assert started.wait(10)

    refresher = sqlite3.connect(str(db_path), timeout=30)
    try:
        refresher.execute(
            "INSERT INTO aiohttp_sessions VALUES ('r1', ?, ?) "
            "ON CONFLICT(session_key) DO UPDATE SET "
            "data = excluded.data, expires_at = excluded.expires_at",
            (
                json.dumps({"created": 2, "session": {"v": "new"}}),
                int(time.time()) + 1000,
            ),
        )
        refresher.commit()
    finally:
        refresher.close()

    release.set()
    thread.join(10)
    assert not thread.is_alive()
    assert "error" not in outcome, outcome["error"]

    final = read_row(db_path, "r1")
    assert final is not None
    assert json.loads(final[1])["session"] == {"v": "new"}
    assert final[2] is not None and final[2] > time.time()


# --------------------------------------------------------------------- #
# Concurrent writers on the shared file
# --------------------------------------------------------------------- #


async def test_concurrent_writers_from_two_storages_do_not_lose_rows(
    db_path: Path,
) -> None:
    store_a = SQLiteStorage(db_path, timeout=10)
    store_b = SQLiteStorage(db_path, timeout=10)
    try:
        await asyncio.gather(
            *[
                store_a._run(store_a._upsert_row, f"a{i}", "{}", None)
                for i in range(25)
            ],
            *[
                store_b._run(store_b._upsert_row, f"b{i}", "{}", None)
                for i in range(25)
            ],
            store_a.cleanup_expired(10),
            store_b.cleanup_expired(10),
        )
        assert await _stored_keys(store_a) == {
            f"a{i}" for i in range(25)
        } | {f"b{i}" for i in range(25)}
    finally:
        await store_b.close()
        await store_a.close()


# --------------------------------------------------------------------- #
# Failure boundary: cookie vs. database write
# --------------------------------------------------------------------- #


async def test_db_write_failure_propagates_but_cookie_was_set(
    db_path: Path,
) -> None:
    class FailingStorage(SQLiteStorage):
        def _upsert_row(self, key: str, data: str, expires_at: int | None) -> None:
            raise sqlite3.OperationalError("simulated write failure")

    store = FailingStorage(db_path)
    await store._start()
    response = web.Response()
    session = Session(None, data=None, new=True, max_age=None)
    session["a"] = 1

    with pytest.raises(sqlite3.OperationalError):
        await store.save_session(
            None,  # type: ignore[arg-type]
            response,
            session,
        )

    # The cookie is set before the (failing) database write...
    assert response_cookie(response)[COOKIE].value
    # ...but nothing was persisted for that identity.
    assert read_row(db_path, response_cookie(response)[COOKIE].value) is None
    await store.close()
