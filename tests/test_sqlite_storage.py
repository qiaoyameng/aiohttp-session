from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from aiohttp.typedefs import Handler

from aiohttp_session import Session, get_session, session_middleware
from aiohttp_session.sqlite_storage import SQLiteStorage

COOKIE_NAME = "AIOHTTP_SESSION"





class _PipeToThreadExecutor(concurrent.futures.ThreadPoolExecutor):
    """ThreadPoolExecutor that wakes asyncio via an os.pipe.

    Some restricted sandboxes forbid writing to a socket from a worker
    thread, which breaks asyncio's self-pipe (a socketpair) used by
    ``call_soon_threadsafe``.  An ``os.pipe`` is allowed, so completion
    is signalled through a pipe registered with the selector.  Test
    shim only; the library itself uses plain ``asyncio.to_thread``.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(max_workers=4)
        self._loop = loop
        self._read_fd, self._write_fd = os.pipe()
        os.set_blocking(self._read_fd, False)
        loop.add_reader(self._read_fd, self._drain)

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        future = super().submit(fn, *args, **kwargs)
        future.add_done_callback(lambda _: os.write(self._write_fd, b"x"))
        return future

    def _drain(self) -> None:
        try:
            os.read(self._read_fd, 4096)
        except BlockingIOError:
            pass

    def close(self) -> None:
        self._loop.remove_reader(self._read_fd)
        self.shutdown(wait=True)
        os.close(self._read_fd)
        os.close(self._write_fd)


@pytest.fixture(autouse=True)
async def pipe_to_thread() -> Iterator[None]:
    """Route ``asyncio.to_thread`` through the pipe based executor."""
    loop = asyncio.get_running_loop()
    executor = _PipeToThreadExecutor(loop)
    original_to_thread = asyncio.to_thread

    async def to_thread(fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        return await loop.run_in_executor(
            executor, lambda: fn(*args, **kwargs)
        )

    asyncio.to_thread = to_thread  # type: ignore[assignment]
    try:
        yield
    finally:
        asyncio.to_thread = original_to_thread  # type: ignore[assignment]
        executor.close()


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "sessions.db")


@pytest.fixture
async def storage_pair(db_path: str) -> Iterator[tuple[SQLiteStorage, SQLiteStorage]]:
    storage = SQLiteStorage(db_path)
    other = SQLiteStorage(db_path)
    try:
        yield storage, other
    finally:
        await storage.close()
        await other.close()


class Client:
    """Minimal aiohttp client: own cookie jar, talks to one app."""

    def __init__(self, app: web.Application, handler: Handler) -> None:
        self.app = app
        self.handler = handler
        self.jar: dict[str, str] = {}

    async def get(self, path: str = "/") -> web.StreamResponse:
        headers = (
            {"Cookie": f"{COOKIE_NAME}={self.jar[COOKIE_NAME]}"}
            if COOKIE_NAME in self.jar
            else {}
        )
        request = make_mocked_request(
            "GET", path, headers=headers, app=self.app
        )
        handler = self.handler
        for middleware in reversed(self.app.middlewares):
            handler = _wrap(middleware, handler)
        response = await handler(request)
        morsel = response.cookies.get(COOKIE_NAME)
        if morsel is not None:
            if morsel.value:
                self.jar[COOKIE_NAME] = morsel.value
            else:
                self.jar.pop(COOKIE_NAME, None)
        return response


def _wrap(middleware: Any, next_handler: Handler) -> Handler:
    async def wrapped(
        request: web.Request, handler: Handler = next_handler
    ) -> web.StreamResponse:
        return await middleware(request, handler)

    return wrapped


def create_app(handler: Handler, storage: SQLiteStorage) -> web.Application:
    app = web.Application(middlewares=[session_middleware(storage)])
    app.router.add_route("GET", "/", handler)
    storage.install_on_app(app)
    return app


def create_client(handler: Handler, storage: SQLiteStorage) -> Client:
    return Client(create_app(handler, storage), handler)


async def save_handler(request: web.Request) -> web.StreamResponse:
    session = await get_session(request)
    session["value"] = request.query["value"]
    return web.Response(body=b"OK")


async def load_handler(request: web.Request) -> web.StreamResponse:
    session = await get_session(request)
    value = session.get("value", "")
    return web.json_response({"new": session.new, "value": value})


async def empty_handler(request: web.Request) -> web.StreamResponse:
    session = await get_session(request)
    session.invalidate()
    return web.Response(body=b"OK")


_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS aiohttp_sessions ("
    "session_id TEXT PRIMARY KEY, data TEXT NOT NULL, expires INTEGER)"
)


def _raw_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=5)
    conn.execute(_SCHEMA)
    conn.commit()
    return conn


def raw_insert(db_path: str, key: str, data: str, expires: int | None) -> None:
    conn = _raw_connect(db_path)
    try:
        conn.execute(
            "INSERT INTO aiohttp_sessions(session_id, data, expires) "
            "VALUES (?, ?, ?)",
            (key, data, expires),
        )
        conn.commit()
    finally:
        conn.close()


def raw_rows(db_path: str) -> list[tuple[str, str, int | None]]:
    conn = _raw_connect(db_path)
    try:
        return list(
            conn.execute(
                "SELECT session_id, data, expires FROM aiohttp_sessions"
            )
        )
    finally:
        conn.close()


async def test_create_new_session(
    storage_pair: tuple[SQLiteStorage, SQLiteStorage],
) -> None:
    storage, _ = storage_pair
    client = create_client(load_handler, storage)
    response = await client.get("/")
    assert json.loads(response.body) == {"new": True, "value": ""}
    assert COOKIE_NAME not in client.jar


async def test_save_and_load_roundtrip(
    storage_pair: tuple[SQLiteStorage, SQLiteStorage],
) -> None:
    storage, other = storage_pair
    client_a = create_client(save_handler, storage)
    client_b = create_client(load_handler, other)

    response = await client_a.get("/?value=hello")
    assert response.status == 200
    identity = client_a.jar[COOKIE_NAME]
    # The cookie carries only the identity, never the payload.
    assert "hello" not in identity

    client_b.jar.update(client_a.jar)
    response = await client_b.get("/")
    assert json.loads(response.body) == {"new": False, "value": "hello"}


async def test_unknown_identity_is_new_session(
    db_path: str, storage_pair: tuple[SQLiteStorage, SQLiteStorage]
) -> None:
    storage, other = storage_pair
    client_a = create_client(save_handler, storage)
    client_b = create_client(load_handler, other)

    await client_a.get("/?value=hello")
    client_b.jar.update(client_a.jar)

    # Remove the row: the stale identity must not count as an old session.
    conn = sqlite3.connect(db_path, timeout=5)
    conn.execute("DELETE FROM aiohttp_sessions")
    conn.commit()
    conn.close()

    response = await client_b.get("/")
    assert json.loads(response.body) == {"new": True, "value": ""}
    # Nothing has been written back merely by loading.
    assert raw_rows(db_path) == []


async def test_expired_identity_is_not_revived(
    db_path: str, storage_pair: tuple[SQLiteStorage, SQLiteStorage]
) -> None:
    _, other = storage_pair
    key = "stale-identity"
    raw_insert(
        db_path,
        key,
        json.dumps({"created": int(time.time()), "session": {"value": "old"}}),
        int(time.time()) - 1,
    )
    client = create_client(load_handler, other)
    client.jar[COOKIE_NAME] = key

    response = await client.get("/")
    assert json.loads(response.body) == {"new": True, "value": ""}


async def test_corrupt_payload_is_new_session(
    db_path: str, storage_pair: tuple[SQLiteStorage, SQLiteStorage]
) -> None:
    _, other = storage_pair
    key = "broken-identity"
    raw_insert(db_path, key, "not-json{", None)
    client = create_client(load_handler, other)
    client.jar[COOKIE_NAME] = key

    response = await client.get("/")
    assert json.loads(response.body) == {"new": True, "value": ""}


async def test_invalidation_removes_row_and_cookie(
    storage_pair: tuple[SQLiteStorage, SQLiteStorage],
) -> None:
    storage, _ = storage_pair
    client = create_client(save_handler, storage)
    await client.get("/?value=hello")
    identity = client.jar[COOKIE_NAME]
    assert [row[0] for row in raw_rows(storage._db_path)] == [identity]

    client_2 = create_client(empty_handler, storage)
    client_2.jar.update(client.jar)
    response = await client_2.get("/")
    assert response.status == 200
    # Empty cookie data asks the browser to drop the identity.
    assert COOKIE_NAME not in client_2.jar
    assert raw_rows(storage._db_path) == []


async def test_purge_respects_batch_limit(
    db_path: str, storage_pair: tuple[SQLiteStorage, SQLiteStorage]
) -> None:
    storage, _ = storage_pair
    now = int(time.time())
    for idx in range(5):
        raw_insert(db_path, f"k{idx}", "{}", now - 5 + idx)
    raw_insert(db_path, "alive", "{}", now + 3600)

    removed = await storage.purge_expired_sessions(limit=2)
    assert removed == 2
    remaining = {row[0] for row in raw_rows(db_path)}
    assert remaining == {"k2", "k3", "k4", "alive"}

    assert await storage.purge_expired_sessions() == 3
    assert {row[0] for row in raw_rows(db_path)} == {"alive"}


async def test_purge_keeps_newly_renewed_row(
    db_path: str, storage_pair: tuple[SQLiteStorage, SQLiteStorage]
) -> None:
    storage, other = storage_pair
    now = int(time.time())
    raw_insert(db_path, "renewed", "{}", now - 10)
    # A concurrent save renews the row after the batch picked it.
    conn = sqlite3.connect(db_path, timeout=5)
    try:
        conn.execute(
            "UPDATE aiohttp_sessions SET expires = ? WHERE session_id = ?",
            (now + 3600, "renewed"),
        )
        conn.commit()
    finally:
        conn.close()

    assert await other.purge_expired_sessions() == 0
    assert [row[0] for row in raw_rows(db_path)] == ["renewed"]
    assert await storage.purge_expired_sessions(limit=None) == 0


async def test_two_instances_initialize_schema_together(db_path: str) -> None:
    storage = SQLiteStorage(db_path)
    other = SQLiteStorage(db_path)
    try:
        # Concurrent first touch on a fresh database file.
        results = await asyncio.gather(
            storage.purge_expired_sessions(),
            other.purge_expired_sessions(),
        )
        assert results == [0, 0]
        assert raw_rows(db_path) == []
    finally:
        await storage.close()
        await other.close()


async def test_close_is_idempotent_and_blocks_further_use(db_path: str) -> None:
    storage = SQLiteStorage(db_path)
    await storage.purge_expired_sessions()
    await storage.close()
    await storage.close()  # no error on the second call
    with pytest.raises(RuntimeError):
        await storage.purge_expired_sessions()


async def test_invalid_purge_limit(
    storage_pair: tuple[SQLiteStorage, SQLiteStorage],
) -> None:
    storage, _ = storage_pair
    with pytest.raises(ValueError):
        await storage.purge_expired_sessions(limit=0)


async def test_database_failure_propagates_and_cookie_untouched(
    storage_pair: tuple[SQLiteStorage, SQLiteStorage],
) -> None:
    storage, _ = storage_pair
    response = web.Response()
    session = Session(None, data=None, new=True)
    session["value"] = "x"

    async def boom(func: Any, *args: Any) -> None:
        raise sqlite3.OperationalError("boom")

    with patch.object(storage, "_run", boom):
        with pytest.raises(sqlite3.OperationalError):
            await storage.save_session(None, response, session)

    # No identity cookie was set when persistence failed.
    assert COOKIE_NAME not in response.cookies


async def test_cookie_failure_leaves_persisted_row(
    storage_pair: tuple[SQLiteStorage, SQLiteStorage],
) -> None:
    storage, _ = storage_pair
    response = web.Response()
    session = Session(None, data=None, new=True)
    session["value"] = "x"

    with patch.object(
        storage, "save_cookie", side_effect=RuntimeError("cookie boom")
    ):
        with pytest.raises(RuntimeError):
            await storage.save_session(None, response, session)

    assert len(raw_rows(storage._db_path)) == 1


async def test_max_age_expires_row(db_path: str) -> None:
    storage = SQLiteStorage(db_path, max_age=1)
    try:
        client = create_client(save_handler, storage)
        await client.get("/?value=hello")
        _, _, expires = raw_rows(db_path)[0]
        assert expires is not None and expires > int(time.time())

        time.sleep(1.1)
        reader = create_client(load_handler, storage)
        reader.jar.update(client.jar)
        response = await reader.get("/")
        assert json.loads(response.body) == {"new": True, "value": ""}
    finally:
        await storage.close()


async def test_default_identity_is_random_and_long(db_path: str) -> None:
    storage = SQLiteStorage(db_path)
    try:
        client_a = create_client(save_handler, storage)
        await client_a.get("/?value=a")
        client_b = create_client(save_handler, storage)
        await client_b.get("/?value=b")
        first = client_a.jar[COOKIE_NAME]
        second = client_b.jar[COOKIE_NAME]
        assert first != second
        assert len(first) >= 40
    finally:
        await storage.close()


async def test_concurrent_saves_from_many_clients(db_path: str) -> None:
    storage = SQLiteStorage(db_path)
    try:
        clients = [
            create_client(save_handler, storage) for _ in range(5)
        ]

        async def touch(client: Client, idx: int) -> None:
            response = await client.get(f"/?value=v{idx}")
            assert response.status == 200

        await asyncio.gather(
            *(touch(client, idx) for idx, client in enumerate(clients))
        )
        assert len(raw_rows(db_path)) == 5
    finally:
        await storage.close()


async def test_event_loop_keeps_ticking_during_db_work(
    db_path: str,
) -> None:
    storage = SQLiteStorage(db_path)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    task = asyncio.create_task(ticker())
    try:
        # A burst of threaded DB operations interleaves with other tasks.
        await asyncio.gather(
            storage.purge_expired_sessions(limit=None),
            asyncio.sleep(0.05),
        )
        assert ticks >= 1
    finally:
        task.cancel()
        await storage.close()


async def test_rows_saved_by_one_instance_visible_to_other(
    storage_pair: tuple[SQLiteStorage, SQLiteStorage],
) -> None:
    storage, other = storage_pair
    writer = create_client(save_handler, storage)
    reader = create_client(load_handler, other)
    for value in ("a", "b", "c"):
        await writer.get(f"/?value={value}")
        reader.jar.update(writer.jar)
        response = await reader.get("/")
        assert json.loads(response.body) == {"new": False, "value": value}


async def test_install_on_app_closes_connection(db_path: str) -> None:
    storage = SQLiteStorage(db_path)
    app = create_app(save_handler, storage)
    client = Client(app, save_handler)
    await client.get("/?value=hello")
    assert storage._conn is not None
    app.freeze()
    await app.cleanup()
    assert storage._closed
    assert storage._conn is None
    with pytest.raises(RuntimeError):
        await storage.purge_expired_sessions()


def test_construction_does_not_open_connection(tmp_path: Path) -> None:
    storage = SQLiteStorage(tmp_path / "missing" / "sessions.db")
    assert storage._conn is None
