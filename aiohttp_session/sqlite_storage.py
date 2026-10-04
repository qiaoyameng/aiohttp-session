"""SQLite backed session storage.

Suitable for small single-node deployments that do not want to run a
Redis/Memcached service.  Only the opaque session identity is kept in the
client cookie; the session payload and its absolute expiration time live
in a SQLite database file.
"""

import asyncio
import json
import secrets
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from aiohttp import web

from . import AbstractStorage, Session

_P = ParamSpec("_P")
_T = TypeVar("_T")

_TABLE = "aiohttp_sessions"

_CREATE_TABLE = (
    f"CREATE TABLE IF NOT EXISTS {_TABLE} ("
    "session_id TEXT PRIMARY KEY, "
    "data TEXT NOT NULL, "
    "expires INTEGER)"
)
_CREATE_INDEX = (
    f"CREATE INDEX IF NOT EXISTS idx_{_TABLE}_expires "
    f"ON {_TABLE}(expires)"
)

_INSERT_SESSION = (
    f"INSERT INTO {_TABLE}(session_id, data, expires) VALUES (?, ?, ?) "
    "ON CONFLICT(session_id) DO UPDATE SET data = excluded.data, "
    "expires = excluded.expires"
)
_LOAD_SESSION = f"SELECT data, expires FROM {_TABLE} WHERE session_id = ?"
_DELETE_SESSION = f"DELETE FROM {_TABLE} WHERE session_id = ?"
# Delete a bounded batch of expired rows in one statement.  The rowid
# membership is re-qualified against *current* rows in the DELETE: if a
# concurrent save renews a selected row (new ``expires`` in the future)
# the DELETE sees the fresh value through the join and keeps it.
_PURGE_EXPIRED_SAFE = (
    f"DELETE FROM {_TABLE} WHERE rowid IN ("
    f"SELECT old.rowid FROM {_TABLE} AS old JOIN {_TABLE} AS cur "
    "ON cur.rowid = old.rowid "
    "WHERE old.expires IS NOT NULL AND old.expires <= ? "
    "AND cur.expires IS NOT NULL AND cur.expires <= ? "
    "ORDER BY old.expires LIMIT ?)"
)
_PURGE_EXPIRED_SAFE_UNLIMITED = (
    f"DELETE FROM {_TABLE} WHERE rowid IN ("
    f"SELECT old.rowid FROM {_TABLE} AS old JOIN {_TABLE} AS cur "
    "ON cur.rowid = old.rowid "
    "WHERE old.expires IS NOT NULL AND old.expires <= ? "
    "AND cur.expires IS NOT NULL AND cur.expires <= ?)"
)


def _connect(path: str | Path, timeout: float) -> sqlite3.Connection:
    """Open a connection and initialize the schema.

    ``check_same_thread`` is disabled on purpose: the connection is owned
    by the storage instance and every database call goes through a single
    ``asyncio.Lock``, so it is only ever used from one worker thread at a
    time.  Schema creation is wrapped in a locking transaction so that
    two workers (or two storage instances) initializing a fresh database
    do not corrupt each other; ``IF NOT EXISTS`` makes it idempotent.
    """
    # Autocommit mode: short statements commit on their own and schema
    # setup uses an explicit locking transaction below.
    conn = sqlite3.connect(
        str(path),
        timeout=timeout,
        check_same_thread=False,
        isolation_level=None,
    )
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(_CREATE_TABLE)
        conn.execute(_CREATE_INDEX)
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        conn.close()
        raise
    return conn


def _load_row(
    conn: sqlite3.Connection, key: str
) -> tuple[str, int | None] | None:
    row = conn.execute(_LOAD_SESSION, (key,)).fetchone()
    if row is None:
        return None
    data, expires = row
    return str(data), expires


def _delete_row(conn: sqlite3.Connection, key: str) -> None:
    conn.execute(_DELETE_SESSION, (key,))


class SQLiteStorage(AbstractStorage):
    """Session storage backed by a SQLite database file.

    The client cookie carries only the random session identity; the JSON
    encoded session data and the absolute expiration time (Unix seconds,
    ``NULL`` means "never expires") are stored in the
    ``aiohttp_sessions`` table.

    Each storage instance owns its own :class:`sqlite3.Connection`.  The
    connection is opened lazily on the first request and all blocking
    database operations run in a worker thread via
    :func:`asyncio.to_thread`, serialized by an :class:`asyncio.Lock`, so
    the event loop is never blocked and SQLite's connection-per-thread
    restriction is respected.  ``timeout`` is forwarded to SQLite as the
    busy timeout; concurrent writers wait that long instead of failing
    immediately with "database is locked".

    Failure boundaries: :meth:`save_session` persists the row *before*
    touching the response cookie, so a database error propagates to the
    caller with the cookie untouched; cookie failures likewise leave the
    already persisted row in place.  No background task is started, use
    :meth:`purge_expired_sessions` (e.g. from a scheduler) to remove old
    rows and :meth:`close` (or :meth:`install_on_app`) to release the
    connection.

    Pass ``":memory:"`` to use an in-memory database; note that such a
    database is private to every storage instance, so two instances no
    longer share sessions.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        cookie_name: str = "AIOHTTP_SESSION",
        domain: str | None = None,
        max_age: int | None = None,
        path: str = "/",
        secure: bool | None = None,
        httponly: bool = True,
        samesite: str | None = None,
        key_factory: Callable[[], str] = lambda: secrets.token_urlsafe(32),
        encoder: Callable[[object], str] = json.dumps,
        decoder: Callable[[str], Any] = json.loads,
        timeout: float = 5.0,
    ) -> None:
        super().__init__(
            cookie_name=cookie_name,
            domain=domain,
            max_age=max_age,
            path=path,
            secure=secure,
            httponly=httponly,
            samesite=samesite,
            encoder=encoder,
            decoder=decoder,
        )
        self._db_path = str(db_path)
        self._timeout = timeout
        self._key_factory = key_factory
        self._lock = asyncio.Lock()
        self._conn: sqlite3.Connection | None = None
        self._closed = False

    async def _get_connection_locked(self) -> sqlite3.Connection:
        """Return the connection; called with ``self._lock`` held."""
        if self._closed:
            raise RuntimeError("SQLiteStorage is closed")
        if self._conn is None:
            self._conn = await asyncio.to_thread(
                _connect, self._db_path, self._timeout
            )
        return self._conn

    async def _run(
        self, func: Callable[_P, _T], *args: Any, **kwargs: Any
    ) -> _T:
        """Run *func* on the owned connection without blocking the loop.

        The whole operation (including the thread hop) holds the lock so
        that :meth:`close` cannot tear the connection down mid-statement.
        """
        async with self._lock:
            conn = await self._get_connection_locked()
            return await asyncio.to_thread(func, conn, *args, **kwargs)

    async def load_session(self, request: web.Request) -> Session:
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        key = str(cookie)
        row = await self._run(_load_row, key)
        if row is None:
            # Unknown identity: do not resurrect it as an old session.
            return Session(None, data=None, new=True, max_age=self.max_age)
        data_str, expires = row
        now = int(time.time())
        if expires is not None and expires <= now:
            # Absolute expiration recorded in the database wins.
            return Session(None, data=None, new=True, max_age=self.max_age)
        try:
            data = self._decoder(data_str)
        except ValueError:
            # Corrupt payload: do not treat the identity as a valid
            # old session.
            return Session(None, data=None, new=True, max_age=self.max_age)
        return Session(key, data=data, new=False, max_age=self.max_age)

    async def save_session(
        self,
        request: web.Request,
        response: web.StreamResponse,
        session: Session,
    ) -> None:
        key = session.identity
        if key is None:
            key = self._key_factory()
            is_new_identity = True
        else:
            key = str(key)
            is_new_identity = False

        now = int(time.time())
        if session.max_age is not None:
            expires: int | None = now + session.max_age
        else:
            expires = None

        if session.empty:
            # Persist first; the cookie is dropped only after the DB
            # operation succeeded.
            if not is_new_identity:
                await self._run(_delete_row, key)
            self.save_cookie(response, "", max_age=session.max_age)
            return

        data_str = self._encoder(self._get_session_data(session))

        def _save(conn: sqlite3.Connection) -> None:
            conn.execute(_INSERT_SESSION, (key, data_str, expires))

        await self._run(_save)
        self.save_cookie(response, key, max_age=session.max_age)

    async def purge_expired_sessions(self, limit: int | None = 1000) -> int:
        """Delete at most *limit* expired sessions; return rows removed.

        ``None`` removes all expired rows in one statement.  The
        expiration of each candidate is re-checked against the current
        row value in the DELETE, so a session saved again with a fresh
        ``expires`` after being picked is not removed.  No background
        loop is started; call this periodically yourself.
        """
        if limit is not None and limit <= 0:
            raise ValueError("limit must be a positive integer or None")
        now = int(time.time())

        def _purge(conn: sqlite3.Connection) -> int:
            if limit is None:
                cursor = conn.execute(_PURGE_EXPIRED_SAFE_UNLIMITED, (now, now))
            else:
                cursor = conn.execute(_PURGE_EXPIRED_SAFE, (now, now, limit))
            return int(cursor.rowcount)

        return await self._run(_purge)

    async def close(self) -> None:
        """Close the owned connection. Idempotent and safe if unused."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            conn, self._conn = self._conn, None
        if conn is not None:
            await asyncio.to_thread(conn.close)

    def install_on_app(self, app: web.Application) -> None:
        """Register :meth:`close` on *app* shutdown as a cleanup hook."""
        async def _cleanup(_app: web.Application) -> None:
            await self.close()

        app.on_cleanup.append(_cleanup)
