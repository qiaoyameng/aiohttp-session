"""SQLite backed session storage.

The storage keeps only the session identity (a random token) in the
client cookie; the session payload and its absolute expiration time
live in a single SQLite table.

All sqlite3 calls are blocking.  To avoid stalling the event loop every
operation is executed by a dedicated worker thread that owns the
:class:`sqlite3.Connection` exclusively (sqlite3 connections are not
safe to share between threads).  The connection is created lazily on
that thread, including the table initialization, so two workers
(processes or storage instances sharing the same file) may run the
startup code concurrently -- the schema is created with
``IF NOT EXISTS``.

Operations are serialized on that one thread, which also serializes
writes and lets SQLite's busy timeout resolve lock contention with
connections from other processes.

The storage is *not* closed automatically and it runs no background
tasks; call :meth:`SQLiteStorage.close` (or use it as an async context
manager) on application shutdown.
"""

import asyncio
import json
import os
import secrets
import sqlite3
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import TracebackType
from typing import Any

from aiohttp import web

from . import AbstractStorage, Session

__all__ = ("SQLiteStorage",)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS aiohttp_sessions (
    session_key TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    expires_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_aiohttp_sessions_expires_at
    ON aiohttp_sessions (expires_at);
"""

_PRAGMAS = (
    # WAL lets a reader and a writer from different processes coexist
    # instead of immediately raising "database is locked".
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
)

# Number of times initialization is retried while another connection
# holds the lock during one-time WAL conversion / schema creation.
_INIT_ATTEMPTS = 20


class _SerialWorker:
    """Run blocking functions on one thread that owns a sqlite DB.

    Tasks are FIFO-serialized on a single worker thread, so the
    :class:`sqlite3.Connection` is only ever touched by that thread.

    Completion is signalled back to the event loop through a pipe
    watched with ``loop.add_reader`` rather than relying solely on the
    executor future's cross-thread callback: that callback occasionally
    fails to wake a parked selector when the blocking function returns
    immediately (observed with very fast sqlite statements).
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="aiohttp-session-sqlite"
        )
        self._read_fd, self._write_fd = os.pipe()
        os.set_blocking(self._read_fd, False)
        self._write_lock = threading.Lock()
        self.closed = False
        self._loop.add_reader(self._read_fd, self._drain_pipe)

    def _drain_pipe(self) -> None:
        while True:
            try:
                os.read(self._read_fd, 65536)
            except BlockingIOError:
                break

    def submit(self, func: Callable[..., Any], *args: Any) -> asyncio.Future[Any]:
        """Run *func* on the worker thread; return an asyncio future."""
        if self.closed:
            raise RuntimeError("SQLiteStorage worker is closed")
        concurrent_future = self._executor.submit(func, *args)
        concurrent_future.add_done_callback(self._on_done)
        return asyncio.wrap_future(concurrent_future, loop=self._loop)

    def _on_done(self, _future: Any) -> None:
        # Runs on the worker thread.  A readable byte reliably wakes the
        # selector even when the wrapped future callback alone does not.
        # The wrapped future is the source of truth; the byte is only a
        # wake-up nudge, so a (very unlikely) full pipe is ignored.
        try:
            with self._write_lock:
                if not self.closed:
                    os.write(self._write_fd, b"x")
        except (BlockingIOError, OSError):
            pass

    def shutdown(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self._executor.shutdown(wait=True)
        finally:
            if not self._loop.is_closed():
                self._loop.remove_reader(self._read_fd)
            with self._write_lock:
                os.close(self._write_fd)
            os.close(self._read_fd)


class SQLiteStorage(AbstractStorage):
    """Store sessions in a SQLite database using the stdlib ``sqlite3``.

    Args:
        db_path: Path of the SQLite file, or ``":memory:"`` for an
            in-memory database.  Parent directories must exist.  The
            file (and the ``-wal``/``-shm`` side files in WAL mode) is
            created on first use.
        cookie_name: Name of the cookie that carries the identity.
        domain: Cookie domain, ``str`` or ``None``.
        max_age: Default session lifetime in seconds; ``None`` means
            sessions never expire server-side (the row has no absolute
            expiration time).  Per-session ``session.max_age`` overrides
            it.
        path: Cookie path.
        secure: Cookie ``Secure`` flag.
        httponly: Cookie ``HttpOnly`` flag.
        samesite: Cookie ``SameSite`` flag.
        key_factory: Zero-argument callable returning a new session
            identity.  Defaults to :func:`secrets.token_urlsafe`, i.e.
            a cryptographically secure random source.
        encoder: Session payload serializer.
        decoder: Session payload deserializer.
        timeout: SQLite busy timeout in seconds.  While a write lock is
            held by another connection (e.g. a second worker process)
            statements wait up to that long before
            :class:`sqlite3.OperationalError` ("database is locked") is
            raised.

    Failure boundaries:
        * Table initialization and every database operation run on a
          worker thread; errors (including initialization errors or
          ``database is locked`` after *timeout*) propagate from the
          awaited coroutine that triggered them.
        * :meth:`save_session` sets the cookie *before* writing to the
          database.  A failed write therefore propagates as an
          exception while the response may already carry a cookie; no
          session is persisted for that identity.  Conversely a cookie
          failure raises before any database write happens.
        * Expired rows are removed only by explicit
          :meth:`cleanup_expired` calls; there is no background loop.

    Shut down explicitly, or use the storage as an async context
    manager::

        await storage.close()
        # app.on_cleanup.append(lambda app: storage.close())
    """

    def __init__(
        self,
        db_path: str | Path = "aiohttp_sessions.db",
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
        self._key_factory = key_factory
        self._db_path = str(db_path)
        self._timeout = timeout
        self._connection: sqlite3.Connection | None = None
        self._worker_ident: int | None = None
        self._init_error: BaseException | None = None
        # The worker (and its loop-bound completion pipe) are created
        # lazily on the first awaited call, so the storage can be
        # constructed before an event loop is running.
        self._worker: _SerialWorker | None = None
        self._init_future: asyncio.Future[Any] | None = None

    async def _start(self) -> None:
        """Create the worker and wait for the schema to initialize."""
        if self._worker is None:
            worker = _SerialWorker(asyncio.get_running_loop())
            self._worker = worker
            # FIFO order guarantees initialization runs before any
            # other task submitted to the same worker.
            self._init_future = worker.submit(self._init_connection)
        assert self._init_future is not None
        await self._init_future

    async def _ensure_started(self) -> None:
        """Like :meth:`_start` but swallows an initialization failure."""
        if self._worker is None:
            worker = _SerialWorker(asyncio.get_running_loop())
            self._worker = worker
            self._init_future = worker.submit(self._init_connection)
        assert self._init_future is not None
        try:
            await self._init_future
        except BaseException:
            # Remembered on self._init_error; close() ignores it.
            pass

    def _init_connection(self) -> None:
        """Open the connection and initialize the schema.

        Runs first on the dedicated worker thread.  Switching a fresh
        file into WAL mode briefly needs an exclusive lock, so when two
        workers initialize the same database a connection may hit
        "database is locked"; initialization is idempotent and is
        retried with a short backoff.  A terminal error is stored and
        re-raised by every subsequent operation.
        """
        self._worker_ident = threading.get_ident()
        busy_timeout_ms = max(0, int(self._timeout * 1000))
        last_error: sqlite3.OperationalError | None = None
        for attempt in range(_INIT_ATTEMPTS):
            conn: sqlite3.Connection | None = None
            try:
                conn = sqlite3.connect(
                    self._db_path,
                    timeout=self._timeout,
                    check_same_thread=True,
                )
                # Apply before any other statement so the one-time WAL
                # conversion also honors the busy timeout.
                conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
                for pragma in _PRAGMAS:
                    conn.execute(pragma)
                conn.executescript(_SCHEMA)
                conn.commit()
            except sqlite3.OperationalError as exc:
                if conn is not None:
                    conn.close()
                last_error = exc
                retryable = "locked" in str(exc).lower()
                if not retryable or attempt == _INIT_ATTEMPTS - 1:
                    self._init_error = exc
                    raise
                time.sleep(min(0.05 * (attempt + 1), 0.5))
            except BaseException as exc:
                if conn is not None:
                    conn.close()
                self._init_error = exc
                raise
            else:
                self._connection = conn
                self._init_error = None
                return
        assert last_error is not None  # pragma: no cover
        self._init_error = last_error
        raise last_error

    def _conn(self) -> sqlite3.Connection:
        """Return the worker-owned connection or raise its init error."""
        if self._init_error is not None:
            raise self._init_error
        conn = self._connection
        if conn is None:
            raise RuntimeError(
                "SQLiteStorage connection is not initialized or was closed"
            )
        return conn

    async def _run(self, func: Callable[..., Any], *args: Any) -> Any:
        """Wait for initialization, then run *func* on the worker thread."""
        await self._start()
        assert self._worker is not None
        return await self._worker.submit(func, *args)

    async def load_session(self, request: web.Request) -> Session:
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        key = str(cookie)
        row = await self._run(self._load_row, key)
        if row is None:
            # Unknown or already expired identity: it must not be
            # treated as a valid existing session.
            return Session(None, data=None, new=True, max_age=self.max_age)
        data_str, expires_at = row
        if expires_at is not None and expires_at <= time.time():
            return Session(None, data=None, new=True, max_age=self.max_age)
        try:
            data = self._decoder(data_str)
        except ValueError:
            data = None
        return Session(key, data=data, new=False, max_age=self.max_age)

    def _load_row(self, key: str) -> tuple[str, int | None] | None:
        cur = self._conn().execute(
            "SELECT data, expires_at FROM aiohttp_sessions "
            "WHERE session_key = ?",
            (key,),
        )
        return cur.fetchone()

    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        key = session.identity
        if key is None:
            key = self._key_factory()
            self.save_cookie(response, key, max_age=session.max_age)
        elif session.empty:
            self.save_cookie(response, "", max_age=session.max_age)
        else:
            key = str(key)
            self.save_cookie(response, key, max_age=session.max_age)

        if session.empty:
            await self._run(self._delete_row, str(key))
            return

        data_str = self._encoder(self._get_session_data(session))
        max_age = self.max_age if session.max_age is None else session.max_age
        expires_at = None if max_age is None else int(time.time()) + int(max_age)
        await self._run(self._upsert_row, str(key), data_str, expires_at)

    def _upsert_row(self, key: str, data: str, expires_at: int | None) -> None:
        conn = self._conn()
        conn.execute(
            "INSERT INTO aiohttp_sessions (session_key, data, expires_at) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(session_key) DO UPDATE SET "
            "data = excluded.data, expires_at = excluded.expires_at",
            (key, data, expires_at),
        )
        conn.commit()

    def _delete_row(self, key: str) -> None:
        conn = self._conn()
        conn.execute(
            "DELETE FROM aiohttp_sessions WHERE session_key = ?", (key,)
        )
        conn.commit()

    async def cleanup_expired(self, limit: int = 1000) -> int:
        """Delete at most *limit* sessions past their absolute expiration.

        Rows without an expiration time are never removed.  Returns the
        number of deleted rows.  Invoke it repeatedly (e.g. from an
        externally scheduled task) until it returns less than *limit*.

        The delete filters on ``expires_at`` in the same statement, so a
        row refreshed (given a future expiration) by a concurrent writer
        is never removed by a cleanup that started before the refresh.
        """
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        return await self._run(self._cleanup_expired, int(time.time()), limit)

    def _cleanup_expired(self, now: float, limit: int) -> int:
        conn = self._conn()
        cur = conn.execute(
            "DELETE FROM aiohttp_sessions "
            "WHERE rowid IN ("
            "  SELECT rowid FROM aiohttp_sessions "
            "  WHERE expires_at IS NOT NULL AND expires_at <= ? "
            "  LIMIT ?"
            ")",
            (now, limit),
        )
        deleted = cur.rowcount
        conn.commit()
        return deleted

    async def close(self) -> None:
        """Close the connection and stop the worker thread.

        Idempotent.  Pending operations are drained first; after
        :meth:`close` every further operation raises
        :class:`RuntimeError`.
        """
        if self._worker is None:
            # Never started: no worker thread or connection to clean up.
            return
        if self._worker.closed:
            return
        try:
            # close() must not raise the initialization error; it is
            # already surfaced from regular operations.  Just make sure
            # initialization has settled before closing the connection.
            await self._ensure_started()
            if self._init_error is None:
                await self._worker.submit(self._close_connection)
        finally:
            self._worker.shutdown()

    def _close_connection(self) -> None:
        conn = self._connection
        if conn is not None:
            conn.close()
            self._connection = None

    async def __aenter__(self) -> "SQLiteStorage":
        try:
            await self._start()
        except BaseException:
            # A failed entry must not leak the worker thread.
            assert self._worker is not None
            self._worker.shutdown()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
