"""Identity rotation (session fixation defence) tests.

The server-side behaviour is exercised against :class:`AtomicMemoryBackend`,
an in-memory double whose rotation is a single synchronous critical section
(the same atomicity guarantee a Redis Lua script provides).  A late-saving
request -- one that loaded the old identity before rotation and saves after
it -- is recreated with coroutine scheduling, and must never be able to
resurrect the revoked identity or reveal the logged-in state.

No real network sockets are used: the session middleware is driven with
:func:`aiohttp.test_utils.make_mocked_request`.
"""

from __future__ import annotations

import asyncio
import json
import itertools
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import quote, unquote

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

import aiohttp_session
from aiohttp_session import (
    IdentityRevokedError,
    IdentityRotationUnsupported,
    RotatedIdentity,
    Session,
    SessionRotationError,
    TOMBSTONE_MARKER,
    get_session,
    session_middleware,
)

try:
    from aiohttp_session.memcached_storage import MemcachedStorage
except ModuleNotFoundError:  # pragma: no cover - aiomcache not installed here
    import sys
    import types

    aiomcache_mod = types.ModuleType("aiomcache")
    exceptions_mod = types.ModuleType("aiomcache.exceptions")

    class ClientException(Exception):
        pass

    class Client:  # minimal stand-in; never exercised
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    exceptions_mod.ClientException = ClientException
    aiomcache_mod.exceptions = exceptions_mod
    aiomcache_mod.Client = Client
    sys.modules.setdefault("aiomcache", aiomcache_mod)
    sys.modules.setdefault("aiomcache.exceptions", exceptions_mod)
    from aiohttp_session.memcached_storage import MemcachedStorage


class AtomicMemoryBackend:
    """In-memory server-side backend with a genuinely atomic rotation.

    Critical sections are synchronous, so under asyncio they are
    indivisible with respect to other coroutines -- the same guarantee a
    Redis Lua script provides.  Records map an identity to
    ``(payload, expire_at)``; revoked identities keep a payload prefixed
    with the tombstone marker, and writes onto a tombstone are rejected.
    """

    def __init__(self) -> None:
        self._records: dict[str, tuple[str, float | None]] = {}
        self._clock = time.time

    def set_clock(self, func: Callable[[], float]) -> None:
        self._clock = func

    def alive(self) -> dict[str, tuple[str, float | None]]:
        now = self._clock()
        return {
            key: value
            for key, value in self._records.items()
            if value[1] is None or value[1] > now
        }

    def is_tombstone(self, identity: str) -> bool:
        record = self.alive().get(identity)
        return record is not None and record[0].startswith(TOMBSTONE_MARKER)

    def rotate(
        self,
        old_identity: str,
        new_identity: str,
        new_data: str,
        tombstone: str,
        *,
        new_ttl: int | None,
        tombstone_ttl: int | None,
    ) -> int | None:
        now = self._clock()
        records = self._records
        current = records.get(old_identity)
        if current is None or (current[1] is not None and current[1] <= now):
            raise IdentityRevokedError(f"{old_identity!r} is missing")
        if current[0].startswith(TOMBSTONE_MARKER):
            raise IdentityRevokedError(f"{old_identity!r} has been revoked")
        if new_identity in records:
            raise IdentityRevokedError(f"{new_identity!r} already exists")
        old_expire = current[1]
        old_ttl = None if old_expire is None else int(old_expire - now)
        records[new_identity] = (
            new_data,
            None if new_ttl is None else now + new_ttl,
        )
        records[old_identity] = (
            tombstone,
            None if tombstone_ttl is None else now + tombstone_ttl,
        )
        return old_ttl

    def guarded_set(
        self, identity: str, data: str, *, ttl: int | None
    ) -> None:
        now = self._clock()
        current = self._records.get(identity)
        if current is not None and current[0].startswith(TOMBSTONE_MARKER):
            raise IdentityRevokedError(f"{identity!r} was revoked")
        self._records[identity] = (data, None if ttl is None else now + ttl)

    def get(self, identity: str) -> str | None:
        record = self.alive().get(identity)
        return None if record is None else record[0]

    def delete(self, identity: str) -> None:
        self._records.pop(identity, None)


class FailingMemoryBackend(AtomicMemoryBackend):
    """Backend whose first rotation fails without touching any record."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_next_rotate = True

    def rotate(self, *args: Any, **kwargs: Any) -> int | None:
        if self.fail_next_rotate:
            self.fail_next_rotate = False
            raise RuntimeError("backend is down")
        return super().rotate(*args, **kwargs)


class InMemoryServerStorage(MemcachedStorage):
    """Server-side storage backed by an :class:`AtomicMemoryBackend`.

    Reuses :class:`MemcachedStorage`'s rotation orchestration
    (:meth:`~aiohttp_session.AbstractStorage._build_rotation`) but swaps
    the aiomcache connection for the atomic in-memory backend.
    """

    def __init__(
        self, backend: AtomicMemoryBackend, *, max_age: int | None = None
    ) -> None:
        super().__init__(memcached_conn=None, max_age=max_age)  # type: ignore[arg-type]
        self.backend = backend
        self._counter = itertools.count(1)

    def _new_identity(self) -> str:
        return f"id-{next(self._counter):08d}"

    async def rotate_identity(
        self,
        request: web.Request,
        session: Session,
        migrated: dict[str, Any],
        *,
        max_age: int | None,
        reset_ttl: bool,
    ) -> RotatedIdentity:
        old_identity = str(session.identity)
        (
            new_identity,
            new_data,
            tombstone,
            new_ttl,
            tombstone_ttl,
        ) = self._build_rotation(
            session, migrated, max_age=max_age, reset_ttl=reset_ttl
        )
        old_ttl = self.backend.rotate(
            old_identity,
            new_identity,
            new_data,
            tombstone,
            new_ttl=new_ttl,
            tombstone_ttl=tombstone_ttl,
        )
        return RotatedIdentity(new_identity, old_ttl)

    async def load_session(self, request: web.Request) -> Session:
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        payload = self.backend.get(str(cookie))
        if payload is None or payload.startswith(TOMBSTONE_MARKER):
            return Session(None, data=None, new=True, max_age=self.max_age)
        return Session(
            str(cookie),
            data=self._decoder(payload),
            new=False,
            max_age=self.max_age,
        )

    async def save_session(
        self,
        request: web.Request,
        response: web.StreamResponse,
        session: Session,
    ) -> None:
        identity = session.identity
        is_new_identity = identity is None
        if is_new_identity:
            identity = self._new_identity()
        key = str(identity)
        if session.empty:
            self.save_cookie(response, "", max_age=session.max_age)
            if not is_new_identity:
                self.backend.delete(key)
            return
        self.save_cookie(response, key, max_age=session.max_age)
        self.backend.guarded_set(
            key,
            self._encoder(self._get_session_data(session)),
            ttl=session.max_age,
        )


# --------------------------------------------------------------------------
# Test harness: drive session_middleware with mocked requests, no sockets.
# --------------------------------------------------------------------------


class CookieJar:
    def __init__(self, initial: dict[str, str] | None = None) -> None:
        self._cookies: dict[str, str] = dict(initial or {})

    def as_headers(self) -> dict[str, str]:
        if not self._cookies:
            return {}
        pairs = "; ".join(
            f"{k}={quote(v, safe='')}" for k, v in self._cookies.items()
        )
        return {"Cookie": pairs}

    def absorb(self, response: web.Response) -> None:
        for name, morsel in response.cookies.items():
            if morsel.value:
                self._cookies[name] = morsel.value
            else:
                self._cookies.pop(name, None)

    @property
    def session_cookie(self) -> str | None:
        return self._cookies.get("AIOHTTP_SESSION")


async def run_request(
    storage_: Any,
    handler: Callable[[web.Request], Any],
    jar: CookieJar | None = None,
    raw_cookie: bool = False,
) -> web.Response:
    """Run one request through the session middleware and return response.

    Exceptions raised by the middleware (e.g. a rejected late save)
    propagate to the caller.
    """
    jar = jar or CookieJar()
    middleware = session_middleware(storage_)
    request = make_mocked_request("GET", "/", headers=jar.as_headers())
    if raw_cookie:
        # make_mocked_request keeps cookies percent-encoded; emulate the
        # real server, which decodes them before handing them to storage.
        storage_.load_cookie = lambda request: (  # type: ignore[method-assign]
            unquote(request.cookies.get(storage_.cookie_name))
        )
    response = await middleware(request, handler)
    assert isinstance(response, web.Response)
    jar.absorb(response)
    return response


def make_record(
    storage_: InMemoryServerStorage,
    session_data: dict[str, Any],
    *,
    created: int | None = None,
    expires_at: int | None = None,
) -> str:
    created = created if created is not None else int(time.time())
    data: dict[str, Any] = {"created": created, "session": session_data}
    if expires_at is not None:
        data["expires_at"] = expires_at
    return storage_._encoder(data)


@pytest.fixture
def backend() -> AtomicMemoryBackend:
    return AtomicMemoryBackend()


@pytest.fixture
def storage(backend: AtomicMemoryBackend) -> InMemoryServerStorage:
    return InMemoryServerStorage(backend)


def seed(
    storage_: InMemoryServerStorage,
    identity: str,
    session_data: dict[str, Any],
    *,
    ttl: int | None = None,
    created: int | None = None,
    expires_at: int | None = None,
) -> None:
    storage_.backend.guarded_set(
        identity,
        make_record(
            storage_,
            session_data,
            created=created,
            expires_at=expires_at,
        ),
        ttl=ttl,
    )


# --------------------------------------------------------------------------
# Atomic backend protocol
# --------------------------------------------------------------------------


def test_duplicate_rotation_of_same_identity_rejected(
    backend: AtomicMemoryBackend,
) -> None:
    backend.guarded_set(
        "old", make_record(InMemoryServerStorage(backend), {"user": "x"}),
        ttl=None,
    )
    old_ttl = backend.rotate(
        "old", "new", make_record(InMemoryServerStorage(backend), {"user": "x"}),
        TOMBSTONE_MARKER + "{}",
        new_ttl=None,
        tombstone_ttl=100,
    )
    assert old_ttl is None
    assert backend.is_tombstone("old")
    # The retried rotation for the same old identity must not create a
    # second session.
    with pytest.raises(IdentityRevokedError):
        backend.rotate(
            "old", "new-2", "{}", TOMBSTONE_MARKER + "{}",
            new_ttl=None,
            tombstone_ttl=100,
        )
    assert backend.get("new-2") is None


def test_late_save_onto_revoked_identity_rejected(
    backend: AtomicMemoryBackend,
) -> None:
    backend.guarded_set("old", '{"session": {}}', ttl=None)
    backend.rotate(
        "old", "new", '{"session": {}}', TOMBSTONE_MARKER + "{}",
        new_ttl=None,
        tombstone_ttl=100,
    )
    with pytest.raises(IdentityRevokedError):
        backend.guarded_set(
            "old", '{"session": {"resurrected": true}}', ttl=None
        )


def test_rotate_missing_identity_creates_nothing(
    backend: AtomicMemoryBackend,
) -> None:
    with pytest.raises(IdentityRevokedError):
        backend.rotate(
            "ghost", "new", "{}", TOMBSTONE_MARKER + "{}",
            new_ttl=None,
            tombstone_ttl=100,
        )
    assert backend.get("new") is None


# --------------------------------------------------------------------------
# Session.rotate_identity through the middleware
# --------------------------------------------------------------------------


async def test_rotate_migrates_selected_fields_and_changes_cookie(
    storage: InMemoryServerStorage,
) -> None:
    seed(storage, "anon", {"cart": ["book"], "drop_me": True})
    jar = CookieJar({"AIOHTTP_SESSION": "anon"})

    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        session["user"] = "alice"
        session["is_admin"] = True
        new_identity = await session.rotate_identity(fields=("cart", "user"))
        assert new_identity == session.identity
        assert "is_admin" not in session
        assert "drop_me" not in session
        return web.Response(text="ok")

    await run_request(storage, handler, jar)

    logged_identity = jar.session_cookie
    assert logged_identity is not None
    assert logged_identity != "anon"
    new_payload = storage.backend.get(logged_identity)
    assert new_payload is not None
    assert storage._decoder(new_payload)["session"] == {
        "cart": ["book"],
        "user": "alice",
    }
    assert storage.backend.is_tombstone("anon")


async def test_old_cookie_cannot_retrieve_login_after_late_save(
    storage: InMemoryServerStorage,
) -> None:
    """The core fixation scenario, scheduled explicitly:

    1. request L loads the old identity, mutates it, then suspends
       (middleware save has not run);
    2. the login request rotates atomically: new record, old identity
       tombstoned, cookie replaced;
    3. L resumes and its middleware save is rejected -- the tombstone
       survives and the old cookie is unchanged;
    4. a later request carrying the old cookie loads a fresh anonymous
       session and never sees the logged-in data.
    """
    seed(storage, "old", {"cart": [1]})
    late_jar = CookieJar({"AIOHTTP_SESSION": "old"})
    login_jar = CookieJar({"AIOHTTP_SESSION": "old"})

    late_entered = asyncio.Event()
    release_late = asyncio.Event()

    async def late_handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        assert not session.new
        session["late"] = True
        late_entered.set()
        await release_late.wait()
        return web.Response(text="late")

    async def login_handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        session["user"] = "bob"
        await session.rotate_identity()
        return web.Response(text="ok")

    late_task = asyncio.ensure_future(
        run_request(storage, late_handler, late_jar)
    )
    await late_entered.wait()
    assert not storage.backend.is_tombstone("old")

    # Rotation happens while L is suspended.
    await run_request(storage, login_handler, login_jar)
    new_identity = login_jar.session_cookie
    assert new_identity != "old"
    assert storage.backend.is_tombstone("old")
    assert storage._decoder(storage.backend.get(new_identity))["session"] == {
        "cart": [1],
        "user": "bob",
    }

    # Release the stale request; its late save is rejected.
    release_late.set()
    with pytest.raises(IdentityRevokedError):
        await late_task
    assert storage.backend.is_tombstone("old")
    assert late_jar.session_cookie == "old"

    # Old cookie can never fetch the logged-in state again.
    async def check_handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        return web.json_response(
            {
                "new": session.new,
                "user": session.get("user"),
                "late": session.get("late"),
            }
        )

    response = await run_request(storage, check_handler, late_jar)
    body = json.loads(response.text)
    assert body["new"] is True
    assert body["user"] is None
    assert body["late"] is None
    payload = storage.backend.get("old")
    assert payload is None or payload.startswith(TOMBSTONE_MARKER)


async def test_retried_request_with_old_cookie_gets_anonymous_session(
    storage: InMemoryServerStorage,
) -> None:
    """A duplicated login request replaying the old cookie after rotation."""
    seed(storage, "old", {"cart": []})

    async def login(request: web.Request) -> web.Response:
        session = await get_session(request)
        if session.new:
            return web.json_response({"anon": True})
        session["user"] = "carol"
        await session.rotate_identity()
        return web.json_response({"ok": True})

    jar = CookieJar({"AIOHTTP_SESSION": "old"})
    response = await run_request(storage, login, jar)
    assert json.loads(response.text) == {"ok": True}
    assert storage.backend.is_tombstone("old")

    # Replay with the revoked cookie: rotation of a fresh session must
    # not occur, and protected data is never reachable.
    jar2 = CookieJar({"AIOHTTP_SESSION": "old"})
    response = await run_request(storage, login, jar2)
    assert json.loads(response.text) == {"anon": True}


async def test_rotation_then_invalidate_clears_new_keeps_tombstone(
    storage: InMemoryServerStorage,
) -> None:
    seed(storage, "old", {"cart": [2]})
    jar = CookieJar({"AIOHTTP_SESSION": "old"})

    async def login(request: web.Request) -> web.Response:
        session = await get_session(request)
        session["user"] = "dave"
        await session.rotate_identity()
        return web.Response(text="ok")

    async def logout(request: web.Request) -> web.Response:
        session = await get_session(request)
        session.invalidate()
        return web.Response(text="ok")

    await run_request(storage, login, jar)
    new_identity = jar.session_cookie
    assert new_identity is not None
    assert storage.backend.is_tombstone("old")

    await run_request(storage, logout, jar)
    assert jar.session_cookie is None
    assert storage.backend.get(new_identity) is None
    # The old identity stays dead after the interleaved invalidation.
    assert storage.backend.is_tombstone("old")
    with pytest.raises(IdentityRevokedError):
        storage.backend.guarded_set("old", '{"session": {}}', ttl=None)


async def test_backend_failure_leaves_session_and_cookie_untouched() -> None:
    backend = FailingMemoryBackend()
    storage = InMemoryServerStorage(backend)
    seed(storage, "old", {"cart": [3]})
    jar = CookieJar({"AIOHTTP_SESSION": "old"})

    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        session["user"] = "erin"
        await session.rotate_identity()
        return web.Response(text="ok")

    with pytest.raises(RuntimeError, match="backend is down"):
        await run_request(storage, handler, jar)

    # No new cookie issued; old record untouched and still live.
    assert jar.session_cookie == "old"
    payload = storage.backend.get("old")
    assert payload is not None
    assert not payload.startswith(TOMBSTONE_MARKER)
    assert storage._decoder(payload)["session"] == {"cart": [3]}


async def test_rotate_new_session_raises(
    storage: InMemoryServerStorage,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        await session.rotate_identity()
        return web.Response(text="ok")

    with pytest.raises(SessionRotationError):
        await run_request(storage, handler, CookieJar())
    # Nothing persisted, no cookie set.
    assert storage.backend.alive() == {}


async def test_cookie_storage_rotation_is_unsupported() -> None:
    from aiohttp_session import SimpleCookieStorage

    cookie_storage = SimpleCookieStorage()

    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        assert not session.new
        session["user"] = "frank"
        with pytest.raises(IdentityRotationUnsupported):
            await session.rotate_identity()
        return web.Response(text="ok")

    # Seed a cookie-backed session.
    seeded = SimpleCookieStorage()._encoder(
        {"created": int(time.time()), "session": {"existing": 1}}
    )
    jar = CookieJar({"AIOHTTP_SESSION": seeded})
    response = await run_request(cookie_storage, handler, jar, raw_cookie=True)
    assert response.status == 200


async def test_missing_migrated_field_leaves_everything_untouched(
    storage: InMemoryServerStorage,
) -> None:
    seed(storage, "old", {"other": 1})
    jar = CookieJar({"AIOHTTP_SESSION": "old"})

    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        session["user"] = "ivan"
        with pytest.raises(KeyError):
            await session.rotate_identity(fields=("cart", "user"))
        return web.Response(text="ok")

    response = await run_request(storage, handler, jar)
    assert response.status == 200
    assert jar.session_cookie == "old"
    assert not storage.backend.is_tombstone("old")


async def test_prepared_response_blocks_save_without_any_write(
    storage: InMemoryServerStorage,
) -> None:
    seed(storage, "old", {"cart": [6]})
    jar = CookieJar({"AIOHTTP_SESSION": "old"})

    async def handler(request: web.Request) -> web.Response:
        # Touching the session marks it changed; the middleware then
        # refuses to save into an already prepared response.
        await get_session(request)
        response = web.Response(text="ok")
        await response.prepare(request)
        return response

    with pytest.raises(RuntimeError, match="prepared response"):
        await run_request(storage, handler, jar)
    assert jar.session_cookie == "old"
    assert set(storage.backend.alive()) == {"old"}


async def test_rotation_preserves_absolute_deadline(
    storage: InMemoryServerStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = {"now": 1000.0}
    storage.backend.set_clock(lambda: clock["now"])
    monkeypatch.setattr(aiohttp_session.time, "time", lambda: clock["now"])

    storage.backend.guarded_set(
        "old",
        make_record(
            storage, {"cart": [4]}, created=900, expires_at=1600
        ),
        ttl=600,
    )
    jar = CookieJar({"AIOHTTP_SESSION": "old"})

    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        session["user"] = "grace"
        await session.rotate_identity()
        return web.Response(text="ok")

    response = await run_request(storage, handler, jar)
    new_identity = jar.session_cookie
    assert new_identity != "old"

    records = storage.backend.alive()
    # New record and the old tombstone share the original 1600 deadline.
    assert records[new_identity][1] == 1600
    assert records["old"][0].startswith(TOMBSTONE_MARKER)
    assert records["old"][1] == 1600
    new_record = storage._decoder(records[new_identity][0])
    assert new_record["expires_at"] == 1600

    # The cookie advertises the *remaining* lifetime (600s), not a reset.
    assert int(response.cookies["AIOHTTP_SESSION"]["max-age"]) == 600


async def test_rotation_can_reset_ttl_explicitly(
    storage: InMemoryServerStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = {"now": 1000.0}
    storage.backend.set_clock(lambda: clock["now"])
    monkeypatch.setattr(aiohttp_session.time, "time", lambda: clock["now"])

    storage.backend.guarded_set(
        "old",
        make_record(
            storage, {"cart": [5]}, created=990, expires_at=1010
        ),
        ttl=10,
    )
    jar = CookieJar({"AIOHTTP_SESSION": "old"})

    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        session["user"] = "heidi"
        await session.rotate_identity(max_age=120, reset_ttl=True)
        return web.Response(text="ok")

    await run_request(storage, handler, jar)
    records = storage.backend.alive()
    assert records[jar.session_cookie][1] == 1120
    # The tombstone still honours the old (short) remaining lifetime.
    assert records["old"][1] == 1010


async def test_invalidate_of_loaded_identity_then_rotation_is_error(
    storage: InMemoryServerStorage,
) -> None:
    seed(storage, "old", {"cart": [7]})
    jar = CookieJar({"AIOHTTP_SESSION": "old"})

    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        session.invalidate()
        with pytest.raises(SessionRotationError):
            await session.rotate_identity()
        return web.Response(text="ok")

    await run_request(storage, handler, jar)
    assert jar.session_cookie is None


async def test_invalidate_brand_new_session_writes_nothing(
    storage: InMemoryServerStorage,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        session.invalidate()
        return web.Response(text="ok")

    jar = CookieJar()
    response = await run_request(storage, handler, jar)
    assert response.status == 200
    assert jar.session_cookie is None
    assert storage.backend.alive() == {}
