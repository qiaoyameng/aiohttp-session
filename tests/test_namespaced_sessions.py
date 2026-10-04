import time
import uuid
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from aiohttp.typedefs import Handler

from aiohttp_session import (
    SESSION_KEY,
    STORAGE_KEY,
    AbstractStorage,
    Session,
    SessionData,
    SessionSaveError,
    get_session,
    new_session,
    session_middleware,
    setup,
)


class CountingMemoryStorage(AbstractStorage):
    """In-memory storage counting actual loads and saves."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.store: dict[str, SessionData] = {}
        self.load_count = 0
        self.save_count = 0

    async def load_session(self, request: web.Request) -> Session:
        self.load_count += 1
        identity = self.load_cookie(request)
        data = self.store.get(identity) if identity is not None else None
        if identity is None or data is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        return Session(identity, data=data, new=False, max_age=self.max_age)

    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        self.save_count += 1
        identity = session.identity
        if session.empty:
            if identity is not None:
                self.store.pop(identity, None)
            self.save_cookie(response, "", max_age=session.max_age)
            return
        if identity is None:
            identity = uuid.uuid4().hex
        self.store[identity] = {
            "created": session.created,
            "session": dict(session._mapping),
        }
        self.save_cookie(response, identity, max_age=session.max_age)


class FailingStorage(CountingMemoryStorage):
    def __init__(self, exc: Exception, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._exc = exc

    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        self.save_count += 1
        raise self._exc


def make_storages() -> tuple[CountingMemoryStorage, CountingMemoryStorage]:
    auth = CountingMemoryStorage(cookie_name="AUTH_SESSION", max_age=60)
    prefs = CountingMemoryStorage(cookie_name="PREFS_SESSION", max_age=3600)
    return auth, prefs


def make_app(*storages: tuple[str, CountingMemoryStorage]) -> web.Application:
    app = web.Application()
    for namespace, storage in storages:
        if namespace == "default":
            setup(app, storage)
        else:
            setup(app, storage, namespace=namespace)
    return app


async def run_request(
    app: web.Application,
    handler: Handler,
    *,
    cookies: dict[str, str] | None = None,
) -> web.StreamResponse:
    headers = {}
    if cookies:
        headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())
    request = make_mocked_request("GET", "/", app=app, headers=headers)
    middleware = app.middlewares[0]
    return await middleware(request, handler)


async def test_only_used_namespace_is_loaded_and_saved() -> None:
    auth, prefs = make_storages()
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request)
        session["user"] = "bob"
        return web.Response(text="OK")

    response = await run_request(app, handler)

    assert auth.load_count == 1
    assert auth.save_count == 1
    assert prefs.load_count == 0
    assert prefs.save_count == 0
    assert "AUTH_SESSION" in response.cookies
    assert "PREFS_SESSION" not in response.cookies


async def test_both_namespaces_are_saved_independently() -> None:
    auth, prefs = make_storages()
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        auth_session = await get_session(request)
        auth_session["user"] = "bob"
        prefs_session = await get_session(request, namespace="prefs")
        prefs_session["theme"] = "dark"
        # repeated access is served from the per-namespace request cache
        assert await get_session(request) is auth_session
        assert await get_session(request, namespace="prefs") is prefs_session
        return web.Response(text="OK")

    response = await run_request(app, handler)

    assert auth.load_count == 1
    assert prefs.load_count == 1
    assert auth.save_count == 1
    assert prefs.save_count == 1
    auth_identity = response.cookies["AUTH_SESSION"].value
    prefs_identity = response.cookies["PREFS_SESSION"].value
    assert auth.store[auth_identity]["session"] == {"user": "bob"}
    assert prefs.store[prefs_identity]["session"] == {"theme": "dark"}
    # cookie configuration stays isolated per namespace
    assert response.cookies["AUTH_SESSION"]["max-age"] == "60"
    assert response.cookies["PREFS_SESSION"]["max-age"] == "3600"


async def test_request_cache_keys_are_isolated_per_namespace() -> None:
    auth, prefs = make_storages()
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        auth_session = await get_session(request)
        prefs_session = await get_session(request, namespace="prefs")
        assert request[SESSION_KEY] is auth_session
        assert request[f"{SESSION_KEY}:prefs"] is prefs_session
        assert request[STORAGE_KEY] is auth
        assert request[f"{STORAGE_KEY}:prefs"] is prefs
        return web.Response(text="OK")

    await run_request(app, handler)


async def test_invalidate_only_touches_its_own_namespace() -> None:
    auth, prefs = make_storages()
    now = int(time.time())
    auth.store["auth-id"] = {"created": now, "session": {"user": "bob"}}
    prefs.store["prefs-id"] = {"created": now, "session": {"theme": "dark"}}
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        auth_session = await get_session(request)
        assert auth_session["user"] == "bob"
        auth_session.invalidate()
        prefs_session = await get_session(request, namespace="prefs")
        assert prefs_session["theme"] == "dark"
        return web.Response(text="OK")

    response = await run_request(
        app, handler, cookies={"AUTH_SESSION": "auth-id", "PREFS_SESSION": "prefs-id"}
    )

    assert auth.load_count == 1
    assert auth.save_count == 1
    assert "auth-id" not in auth.store
    assert prefs.load_count == 1
    assert prefs.save_count == 0
    assert prefs.store["prefs-id"]["session"] == {"theme": "dark"}
    # only the invalidated namespace cookie is cleared
    assert response.cookies["AUTH_SESSION"].value == ""
    assert "PREFS_SESSION" not in response.cookies


async def test_unchanged_sessions_are_not_saved() -> None:
    auth, prefs = make_storages()
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        await get_session(request)
        await get_session(request, namespace="prefs")
        return web.Response(text="OK")

    response = await run_request(app, handler)

    assert auth.load_count == 1
    assert prefs.load_count == 1
    assert auth.save_count == 0
    assert prefs.save_count == 0
    assert not response.cookies


async def test_new_session_with_namespace() -> None:
    auth, prefs = make_storages()
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        session = await new_session(request, namespace="prefs")
        assert session.new
        assert await get_session(request, namespace="prefs") is session
        return web.Response(text="OK")

    await run_request(app, handler)

    assert auth.load_count == 0
    assert prefs.load_count == 0


async def test_get_session_with_unregistered_namespace() -> None:
    auth, prefs = make_storages()
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        with pytest.raises(RuntimeError, match="unknown"):
            await get_session(request, namespace="unknown")
        return web.Response(text="OK")

    await run_request(app, handler)


async def test_http_exception_still_saves_changed_namespaces() -> None:
    auth, prefs = make_storages()
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        prefs_session = await get_session(request, namespace="prefs")
        prefs_session["theme"] = "dark"
        raise web.HTTPFound("/other")

    with pytest.raises(web.HTTPFound):
        await run_request(app, handler)

    assert auth.save_count == 0
    assert prefs.save_count == 1


async def test_session_middleware_accepts_namespace() -> None:
    storage = CountingMemoryStorage(cookie_name="PREFS_SESSION")
    middleware = session_middleware(storage, namespace="prefs")

    async def handler(request: web.Request) -> web.Response:
        session = await get_session(request, namespace="prefs")
        session["theme"] = "dark"
        return web.Response(text="OK")

    request = make_mocked_request("GET", "/")
    response = await middleware(request, handler)

    assert storage.save_count == 1
    assert "PREFS_SESSION" in response.cookies


def test_setup_installs_a_single_middleware() -> None:
    auth, prefs = make_storages()
    app = make_app(("default", auth), ("prefs", prefs))
    assert len(app.middlewares) == 1


def test_duplicate_namespace_is_rejected() -> None:
    app = web.Application()
    setup(app, CountingMemoryStorage(cookie_name="A"))
    with pytest.raises(RuntimeError, match="already registered"):
        setup(app, CountingMemoryStorage(cookie_name="B"))


def test_conflicting_cookie_configuration_is_rejected() -> None:
    app = web.Application()
    setup(app, CountingMemoryStorage(cookie_name="S", path="/"))
    with pytest.raises(ValueError, match="conflicts"):
        setup(app, CountingMemoryStorage(cookie_name="S", path="/"), namespace="prefs")


def test_same_cookie_name_with_different_path_is_allowed() -> None:
    app = web.Application()
    setup(app, CountingMemoryStorage(cookie_name="S", path="/"))
    setup(app, CountingMemoryStorage(cookie_name="S", path="/prefs"), namespace="prefs")
    assert len(app.middlewares) == 1


async def test_save_failure_reports_write_boundary_and_sends_no_cookies() -> None:
    auth = CountingMemoryStorage(cookie_name="AUTH_SESSION")
    prefs = FailingStorage(ValueError("boom"), cookie_name="PREFS_SESSION")
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        auth_session = await get_session(request)
        auth_session["user"] = "bob"
        prefs_session = await get_session(request, namespace="prefs")
        prefs_session["theme"] = "dark"
        return web.Response(text="OK")

    request = make_mocked_request("GET", "/", app=app)
    middleware = app.middlewares[0]
    with pytest.raises(SessionSaveError) as exc_info:
        await middleware(request, handler)

    exc = exc_info.value
    assert exc.failed == "prefs"
    assert exc.saved == ("default",)
    assert isinstance(exc.__cause__, ValueError)
    # the server-side write boundary: auth was persisted before the failure
    assert auth.save_count == 1
    assert len(auth.store) == 1
    assert prefs.save_count == 1
    assert not prefs.store
    # the response was discarded, so no partial set of cookies is sent


async def test_save_stops_at_first_failure() -> None:
    auth = FailingStorage(ValueError("boom"), cookie_name="AUTH_SESSION")
    prefs = CountingMemoryStorage(cookie_name="PREFS_SESSION")
    app = make_app(("default", auth), ("prefs", prefs))

    async def handler(request: web.Request) -> web.Response:
        auth_session = await get_session(request)
        auth_session["user"] = "bob"
        prefs_session = await get_session(request, namespace="prefs")
        prefs_session["theme"] = "dark"
        return web.Response(text="OK")

    request = make_mocked_request("GET", "/", app=app)
    middleware = app.middlewares[0]
    with pytest.raises(SessionSaveError) as exc_info:
        await middleware(request, handler)

    assert exc_info.value.failed == "default"
    assert exc_info.value.saved == ()
    # registration order is fixed: prefs is never attempted after auth fails
    assert prefs.save_count == 0
    assert not prefs.store
