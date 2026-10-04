import uuid
from collections.abc import Awaitable, Callable
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
    setup,
)


class CountingMemoryStorage(AbstractStorage):
    """In-memory storage which counts actual loads and saves."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.store: dict[str, SessionData] = {}
        self.load_count = 0
        self.save_count = 0

    async def load_session(self, request: web.Request) -> Session:
        self.load_count += 1
        token = self.load_cookie(request)
        data = self.store.get(token) if token is not None else None
        if token is None or data is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        return Session(token, data=data, new=False, max_age=self.max_age)

    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        self.save_count += 1
        if session.empty:
            if session.identity is not None:
                self.store.pop(session.identity, None)
            self.save_cookie(response, "", max_age=session.max_age)
            return
        token = session.identity
        if token is None:
            token = uuid.uuid4().hex
            session.set_new_identity(token)
        self.store[token] = self._get_session_data(session)
        self.save_cookie(response, token, max_age=session.max_age)


class FailingStorage(CountingMemoryStorage):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.fail_on_save = False

    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        if self.fail_on_save:
            raise RuntimeError("storage is broken")
        await super().save_session(request, response, session)


def make_storages() -> tuple[CountingMemoryStorage, CountingMemoryStorage]:
    login = CountingMemoryStorage(cookie_name="LOGIN_SESSION", max_age=300)
    prefs = CountingMemoryStorage(cookie_name="PREFS_SESSION", max_age=30 * 24 * 3600)
    return login, prefs


def make_app(
    login: CountingMemoryStorage, prefs: CountingMemoryStorage
) -> web.Application:
    app = web.Application()
    setup(app, login)
    setup(app, prefs, namespace="prefs")
    return app


async def run_request(
    app: web.Application,
    handler: Handler,
    cookies: dict[str, str] | None = None,
) -> tuple[web.Request, web.StreamResponse]:
    headers = {}
    if cookies:
        headers["Cookie"] = "; ".join(
            f"{name}={value}" for name, value in cookies.items()
        )
    request = make_mocked_request("GET", "/", headers=headers)
    middleware = app.middlewares[0]
    response = await middleware(request, handler)
    assert isinstance(response, web.Response)
    return request, response


def response_cookies(response: web.StreamResponse) -> dict[str, str]:
    return {name: morsel.value for name, morsel in response.cookies.items()}


async def test_default_namespace_keeps_legacy_request_keys() -> None:
    login, prefs = make_storages()
    app = make_app(login, prefs)

    async def handler(request: web.Request) -> web.StreamResponse:
        assert SESSION_KEY not in request
        assert request[STORAGE_KEY] is login
        assert request[f"{STORAGE_KEY}:prefs"] is prefs
        session = await get_session(request)
        prefs_session = await get_session(request, namespace="prefs")
        assert request[SESSION_KEY] is session
        assert request[f"{SESSION_KEY}:prefs"] is prefs_session
        assert prefs_session is not session
        return web.Response(body=b"OK")

    await run_request(app, handler)


async def test_only_one_namespace_used() -> None:
    login, prefs = make_storages()
    app = make_app(login, prefs)

    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["user_id"] = 42
        return web.Response(body=b"OK")

    _, response = await run_request(app, handler)

    assert login.load_count == 1
    assert login.save_count == 1
    # the prefs session was never touched: no load, no save, no cookie
    assert prefs.load_count == 0
    assert prefs.save_count == 0
    assert response_cookies(response) == {"LOGIN_SESSION": next(iter(login.store))}
    assert len(login.store) == 1
    assert not prefs.store


async def test_both_namespaces_modified() -> None:
    login, prefs = make_storages()
    app = make_app(login, prefs)

    async def handler(request: web.Request) -> web.StreamResponse:
        login_session = await get_session(request)
        # repeated access must hit the request cache, not the storage
        assert await get_session(request) is login_session
        prefs_session = await get_session(request, namespace="prefs")
        assert await get_session(request, namespace="prefs") is prefs_session
        login_session["user_id"] = 42
        prefs_session["theme"] = "dark"
        return web.Response(body=b"OK")

    _, response = await run_request(app, handler)

    assert login.load_count == 1
    assert login.save_count == 1
    assert prefs.load_count == 1
    assert prefs.save_count == 1
    cookies = response_cookies(response)
    assert set(cookies) == {"LOGIN_SESSION", "PREFS_SESSION"}
    assert cookies["LOGIN_SESSION"] != cookies["PREFS_SESSION"]

    # follow-up request loads both sessions independently
    seen: dict[str, Any] = {}

    async def second_handler(request: web.Request) -> web.StreamResponse:
        login_session = await get_session(request)
        prefs_session = await get_session(request, namespace="prefs")
        seen["user_id"] = login_session.get("user_id")
        seen["theme"] = prefs_session.get("theme")
        return web.Response(body=b"OK")

    await run_request(app, second_handler, cookies)

    assert seen == {"user_id": 42, "theme": "dark"}
    assert login.load_count == 2
    assert prefs.load_count == 2
    # nothing changed, so nothing was saved again
    assert login.save_count == 1
    assert prefs.save_count == 1


async def test_invalidate_one_namespace() -> None:
    login, prefs = make_storages()
    app = make_app(login, prefs)

    async def create_handler(request: web.Request) -> web.StreamResponse:
        login_session = await get_session(request)
        login_session["user_id"] = 42
        prefs_session = await get_session(request, namespace="prefs")
        prefs_session["theme"] = "dark"
        return web.Response(body=b"OK")

    _, response = await run_request(app, create_handler)
    cookies = response_cookies(response)
    prefs_token = cookies["PREFS_SESSION"]
    assert prefs_token in prefs.store

    async def invalidate_handler(request: web.Request) -> web.StreamResponse:
        prefs_session = await get_session(request, namespace="prefs")
        prefs_session.invalidate()
        login_session = await get_session(request)
        login_session["last_page"] = "/dashboard"
        return web.Response(body=b"OK")

    _, response = await run_request(app, invalidate_handler, cookies)

    # the invalidated session was wiped server-side and its cookie deleted
    assert prefs_token not in prefs.store
    assert response.cookies["PREFS_SESSION"].value == ""
    assert response.cookies["PREFS_SESSION"]["expires"]
    # the login session kept its data and identity
    assert response.cookies["LOGIN_SESSION"].value == cookies["LOGIN_SESSION"]
    token = cookies["LOGIN_SESSION"]
    assert login.store[token]["session"] == {"user_id": 42, "last_page": "/dashboard"}
    assert login.save_count == 2
    assert prefs.save_count == 2


async def test_new_session_with_namespace() -> None:
    login, prefs = make_storages()
    app = make_app(login, prefs)

    async def handler(request: web.Request) -> web.StreamResponse:
        prefs_session = await new_session(request, namespace="prefs")
        assert prefs_session.new
        prefs_session["theme"] = "light"
        assert await get_session(request, namespace="prefs") is prefs_session
        return web.Response(body=b"OK")

    _, response = await run_request(app, handler)

    assert prefs.load_count == 0
    assert prefs.save_count == 1
    assert login.save_count == 0
    assert set(response_cookies(response)) == {"PREFS_SESSION"}


async def test_middleware_installed_once_for_multiple_namespaces() -> None:
    login, prefs = make_storages()
    app = make_app(login, prefs)
    assert len(app.middlewares) == 1

    async def handler(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["count"] = session.get("count", 0) + 1
        return web.Response(body=b"OK")

    await run_request(app, handler)
    # a single middleware pass means a single save per changed session
    assert login.save_count == 1


async def test_duplicate_namespace_rejected() -> None:
    login, prefs = make_storages()
    app = web.Application()
    setup(app, login)
    with pytest.raises(RuntimeError, match="already set up"):
        setup(app, prefs)
    with pytest.raises(RuntimeError, match="already set up"):
        setup(app, CountingMemoryStorage(cookie_name="OTHER"), namespace="default")


async def test_conflicting_cookie_configuration_rejected() -> None:
    app = web.Application()
    setup(app, CountingMemoryStorage(cookie_name="SHARED"))
    # same cookie name, domain and overlapping path would overwrite cookies
    with pytest.raises(ValueError, match="overwrite"):
        setup(app, CountingMemoryStorage(cookie_name="SHARED"), namespace="prefs")
    with pytest.raises(ValueError, match="overwrite"):
        setup(
            app,
            CountingMemoryStorage(cookie_name="SHARED", path="/app"),
            namespace="prefs",
        )
    # different cookie name or non-overlapping path is fine
    setup(app, CountingMemoryStorage(cookie_name="OTHER"), namespace="prefs")
    setup(
        app,
        CountingMemoryStorage(cookie_name="SHARED", domain="example.com"),
        namespace="sub",
    )


async def test_setup_rejects_non_storage() -> None:
    app = web.Application()
    with pytest.raises(RuntimeError):
        setup(app, object())  # type: ignore[arg-type]


async def test_get_session_unknown_namespace_raises() -> None:
    login, _ = make_storages()
    app = web.Application()
    setup(app, login)

    async def handler(request: web.Request) -> web.StreamResponse:
        with pytest.raises(RuntimeError, match="Install aiohttp_session"):
            await get_session(request, namespace="missing")
        return web.Response(body=b"OK")

    await run_request(app, handler)


async def test_save_failure_reports_write_boundary() -> None:
    login = CountingMemoryStorage(cookie_name="LOGIN_SESSION")
    prefs = FailingStorage(cookie_name="PREFS_SESSION")
    app = make_app(login, prefs)
    prefs.fail_on_save = True

    async def handler(request: web.Request) -> web.StreamResponse:
        (await get_session(request))["user_id"] = 42
        (await get_session(request, namespace="prefs"))["theme"] = "dark"
        return web.Response(body=b"OK")

    request = make_mocked_request("GET", "/")
    middleware = app.middlewares[0]
    with pytest.raises(SessionSaveError) as exc_info:
        await middleware(request, handler)

    exc = exc_info.value
    # fixed save order: login (registered first) already wrote server-side,
    # prefs failed, nothing was left pending
    assert exc.saved == ("default",)
    assert exc.failed == "prefs"
    assert exc.pending == ()
    # the login storage really did persist its data
    assert login.save_count == 1
    assert len(login.store) == 1
    assert prefs.save_count == 0


async def test_save_failure_skips_pending_namespaces() -> None:
    login = FailingStorage(cookie_name="LOGIN_SESSION")
    prefs = CountingMemoryStorage(cookie_name="PREFS_SESSION")
    app = make_app(login, prefs)
    login.fail_on_save = True

    async def handler(request: web.Request) -> web.StreamResponse:
        (await get_session(request))["user_id"] = 42
        (await get_session(request, namespace="prefs"))["theme"] = "dark"
        return web.Response(body=b"OK")

    request = make_mocked_request("GET", "/")
    middleware = app.middlewares[0]
    with pytest.raises(SessionSaveError) as exc_info:
        await middleware(request, handler)

    exc = exc_info.value
    # login is saved first and fails, so prefs is never even attempted
    assert exc.saved == ()
    assert exc.failed == "default"
    assert exc.pending == ("prefs",)
    assert prefs.save_count == 0
    assert not prefs.store


async def test_save_failure_sends_no_partial_cookies() -> None:
    login = CountingMemoryStorage(cookie_name="LOGIN_SESSION")
    prefs = FailingStorage(cookie_name="PREFS_SESSION")
    app = web.Application()
    setup(app, login)
    setup(app, prefs, namespace="prefs")
    prefs.fail_on_save = True

    responses: list[web.Response] = []

    async def handler(request: web.Request) -> web.StreamResponse:
        (await get_session(request))["user_id"] = 42
        (await get_session(request, namespace="prefs"))["theme"] = "dark"
        response = web.Response(body=b"OK")
        responses.append(response)
        return response

    request = make_mocked_request("GET", "/")
    middleware = app.middlewares[0]
    with pytest.raises(SessionSaveError):
        await middleware(request, handler)

    # login was persisted server-side, but the handler's response carries
    # no cookies at all: cookie application happens only after all saves
    assert len(login.store) == 1
    assert len(responses) == 1
    assert not responses[0].cookies
