"""Explicit streaming session checkpoint support.

A session loaded and changed before the first ``response.prepare()`` is
saved into the very first header batch of a streaming response.  Once
committed the session is frozen, so late mutations raise immediately
instead of being silently dropped when the connection closes.
"""

import pytest
from aiohttp import web
from aiohttp.typedefs import Handler
from multidict import CIMultiDict

from aiohttp_session import (
    SESSION_KEY,
    SimpleCookieStorage,
    commit_session,
    get_session,
    session_middleware,
)

from .typedefs import AiohttpClient


class CountingStorage(SimpleCookieStorage):
    """SimpleCookieStorage which records load/save invocations."""

    def __init__(self) -> None:
        super().__init__()
        self.loads = 0
        self.saves = 0

    async def load_session(self, request: web.Request):  # type: ignore[override]
        self.loads += 1
        return await super().load_session(request)

    async def save_session(self, request, response, session):  # type: ignore[override]
        self.saves += 1
        return await super().save_session(request, response, session)


class FailingStorage(CountingStorage):
    async def save_session(self, request, response, session):  # type: ignore[override]
        self.saves += 1
        raise OSError("storage is down")


def create_app(
    storage: SimpleCookieStorage, *handlers: tuple[str, Handler]
) -> web.Application:
    middleware = session_middleware(storage)
    app = web.Application(middlewares=[middleware])
    for url, handler in handlers:
        app.router.add_route("GET", url, handler)
    return app


async def test_streaming_cookie_shipped_before_body(
    aiohttp_client: AiohttpClient,
) -> None:
    """The first headers batch carries the cookie; later writes fail."""

    async def stream(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["before"] = "prepare"

        response = web.StreamResponse()
        await response.prepare(request)
        # The checkpoint ran inside prepare(): the cookie is committed and
        # the session is frozen.  Late mutations raise immediately rather
        # than being silently lost at connection end.
        assert SESSION_KEY.upper() in response.cookies
        with pytest.raises(RuntimeError):
            session["after"] = "prepare"
        with pytest.raises(RuntimeError):
            del session["before"]
        with pytest.raises(RuntimeError):
            session.invalidate()
        with pytest.raises(RuntimeError):
            session.changed()
        await response.write(b"data")
        return response

    client = await aiohttp_client(create_app(CountingStorage(), ("/", stream)))

    resp = await client.get("/")
    assert resp.status == 200
    assert SESSION_KEY.upper() in resp.cookies
    assert await resp.text() == "data"


async def test_streaming_save_failure_prevents_success_headers(
    aiohttp_client: AiohttpClient,
) -> None:
    """A storage error from prepare() must not leak a success response."""

    async def stream(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["before"] = "prepare"
        response = web.StreamResponse()
        with pytest.raises(OSError):
            await response.prepare(request)
        return response

    storage = FailingStorage()
    client = await aiohttp_client(create_app(storage, ("/", stream)))

    resp = await client.get("/")
    assert storage.saves == 1
    assert resp.status == 500
    assert SESSION_KEY.upper() not in resp.cookies


async def test_explicit_commit_is_idempotent(aiohttp_client: AiohttpClient) -> None:
    """commit_session() persists once and freezes the session."""

    async def stream(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["before"] = "commit"

        response = web.StreamResponse()
        await commit_session(request, response)
        with pytest.raises(RuntimeError):
            session["after"] = "commit"
        # prepare() triggers the checkpoint again but must not save twice.
        await response.prepare(request)
        await response.write(b"chunk")
        return response

    storage = CountingStorage()
    client = await aiohttp_client(create_app(storage, ("/", stream)))

    resp = await client.get("/")
    assert resp.status == 200
    assert SESSION_KEY.upper() in resp.cookies
    assert storage.saves == 1
    assert await resp.text() == "chunk"


def _mocked_prepare_request() -> tuple[web.Request, dict[str, CIMultiDict[str]]]:
    """Request with a writer that records the headers prepare() serializes."""
    from unittest.mock import MagicMock

    from aiohttp.test_utils import make_mocked_request

    request = make_mocked_request("GET", "/")
    writer = MagicMock()
    serialized: dict[str, CIMultiDict[str]] = {}

    async def write_headers(
        status_line: str, headers: CIMultiDict[str]
    ) -> None:
        serialized["headers"] = headers

    writer.write_headers = write_headers
    request._payload_writer = writer
    return request, serialized


async def test_checkpoint_cookie_in_first_prepared_headers() -> None:
    """Socket-free check: cookie lands in the headers prepare() serializes."""

    middleware = session_middleware(CountingStorage())
    request, serialized = _mocked_prepare_request()

    async def handler(req: web.Request) -> web.StreamResponse:
        session = await get_session(req)
        session["before"] = "prepare"
        response = web.StreamResponse()
        await response.prepare(req)
        with pytest.raises(RuntimeError):
            session["after"] = "prepare"
        return response

    await middleware(request, handler)

    headers = serialized["headers"]
    set_cookie = headers.getall("Set-Cookie", ())
    assert any(value.startswith("AIOHTTP_SESSION=") for value in set_cookie)


async def test_explicit_commit_without_session_raises(
    aiohttp_client: AiohttpClient,
) -> None:
    async def stream(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse()
        with pytest.raises(RuntimeError):
            await commit_session(request, response)
        return response

    client = await aiohttp_client(create_app(CountingStorage(), ("/", stream)))
    await client.get("/")


async def test_unchanged_session_is_not_saved(aiohttp_client: AiohttpClient) -> None:
    async def stream(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        assert session.get("missing") is None
        response = web.StreamResponse()
        await response.prepare(request)
        await response.write(b"ok")
        return response

    storage = CountingStorage()
    client = await aiohttp_client(create_app(storage, ("/", stream)))

    resp = await client.get("/")
    assert resp.status == 200
    assert storage.loads == 1
    assert storage.saves == 0
    assert SESSION_KEY.upper() not in resp.cookies


async def test_request_without_session_skips_storage(
    aiohttp_client: AiohttpClient,
) -> None:
    async def stream(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse()
        await response.prepare(request)
        await response.write(b"no session")
        return response

    storage = CountingStorage()
    client = await aiohttp_client(create_app(storage, ("/", stream)))

    resp = await client.get("/")
    assert resp.status == 200
    assert storage.loads == 0
    assert storage.saves == 0
    assert SESSION_KEY.upper() not in resp.cookies


async def test_prepare_inside_handler_is_committed(
    aiohttp_client: AiohttpClient,
) -> None:
    """Preparing inside the handler still commits at the header checkpoint."""

    async def stream(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["before"] = "prepare"
        response = web.StreamResponse()
        await response.prepare(request)
        await response.write(b"chunk")
        return response

    storage = CountingStorage()
    client = await aiohttp_client(create_app(storage, ("/", stream)))

    resp = await client.get("/")
    assert resp.status == 200
    assert SESSION_KEY.upper() in resp.cookies
    assert storage.saves == 1
    assert await resp.text() == "chunk"
