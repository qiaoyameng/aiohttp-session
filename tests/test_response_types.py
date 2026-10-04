from unittest.mock import MagicMock

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from aiohttp.typedefs import Handler
from multidict import CIMultiDict

from aiohttp_session import (
    SESSION_KEY,
    SimpleCookieStorage,
    get_session,
    session_middleware,
)

from .typedefs import AiohttpClient


def _mocked_request() -> web.Request:
    req = make_mocked_request("GET", "/")
    writer = MagicMock()

    async def write_headers(
        status: str, headers: CIMultiDict[str]
    ) -> None:
        return None

    writer.write_headers = write_headers
    req._payload_writer = writer
    return req


def create_app(*handlers: tuple[str, Handler]) -> web.Application:
    middleware = session_middleware(SimpleCookieStorage())
    app = web.Application(middlewares=[middleware])
    for url, handler in handlers:
        app.router.add_route("GET", url, handler)
    return app


async def test_stream_response(aiohttp_client: AiohttpClient) -> None:
    async def stream_response(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["will"] = "show up"
        return web.StreamResponse()

    client = await aiohttp_client(create_app(("/stream", stream_response)))

    resp = await client.get("/stream")
    assert resp.status == 200
    # Streaming responses save the session at the prepare checkpoint,
    # so the cookie arrives with the first chunk of headers.
    assert resp.cookies[SESSION_KEY.upper()].value


async def test_bad_response_type(aiohttp_client: AiohttpClient) -> None:
    async def bad_response(request: web.Request):  # type: ignore[no-untyped-def]
        return ""

    middleware = session_middleware(SimpleCookieStorage())
    req = make_mocked_request("GET", "/")
    with pytest.raises(RuntimeError):
        await middleware(req, bad_response)


async def test_checkpoint_saves_session_before_headers() -> None:
    """A session changed before prepare() is saved as headers serialize."""

    async def prepared_response(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["before"] = "prepare"
        resp = web.Response()
        await resp.prepare(request)
        return resp

    middleware = session_middleware(SimpleCookieStorage())
    req = _mocked_request()
    response = await middleware(req, prepared_response)
    assert response.prepared
    # The checkpoint persisted the session at the writer hook.
    assert req["aiohttp_session_committed"] is True


async def test_prepared_changed_uncommitted_response_raises() -> None:
    """A changed session on an already-prepared response must fail loudly."""

    middleware = session_middleware(SimpleCookieStorage())
    req = _mocked_request()
    req["aiohttp_session_storage"] = SimpleCookieStorage()

    session = await get_session(req)
    session["must_not_be"] = "lost silently"

    response = web.StreamResponse()
    response._payload_writer = req._payload_writer
    response._prepared = True

    async def handler(request: web.Request) -> web.StreamResponse:
        return response

    with pytest.raises(RuntimeError):
        await middleware(req, handler)


async def test_prepared_unchanged_session_is_returned() -> None:
    async def prepared_response(request: web.Request) -> web.StreamResponse:
        await get_session(request)
        resp = web.Response()
        await resp.prepare(request)
        return resp

    middleware = session_middleware(SimpleCookieStorage())
    req = _mocked_request()
    response = await middleware(req, prepared_response)
    assert response.prepared
