import json

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from aiohttp.typedefs import Handler

from aiohttp_session import (
    SESSION_KEY,
    Session,
    SimpleCookieStorage,
    commit_session,
    get_session,
    session_middleware,
)

from .typedefs import AiohttpClient


def create_app(*handlers: tuple[str, Handler]) -> web.Application:
    middleware = session_middleware(SimpleCookieStorage())
    app = web.Application(middlewares=[middleware])
    for url, handler in handlers:
        app.router.add_route("GET", url, handler)
    return app


async def test_stream_response(aiohttp_client: AiohttpClient) -> None:
    async def stream_response(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["will_not"] = "show up"
        return web.StreamResponse()

    client = await aiohttp_client(create_app(("/stream", stream_response)))

    resp = await client.get("/stream")
    assert resp.status == 200
    assert SESSION_KEY.upper() not in resp.cookies


async def test_stream_response_commit_session(aiohttp_client: AiohttpClient) -> None:
    async def stream_response(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["will"] = "show up"
        resp = web.StreamResponse()
        await commit_session(request, resp)
        await resp.prepare(request)
        # Repeated commit after prepare is a harmless no-op.
        await commit_session(request, resp)
        try:
            session["too"] = "late"
        except RuntimeError:
            await resp.write(b"frozen")
        else:  # pragma: no cover
            await resp.write(b"not frozen")
        return resp

    client = await aiohttp_client(create_app(("/stream", stream_response)))

    resp = await client.get("/stream")
    assert resp.status == 200
    # The very first response headers already carry the session cookie.
    assert "Set-Cookie" in resp.headers
    cookie = resp.cookies[SESSION_KEY.upper()].value
    assert json.loads(cookie)["session"] == {"will": "show up"}
    # Writes after the checkpoint fail fast per the frozen-session contract.
    assert await resp.text() == "frozen"


async def test_stream_response_commit_session_save_error(
    aiohttp_client: AiohttpClient,
) -> None:
    class FailingStorage(SimpleCookieStorage):
        async def save_session(
            self,
            request: web.Request,
            response: web.StreamResponse,
            session: Session,
        ) -> None:
            raise ValueError("cannot save")

    async def stream_response(request: web.Request) -> web.StreamResponse:
        session = await get_session(request)
        session["key"] = "value"
        resp = web.StreamResponse()
        # Saving fails before any success headers are sent.
        await commit_session(request, resp)
        await resp.prepare(request)  # pragma: no cover
        return resp  # pragma: no cover

    middleware = session_middleware(FailingStorage())
    app = web.Application(middlewares=[middleware])
    app.router.add_route("GET", "/stream", stream_response)
    client = await aiohttp_client(app)

    resp = await client.get("/stream")
    assert resp.status == 500
    assert "Set-Cookie" not in resp.headers


async def test_stream_response_commit_session_unused(
    aiohttp_client: AiohttpClient,
) -> None:
    async def stream_response(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse()
        # No session was accessed: commit is a no-op, storage is untouched.
        await commit_session(request, resp)
        await resp.prepare(request)
        await resp.write(b"ok")
        return resp

    client = await aiohttp_client(create_app(("/stream", stream_response)))

    resp = await client.get("/stream")
    assert resp.status == 200
    assert SESSION_KEY.upper() not in resp.cookies
    assert await resp.text() == "ok"


async def test_bad_response_type(aiohttp_client: AiohttpClient) -> None:
    async def bad_response(request: web.Request):  # type: ignore[no-untyped-def]
        return ""

    middleware = session_middleware(SimpleCookieStorage())
    req = make_mocked_request("GET", "/")
    with pytest.raises(RuntimeError):
        await middleware(req, bad_response)


async def test_prepared_response_type(aiohttp_client: AiohttpClient) -> None:
    async def prepared_response(request: web.Request) -> web.StreamResponse:
        resp = web.Response()
        await resp.prepare(request)
        return resp

    middleware = session_middleware(SimpleCookieStorage())
    req = make_mocked_request("GET", "/")
    with pytest.raises(RuntimeError):
        await middleware(req, prepared_response)
