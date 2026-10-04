"""User sessions for aiohttp.web."""

__version__ = "2.12.1"

import abc
import json
import time
from collections.abc import Callable, Iterator, MutableMapping
from http.cookies import SimpleCookie
from typing import Any, TypedDict, cast

from multidict import CIMultiDict

from aiohttp import hdrs, web
from aiohttp.typedefs import Handler, Middleware


class _CookieParams(TypedDict, total=False):
    domain: str | None
    max_age: int | None
    path: str
    secure: bool | None
    httponly: bool
    samesite: str | None
    expires: str


class SessionData(TypedDict, total=False):
    created: int
    session: dict[str, Any]


class Session(MutableMapping[str, Any]):

    """Session dict-like object."""

    def __init__(
        self,
        identity: Any | None,
        *,
        data: SessionData | None,
        new: bool,
        max_age: int | None = None,
    ) -> None:
        self._changed: bool = False
        self._frozen: bool = False
        self._mapping: dict[str, Any] = {}
        self._identity = identity if data != {} else None
        self._new = new if data != {} else True
        self._max_age = max_age
        created = data.get("created", None) if data else None
        session_data = data.get("session", None) if data else None
        now = int(time.time())
        age = now - created if created else now
        if max_age is not None and age > max_age:
            session_data = None
        if self._new or created is None:
            self._created = now
        else:
            self._created = created

        if session_data is not None:
            self._mapping.update(session_data)

    def __repr__(self) -> str:
        return "<{} [new:{}, changed:{}, created:{}] {!r}>".format(
            self.__class__.__name__,
            self.new,
            self._changed,
            self.created,
            self._mapping,
        )

    @property
    def new(self) -> bool:
        return self._new

    @property
    def identity(self) -> Any | None:
        return self._identity

    @property
    def created(self) -> int:
        return self._created

    @property
    def empty(self) -> bool:
        return not bool(self._mapping)

    @property
    def max_age(self) -> int | None:
        return self._max_age

    @max_age.setter
    def max_age(self, value: int | None) -> None:
        self._max_age = value

    def changed(self) -> None:
        self._check_frozen()
        self._changed = True

    def invalidate(self) -> None:
        self._check_frozen()
        self._changed = True
        self._mapping = {}

    def _check_frozen(self) -> None:
        if self._frozen:
            raise RuntimeError(
                "Session is frozen: the response has been committed and "
                "session changes can no longer be persisted. "
                "Call commit_session(request, response) explicitly "
                "before mutating the session of a streaming response."
            )

    def freeze(self) -> None:
        """Mark the session as read-only.

        Once the response headers are about to be sent there is no way to
        persist further session changes.  Freezing the session turns a
        silently lost mutation into an immediate error.
        """
        self._frozen = True

    @property
    def frozen(self) -> bool:
        return self._frozen

    def set_new_identity(self, identity: Any | None) -> None:
        if not self._new:
            raise RuntimeError("Can't change identity for a session which is not new")
        self._check_frozen()

        self._identity = identity

    def __len__(self) -> int:
        return len(self._mapping)

    def __iter__(self) -> Iterator[str]:
        return iter(self._mapping)

    def __contains__(self, key: object) -> bool:
        return key in self._mapping

    def __getitem__(self, key: str) -> Any:
        return self._mapping[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._check_frozen()
        self._mapping[key] = value
        self._changed = True
        self._created = int(time.time())

    def __delitem__(self, key: str) -> None:
        self._check_frozen()
        del self._mapping[key]
        self._changed = True
        self._created = int(time.time())


SESSION_KEY = "aiohttp_session"
STORAGE_KEY = "aiohttp_session_storage"
COMMITTED_KEY = "aiohttp_session_committed"
CHECKPOINT_DONE_KEY = "aiohttp_session_checkpoint_done"
CHECKPOINT_INSTALLED_KEY = "aiohttp_session_checkpoint_installed"


async def get_session(request: web.Request) -> Session:
    session = request.get(SESSION_KEY)
    if session is None:
        storage = request.get(STORAGE_KEY)
        if storage is None:
            raise RuntimeError(
                "Install aiohttp_session middleware " "in your aiohttp.web.Application"
            )

        session = await storage.load_session(request)
        if not isinstance(session, Session):
            raise RuntimeError(
                "Installed {!r} storage should return session instance "
                "on .load_session() call, got {!r}.".format(storage, session)
            )
        request[SESSION_KEY] = session
        _install_checkpoint(request)
    return session


async def new_session(request: web.Request) -> Session:
    storage = request.get(STORAGE_KEY)
    if storage is None:
        raise RuntimeError(
            "Install aiohttp_session middleware " "in your aiohttp.web.Application"
        )

    session = await storage.new_session()
    if not isinstance(session, Session):
        raise RuntimeError(
            "Installed {!r} storage should return session instance "
            "on .load_session() call, got {!r}.".format(storage, session)
        )
    request[SESSION_KEY] = session
    _install_checkpoint(request)
    return session


async def commit_session(
    request: web.Request, response: web.StreamResponse | None = None
) -> Session:
    """Persist the request session before the response headers are sent.

    This is the explicit streaming-session checkpoint.  The
    :func:`session_middleware` calls it automatically for every response
    (regular :class:`~aiohttp.web.Response`, raised
    :class:`~aiohttp.web.HTTPException` and chunked
    :class:`~aiohttp.web.StreamResponse`) right before the first chunk of
    headers is written to the wire, so handlers normally do not need to
    call it.

    Call it explicitly from a streaming handler when you need the session
    frozen at a specific point, passing the response the ``Set-Cookie``
    header should be attached to::

        resp = web.StreamResponse()
        await commit_session(request, resp)
        await resp.prepare(request)

    If *response* is omitted the automatic checkpoint (or a later explicit
    call carrying the response) performs the actual save.

    The call is idempotent: the session is saved at most once.  Once
    committed the session is frozen and any further mutation raises
    :class:`RuntimeError` immediately, instead of being silently lost when
    the connection closes.

    Storage is never accessed unless :func:`get_session` or
    :func:`new_session` was awaited and the session is marked as changed.
    """
    session = request.get(SESSION_KEY)
    if session is None:
        raise RuntimeError(
            "Cannot commit session: get_session() or new_session() "
            "was never awaited for this request"
        )

    if request.get(COMMITTED_KEY):
        return session

    if response is not None and session._changed:
        storage = request[STORAGE_KEY]
        await storage.save_session(request, response, session)

    if response is not None:
        request[COMMITTED_KEY] = True
        request[CHECKPOINT_DONE_KEY] = True
        session.freeze()
    return session


class _HeaderCookieResponse:
    """Response stand-in used by the streaming checkpoint.

    ``AbstractStorage.save_session`` only calls ``set_cookie`` /
    ``del_cookie`` on the response.  At the checkpoint the real
    response's cookie jar has already been serialized, so cookies are
    rendered directly into the outgoing header mapping that is about to
    be written, using the same ``SimpleCookie`` formatting aiohttp uses.
    """

    def __init__(self, headers: CIMultiDict[str]) -> None:
        self._headers = headers

    @staticmethod
    def _render(
        name: str,
        value: str,
        *,
        expires: str | None,
        domain: str | None,
        max_age: int | str | None,
        path: str,
        secure: bool | None,
        httponly: bool | None,
        version: str | None,
        samesite: str | None,
        partitioned: bool | None,
        headers: CIMultiDict[str],
    ) -> None:
        cookie = SimpleCookie()
        cookie[name] = value
        morsel = cookie[name]
        if expires is not None:
            morsel["expires"] = expires
        if domain is not None:
            morsel["domain"] = domain
        if max_age is not None:
            morsel["max-age"] = str(max_age)
        morsel["path"] = path
        if secure is not None:
            morsel["secure"] = secure
        if httponly is not None:
            morsel["httponly"] = httponly
        if version is not None:
            morsel["version"] = version
        if samesite is not None:
            morsel["samesite"] = samesite
        if partitioned is not None:
            morsel["partitioned"] = partitioned
        headers.add(hdrs.SET_COOKIE, morsel.output(header="")[1:])

    def set_cookie(
        self,
        name: str,
        value: str,
        *,
        expires: str | None = None,
        domain: str | None = None,
        max_age: int | str | None = None,
        path: str = "/",
        secure: bool | None = None,
        httponly: bool | None = None,
        version: str | None = None,
        samesite: str | None = None,
        partitioned: bool | None = None,
    ) -> None:
        self._render(
            name,
            value,
            expires=expires,
            domain=domain,
            max_age=max_age,
            path=path,
            secure=secure,
            httponly=httponly,
            version=version,
            samesite=samesite,
            partitioned=partitioned,
            headers=self._headers,
        )

    def del_cookie(
        self,
        name: str,
        *,
        domain: str | None = None,
        path: str = "/",
        secure: bool | None = None,
        httponly: bool | None = None,
        samesite: str | None = None,
    ) -> None:
        self.set_cookie(
            name,
            "",
            max_age=0,
            expires="Thu, 01 Jan 1970 00:00:00 GMT",
            domain=domain,
            path=path,
            secure=secure,
            httponly=httponly,
            samesite=samesite,
        )


def _install_checkpoint(request: web.Request) -> None:
    """Save+freeze the session before the first response headers ship.

    The per-request ``StreamWriter.write_headers`` method is wrapped so the
    session is saved right before the status line and headers are
    serialized (the wrapped call runs before the base implementation).
    This is ahead of any byte reaching the transport, regardless of
    whether ``response.prepare(request)`` is awaited inside the handler
    or later by the server.  A raised storage error therefore aborts the
    response before success headers leave the server, and the
    ``Set-Cookie`` header reaches the very first batch.

    The checkpoint writes the cookie straight into the outgoing header
    mapping via :class:`_HeaderCookieResponse`, so it does not depend on
    when the response's own cookie jar is serialized.

    Nothing is installed (and no storage is touched) unless the handler
    loads a session -- :func:`get_session` installs the checkpoint lazily.
    """
    if request.get(CHECKPOINT_INSTALLED_KEY):
        return
    request[CHECKPOINT_INSTALLED_KEY] = True

    writer = getattr(request, "_payload_writer", None)
    if writer is None:
        return

    captured_response: web.StreamResponse | None = None
    original_prepare_hook = request._prepare_hook

    async def prepare_hook(response: web.StreamResponse) -> None:
        nonlocal captured_response
        captured_response = response
        await original_prepare_hook(response)

    # object.__setattr__ avoids the DEBUG-build warning for attributes
    # outside Request.ATTRS.
    object.__setattr__(request, "_prepare_hook", prepare_hook)

    original_write_headers = writer.write_headers

    async def write_headers(
        status_line: str, headers: CIMultiDict[str]
    ) -> None:
        session = request.get(SESSION_KEY)
        if (
            session is not None
            and not request.get(CHECKPOINT_DONE_KEY)
            and not isinstance(captured_response, web.WebSocketResponse)
        ):
            # Run the checkpoint at most once per request: if the storage
            # raises here aiohttp builds an error response and prepares it
            # again, which must not trigger a second (failing) save.
            request[CHECKPOINT_DONE_KEY] = True
            try:
                await commit_session(request, _HeaderCookieResponse(headers))
            except BaseException:
                session.freeze()
                raise
        await original_write_headers(status_line, headers)

    writer.write_headers = write_headers  # type: ignore[method-assign]


def session_middleware(storage: "AbstractStorage") -> Middleware:
    if not isinstance(storage, AbstractStorage):
        raise RuntimeError(f"Expected AbstractStorage got {storage}")

    @web.middleware
    async def factory(request: web.Request, handler: Handler) -> web.StreamResponse:
        request[STORAGE_KEY] = storage
        raise_response = False
        # TODO aiohttp 4:
        # Remove Union from response, and drop the raise_response variable
        response: web.StreamResponse | web.HTTPException
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            response = exc
            raise_response = True
        if not isinstance(response, (web.StreamResponse, web.HTTPException)):
            raise RuntimeError(f"Expect response, not {type(response)!r}")
        if response.prepared:
            # The checkpoint runs inside prepare(); if it did, the
            # session is committed already.  A changed but uncommitted
            # session here means prepare ran on a writer without the hook
            # (or storage was bypassed) -- the headers are gone, so fail
            # loudly rather than silently dropping the change.  WebSocket
            # handshakes never carried a session cookie historically, so
            # keep that permissive behavior.
            session = request.get(SESSION_KEY)
            if (
                session is not None
                and session._changed
                and not request.get(COMMITTED_KEY)
                and not isinstance(response, web.WebSocketResponse)
            ):
                raise RuntimeError(
                    "Cannot save session data into an already prepared "
                    "response"
                )
            return response
        # Unprepared response: the server (or the handler) will call
        # prepare(), and the lazily installed checkpoint commits the
        # session then.  Plain responses are normally committed at the
        # same point; save eagerly here as well to keep the historical
        # post-handler semantics for code that inspects the returned
        # response cookie before prepare().
        session = request.get(SESSION_KEY)
        if (
            session is not None
            and session._changed
            and not request.get(COMMITTED_KEY)
            and isinstance(response, (web.Response, web.HTTPException))
        ):
            await storage.save_session(request, response, session)
            request[COMMITTED_KEY] = True
            session.freeze()
        if raise_response:
            raise cast(web.HTTPException, response)
        return response

    return factory


def setup(app: web.Application, storage: "AbstractStorage") -> None:
    """Setup the library in aiohttp fashion."""

    app.middlewares.append(session_middleware(storage))


class AbstractStorage(metaclass=abc.ABCMeta):
    def __init__(
        self,
        *,
        cookie_name: str = "AIOHTTP_SESSION",
        domain: str | None = None,
        max_age: int | None = None,
        path: str = "/",
        secure: bool | None = None,
        httponly: bool = True,
        samesite: str | None = None,
        encoder: Callable[[object], str] = json.dumps,
        decoder: Callable[[str], Any] = json.loads,
    ) -> None:
        self._cookie_name = cookie_name
        self._cookie_params = _CookieParams(
            domain=domain,
            max_age=max_age,
            path=path,
            secure=secure,
            httponly=httponly,
            samesite=samesite,
        )
        self._max_age = max_age
        self._encoder = encoder
        self._decoder = decoder

    @property
    def cookie_name(self) -> str:
        return self._cookie_name

    @property
    def max_age(self) -> int | None:
        return self._max_age

    @property
    def cookie_params(self) -> _CookieParams:
        return self._cookie_params

    def _get_session_data(self, session: Session) -> SessionData:
        if session.empty:
            return {}

        return {"created": session.created, "session": session._mapping}

    async def new_session(self) -> Session:
        return Session(None, data=None, new=True, max_age=self.max_age)

    @abc.abstractmethod
    async def load_session(self, request: web.Request) -> Session:
        pass

    @abc.abstractmethod
    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        pass

    def load_cookie(self, request: web.Request) -> str | None:
        return request.cookies.get(self._cookie_name)

    def save_cookie(
        self,
        response: web.StreamResponse,
        cookie_data: str,
        *,
        max_age: int | None = None,
    ) -> None:
        params = self._cookie_params.copy()
        if max_age is not None:
            params["max_age"] = max_age
            t = time.gmtime(time.time() + max_age)
            params["expires"] = time.strftime("%a, %d-%b-%Y %T GMT", t)
        if not cookie_data:
            response.del_cookie(
                self._cookie_name, domain=params["domain"], path=params["path"]
            )
        else:
            response.set_cookie(self._cookie_name, cookie_data, **params)


class SimpleCookieStorage(AbstractStorage):
    """Simple JSON storage.

    Doesn't any encryption/validation, use it for tests only"""

    def __init__(
        self,
        *,
        cookie_name: str = "AIOHTTP_SESSION",
        domain: str | None = None,
        max_age: int | None = None,
        path: str = "/",
        secure: bool | None = None,
        httponly: bool = True,
        samesite: str | None = None,
        encoder: Callable[[object], str] = json.dumps,
        decoder: Callable[[str], Any] = json.loads,
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

    async def load_session(self, request: web.Request) -> Session:
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)

        data = self._decoder(cookie)
        return Session(None, data=data, new=False, max_age=self.max_age)

    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        cookie_data = self._encoder(self._get_session_data(session))
        self.save_cookie(response, cookie_data, max_age=session.max_age)
