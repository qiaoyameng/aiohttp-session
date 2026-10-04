"""User sessions for aiohttp.web."""

__version__ = "2.12.1"

import abc
import json
import time
from collections.abc import Callable, Iterator, MutableMapping
from http.cookies import SimpleCookie
from typing import Any, TypedDict, cast

from aiohttp import web
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
        self._changed = True

    def invalidate(self) -> None:
        self._changed = True
        self._mapping = {}

    def set_new_identity(self, identity: Any | None) -> None:
        if not self._new:
            raise RuntimeError("Can't change identity for a session which is not new")

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
        self._mapping[key] = value
        self._changed = True
        self._created = int(time.time())

    def __delitem__(self, key: str) -> None:
        del self._mapping[key]
        self._changed = True
        self._created = int(time.time())


SESSION_KEY = "aiohttp_session"
STORAGE_KEY = "aiohttp_session_storage"
REGISTRY_KEY = web.AppKey("aiohttp_session_registry", dict)
DEFAULT_NAMESPACE = "default"


def _session_key(namespace: str) -> str:
    if namespace == DEFAULT_NAMESPACE:
        return SESSION_KEY
    return f"{SESSION_KEY}:{namespace}"


def _storage_key(namespace: str) -> str:
    if namespace == DEFAULT_NAMESPACE:
        return STORAGE_KEY
    return f"{STORAGE_KEY}:{namespace}"


class SessionSaveError(RuntimeError):
    """Raised when saving one of the configured session namespaces fails.

    Multiple storages do not provide distributed transactions: namespaces
    listed in ``saved`` already completed their server-side writes, the
    ``failed`` namespace raised, and ``pending`` namespaces were never
    attempted. No session cookies were sent to the client.
    """

    def __init__(
        self, *, saved: list[str], failed: str, pending: list[str]
    ) -> None:
        self.saved = tuple(saved)
        self.failed = failed
        self.pending = tuple(pending)
        super().__init__(
            "Failed to save session namespace {!r}; server-side writes "
            "already completed for {!r}, {!r} were never attempted; "
            "no session cookies were sent".format(failed, saved, pending)
        )


async def get_session(
    request: web.Request, *, namespace: str | None = None
) -> Session:
    ns = DEFAULT_NAMESPACE if namespace is None else namespace
    session = request.get(_session_key(ns))
    if session is None:
        storage = request.get(_storage_key(ns))
        if storage is None:
            raise RuntimeError(
                "Install aiohttp_session middleware "
                "in your aiohttp.web.Application"
            )

        session = await storage.load_session(request)
        if not isinstance(session, Session):
            raise RuntimeError(
                "Installed {!r} storage should return session instance "
                "on .load_session() call, got {!r}.".format(storage, session)
            )
        request[_session_key(ns)] = session
    return session


async def new_session(
    request: web.Request, *, namespace: str | None = None
) -> Session:
    ns = DEFAULT_NAMESPACE if namespace is None else namespace
    storage = request.get(_storage_key(ns))
    if storage is None:
        raise RuntimeError(
            "Install aiohttp_session middleware "
            "in your aiohttp.web.Application"
        )

    session = await storage.new_session()
    if not isinstance(session, Session):
        raise RuntimeError(
            "Installed {!r} storage should return session instance "
            "on .load_session() call, got {!r}.".format(storage, session)
        )
    request[_session_key(ns)] = session
    return session


def session_middleware(
    storage: "AbstractStorage", *, namespace: str | None = None
) -> Middleware:
    if not isinstance(storage, AbstractStorage):
        raise RuntimeError(f"Expected AbstractStorage got {storage}")
    ns = DEFAULT_NAMESPACE if namespace is None else namespace

    @web.middleware
    async def factory(request: web.Request, handler: Handler) -> web.StreamResponse:
        request[_storage_key(ns)] = storage
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
        if not isinstance(response, (web.Response, web.HTTPException)):
            # likely got websocket or streaming
            return response
        if response.prepared:
            raise RuntimeError("Cannot save session data into prepared response")
        session = request.get(_session_key(ns))
        if session is not None:
            if session._changed:
                await storage.save_session(request, response, session)
        if raise_response:
            raise cast(web.HTTPException, response)
        return response

    return factory


def _paths_overlap(first: str, second: str) -> bool:
    first = first.rstrip("/") or "/"
    second = second.rstrip("/") or "/"
    if first == second or first == "/" or second == "/":
        return True
    return first.startswith(second + "/") or second.startswith(first + "/")


def _cookie_conflicts(first: "AbstractStorage", second: "AbstractStorage") -> bool:
    """Check whether two storages would overwrite each other's cookies."""
    if first.cookie_name != second.cookie_name:
        return False
    first_domain = (first.cookie_params.get("domain") or "").lower()
    second_domain = (second.cookie_params.get("domain") or "").lower()
    if first_domain != second_domain:
        return False
    return _paths_overlap(
        first.cookie_params["path"], second.cookie_params["path"]
    )


async def _save_sessions(
    request: web.Request,
    response: web.StreamResponse,
    registry: dict[str, "AbstractStorage"],
) -> None:
    """Save all changed sessions in registration order.

    Cookies are buffered on scratch responses and only applied to the real
    response once every save succeeded, so a failure never sends a partial
    set of cookies. Server-side writes that already happened are reported
    through SessionSaveError.
    """
    pending: list[tuple[str, AbstractStorage, Session]] = []
    for ns, storage in registry.items():
        session = request.get(_session_key(ns))
        if session is not None and session._changed:
            pending.append((ns, storage, session))
    if not pending:
        return
    cookies: SimpleCookie[str] = SimpleCookie()
    saved: list[str] = []
    for ns, storage, session in pending:
        scratch = web.Response()
        try:
            await storage.save_session(request, scratch, session)
        except Exception as exc:
            remaining = [name for name, _, _ in pending[len(saved) + 1:]]
            raise SessionSaveError(
                saved=saved, failed=ns, pending=remaining
            ) from exc
        cookies.update(scratch.cookies)
        saved.append(ns)
    response.cookies.update(cookies)


def _registry_middleware(registry: dict[str, "AbstractStorage"]) -> Middleware:
    @web.middleware
    async def middleware(
        request: web.Request, handler: Handler
    ) -> web.StreamResponse:
        for ns, storage in registry.items():
            request[_storage_key(ns)] = storage
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
        if not isinstance(response, (web.Response, web.HTTPException)):
            # likely got websocket or streaming
            return response
        if response.prepared:
            raise RuntimeError("Cannot save session data into prepared response")
        await _save_sessions(request, response, registry)
        if raise_response:
            raise cast(web.HTTPException, response)
        return response

    return middleware


def setup(
    app: web.Application,
    storage: "AbstractStorage",
    *,
    namespace: str | None = None,
) -> None:
    """Setup the library in aiohttp fashion."""
    if not isinstance(storage, AbstractStorage):
        raise RuntimeError(f"Expected AbstractStorage got {storage}")
    ns = DEFAULT_NAMESPACE if namespace is None else namespace
    registry = app.get(REGISTRY_KEY)
    if registry is None:
        registry = {}
        app[REGISTRY_KEY] = registry
        # Install the shared middleware exactly once, no matter how many
        # namespaces are registered, so no session is ever saved twice.
        app.middlewares.append(_registry_middleware(registry))
    if ns in registry:
        raise RuntimeError(
            f"Session namespace {ns!r} is already set up on this application"
        )
    for existing_ns, existing in registry.items():
        if _cookie_conflicts(storage, existing):
            raise ValueError(
                "Cookie configuration of namespace {!r} (cookie {!r}, "
                "domain {!r}, path {!r}) overlaps with namespace {!r} and "
                "would overwrite its cookies".format(
                    ns,
                    storage.cookie_name,
                    storage.cookie_params.get("domain"),
                    storage.cookie_params["path"],
                    existing_ns,
                )
            )
    registry[ns] = storage


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
