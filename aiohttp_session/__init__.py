"""User sessions for aiohttp.web."""

__version__ = "2.12.1"

import abc
import enum
import json
import time
from collections.abc import Callable, Collection, Iterator, Mapping, MutableMapping
from typing import Any, TypedDict, cast

from aiohttp import web
from aiohttp.typedefs import Handler, Middleware


class SessionRotationError(RuntimeError):
    """Base class for session identity rotation failures."""


class IdentityRevokedError(SessionRotationError):
    """The identity is revoked by a rotation or invalidation.

    Raised when a rotation or a save targets an identity whose generation no
    longer matches the backend state -- e.g. a request that loaded the old
    identity before rotation and tries to save after the rotation completed.
    """


class RotationNotSupportedError(SessionRotationError):
    """Storage cannot revoke old identities.

    Self-contained cookie storages (plain/encrypted/signed cookies) have no
    server side state to atomically rotate, so they do not implement the
    ``rotate_identity`` protocol.
    """


class TTLPolicy(enum.Enum):
    """How a rotation treats the session lifetime."""

    PRESERVE = "preserve"
    RESET = "reset"


#: A single key, a collection of keys, a predicate on the key or ``None`` for
#: "migrate every field".
FieldsSelector = "str | Collection[str] | Callable[[str], bool] | None"


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
    gen: str
    session: dict[str, Any]


class RotationResult(TypedDict):
    identity: str
    gen: str
    data: dict[str, Any]
    max_age: int | None


class Session(MutableMapping[str, Any]):

    """Session dict-like object."""

    def __init__(
        self,
        identity: Any | None,
        *,
        data: SessionData | None,
        new: bool,
        max_age: int | None = None,
        gen: str | None = None,
    ) -> None:
        self._changed: bool = False
        self._mapping: dict[str, Any] = {}
        self._identity = identity if data != {} else None
        self._new = new if data != {} else True
        self._max_age = max_age
        self._gen = gen
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
    def gen(self) -> str | None:
        """Opaque generation tag of the current identity.

        ``None`` for sessions created client side or by storages that do not
        participate in the :meth:`AbstractStorage.rotate_identity` protocol.
        """
        return self._gen

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

    async def rotate_identity(
        self,
        request: web.Request,
        *,
        fields: FieldsSelector = None,
        ttl: TTLPolicy | str = TTLPolicy.PRESERVE,
    ) -> str:
        """Atomically revoke the current identity and take over a new one.

        Must be called after login or privilege elevation to prevent session
        fixation. The backend storage creates a brand new identity, migrates
        the selected *fields* and revokes the old identity in one atomic
        operation; the old identity keeps a tombstone so that requests which
        loaded it before the rotation cannot resurrect it with a late save.

        *fields* selects which session keys to migrate: ``None`` (default)
        migrates every key, a string migrates one key, a collection of
        strings migrates the listed keys and a callable gets each key and
        migrates it when the predicate returns a truthy value.

        *ttl* is :data:`TTLPolicy.PRESERVE` (default) to keep the old
        absolute expiry deadline, or :data:`TTLPolicy.RESET` / ``"reset"``
        for a fresh storage lifetime.

        Returns the new identity string. Raises
        :class:`IdentityRevokedError` if the old identity was already
        rotated or invalidated (this covers duplicate/concurrent rotation
        requests and late saves), and :class:`RotationNotSupportedError`
        for self-contained cookie storages.

        The cookie itself is only updated later by the session middleware
        when it saves the session into the response; call this before the
        response is prepared.
        """
        if self.empty:
            raise RuntimeError(
                "Cannot rotate identity of an empty or invalidated session"
            )
        storage = request.get(STORAGE_KEY)
        if storage is None:
            raise RuntimeError(
                "Install aiohttp_session middleware "
                "in your aiohttp.web.Application"
            )
        result = await storage.rotate_identity(
            request, self, fields=fields, ttl=ttl
        )
        self._identity = result["identity"]
        self._gen = result["gen"]
        self._mapping = dict(result["data"])
        self._changed = True
        if result["max_age"] is not None:
            self._max_age = result["max_age"]
        return result["identity"]

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
    return session


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
        if not isinstance(response, (web.Response, web.HTTPException)):
            # likely got websocket or streaming
            return response
        if response.prepared:
            raise RuntimeError("Cannot save session data into prepared response")
        session = request.get(SESSION_KEY)
        if session is not None:
            if session._changed:
                await storage.save_session(request, response, session)
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

        data: SessionData = {"created": session.created, "session": session._mapping}
        if session.gen is not None:
            data["gen"] = session.gen
        return data

    @staticmethod
    def _select_fields(
        mapping: Mapping[str, Any], fields: FieldsSelector
    ) -> dict[str, Any]:
        if fields is None:
            return dict(mapping)
        if isinstance(fields, str):
            return {fields: mapping[fields]} if fields in mapping else {}
        if callable(fields):
            return {key: value for key, value in mapping.items() if fields(key)}
        if isinstance(fields, Collection):
            return {
                key: mapping[key] for key in fields if key in mapping
            }
        raise TypeError(f"Unsupported fields selector: {fields!r}")

    @staticmethod
    def _ttl_policy(ttl: TTLPolicy | str) -> TTLPolicy:
        if isinstance(ttl, str) and not isinstance(ttl, TTLPolicy):
            try:
                ttl = TTLPolicy(ttl)
            except ValueError:
                raise ValueError(
                    f"Unsupported TTL policy: {ttl!r}, "
                    "expected TTLPolicy.PRESERVE or TTLPolicy.RESET"
                ) from None
        if not isinstance(ttl, TTLPolicy):
            raise TypeError(f"Unsupported TTL policy: {ttl!r}")
        return ttl

    def _rotation_ttls(
        self,
        session: "Session",
        ttl: TTLPolicy,
    ) -> tuple[int | None, int | None]:
        """Return ``(new_ttl, tombstone_ttl)`` seconds for a rotation.

        ``PRESERVE`` keeps the absolute deadline of the old session: the new
        record and the tombstone share the old remaining lifetime, so the
        security deadline is never extended. ``RESET`` gives the new record
        the storage default lifetime while the tombstone keeps the old
        remaining lifetime (it must never outlive the deadline the user was
        granted).
        """
        default = self.max_age
        if session.max_age is None:
            return default, None
        remaining = session.created + session.max_age - int(time.time())
        if remaining < 1:
            remaining = 1
        if ttl is TTLPolicy.PRESERVE:
            return remaining, remaining
        return default, remaining

    async def new_session(self) -> Session:
        return Session(None, data=None, new=True, max_age=self.max_age)

    async def rotate_identity(
        self,
        request: web.Request,
        session: "Session",
        *,
        fields: FieldsSelector = None,
        ttl: TTLPolicy | str = TTLPolicy.PRESERVE,
    ) -> RotationResult:
        """Atomically mint a new identity, migrate data and revoke the old.

        Server side storages override this with a backend atomic operation
        that (1) creates the new identity with a fresh generation,
        (2) copies the selected fields and (3) revokes the old identity,
        leaving a tombstone tagged with the new generation.

        The default implementation raises
        :class:`RotationNotSupportedError`: self-contained cookie storages
        cannot revoke cookies that have already been issued and therefore
        make no server side revocation promise.
        """
        raise RotationNotSupportedError(
            f"{type(self).__name__} does not support identity rotation"
        )

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
