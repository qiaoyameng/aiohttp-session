"""User sessions for aiohttp.web."""

__version__ = "2.13.0"

import abc
import json
import time
from collections.abc import Callable, Iterable, Iterator, MutableMapping
from typing import Any, NamedTuple, TypedDict, cast

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
    expires_at: int
    session: dict[str, Any]


#: Marker value stored under a revoked (tombstoned) identity.
#:
#: A tombstone keeps the old identity dead after rotation.  Any request that
#: was loaded with the old identity before rotation completed and tries to
#: save itself later (a session fixation / late-save attempt) is rejected
#: instead of silently resurrecting the revoked key.
TOMBSTONE_MARKER = "__aiohttp_session_tombstone__"


class SessionRotationError(RuntimeError):
    """Base class for session identity rotation failures."""


class IdentityRotationUnsupported(SessionRotationError):
    """Raised when the storage cannot rotate identities server-side.

    Self-contained cookie storages (``SimpleCookieStorage``,
    :class:`~aiohttp_session.cookie_storage.EncryptedCookieStorage`,
    :class:`~aiohttp_session.nacl_storage.NaClCookieStorage`) have no
    server-side state to revoke, therefore they cannot implement rotation.
    """


class IdentityRevokedError(SessionRotationError):
    """Raised when an identity has already been revoked by a rotation.

    Typical causes:

    * a second (duplicate) :meth:`Session.rotate_identity` request for the
      same identity;
    * a request that loaded the old identity before rotation and tries to
      save after rotation completed (a late save / fixation attempt).
    """


class RotatedIdentity(NamedTuple):
    """Result of an atomic backend rotation.

    * ``new_identity`` -- the identity created by the backend;
    * ``old_ttl`` -- remaining lifetime in seconds of the *old* identity at
      the rotation moment (``None`` if the old record did not expire).
    """

    new_identity: str
    old_ttl: int | None


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
        self._storage: "AbstractStorage | None" = None
        self._request: web.Request | None = None
        created = data.get("created", None) if data else None
        expires_at = data.get("expires_at", None) if data else None
        session_data = data.get("session", None) if data else None
        now = int(time.time())
        age = now - created if created else now
        if max_age is not None and age > max_age:
            session_data = None
        if self._new or created is None:
            self._created = now
        else:
            self._created = created
        if expires_at is not None and expires_at > now:
            # The record carries an absolute deadline (set by a previous
            # rotation).  Keep it instead of silently sliding the TTL.
            self._expires_at: int | None = expires_at
            self._max_age = expires_at - now
        else:
            self._expires_at = None

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

    @property
    def expires_at(self) -> int | None:
        """Absolute expiration UNIX timestamp, or ``None``.

        Pinned by :meth:`rotate_identity` when the original absolute
        deadline is preserved; ``None`` means the storage default sliding
        ``max_age`` applies.
        """
        return self._expires_at

    def _bind(
        self, storage: "AbstractStorage", request: web.Request
    ) -> None:
        self._storage = storage
        self._request = request

    def changed(self) -> None:
        self._changed = True

    def invalidate(self) -> None:
        self._changed = True
        self._mapping = {}

    def set_new_identity(self, identity: Any | None) -> None:
        if not self._new:
            raise RuntimeError("Can't change identity for a session which is not new")

        self._identity = identity

    async def rotate_identity(
        self,
        *,
        fields: Iterable[str] | None = None,
        max_age: int | None = None,
        reset_ttl: bool = False,
    ) -> str:
        """Rotate the session identity atomically, migrating selected data.

        Call this on login or privilege escalation to defeat session
        fixation.  The backend atomically:

        1. creates a *new* identity and stores the migrated data under it;
        2. revokes the *old* identity, leaving a tombstone that blocks late
           saves made by requests which had loaded the old identity;
        3. returns the new identity together with the old TTL.

        Only after the backend rotation succeeds is this :class:`Session`
        switched to the new identity, so the response cookie (written by
        the middleware on :meth:`save_session`) is updated exclusively on
        success.

        *fields* selects which keys are migrated to the new identity.
        ``None`` (the default) migrates every key; an iterable migrates
        exactly those keys (a missing key raises :class:`KeyError`).  Use
        e.g. ``fields=("cart",)`` to keep a shopping cart while dropping an
        anonymous identity.

        TTL policy:

        * by default the old *absolute* deadline is preserved -- rotation
          never extends the security lifetime;
        * *reset_ttl* ``True`` (or an explicit *max_age*) restarts the
          lifetime using that value (the storage default when *max_age* is
          ``None`` and *reset_ttl* is true).

        :raises RuntimeError: the session is not bound to a request/storage.
        :raises SessionRotationError: the session is new, already
            invalidated, or the rotation failed; the session is left
            untouched (still holding the old identity) so a failure
            response does not issue a new cookie.
        """
        if self._storage is None or self._request is None:
            raise RuntimeError(
                "Session.rotate_identity() requires a session loaded "
                "through get_session()/new_session()"
            )
        if self._new:
            raise SessionRotationError(
                "Cannot rotate identity of a new (never persisted) session"
            )
        if fields is None:
            migrated = dict(self._mapping)
        else:
            migrated = {key: self._mapping[key] for key in fields}
        if not migrated:
            raise SessionRotationError(
                "Cannot rotate identity of an invalidated or empty "
                "session (nothing to migrate)"
            )

        storage = self._storage
        rotated = await storage.rotate_identity(
            self._request,
            self,
            migrated,
            max_age=max_age,
            reset_ttl=reset_ttl,
        )
        self._commit_rotation(
            storage,
            rotated.new_identity,
            migrated,
            max_age=max_age,
            reset_ttl=reset_ttl,
            old_ttl=rotated.old_ttl,
        )
        return rotated.new_identity

    def _commit_rotation(
        self,
        storage: "AbstractStorage",
        new_identity: str,
        migrated: dict[str, Any],
        *,
        max_age: int | None,
        reset_ttl: bool,
        old_ttl: int | None,
    ) -> None:
        self._identity = new_identity
        self._mapping = migrated
        self._new = False
        self._changed = True
        now = int(time.time())
        if reset_ttl or max_age is not None:
            ttl = storage.max_age if max_age is None else max_age
            self._max_age = ttl
            self._expires_at = None if ttl is None else now + ttl
        else:
            # Preserve the absolute deadline.  Prefer the deadline pinned
            # in this session; fall back to the old record TTL reported by
            # the backend; finally to the storage default max_age.
            if self._expires_at is not None:
                ttl = self._expires_at - now
            elif old_ttl is not None:
                ttl = old_ttl
                self._expires_at = now + ttl
            else:
                ttl = storage.max_age
            self._max_age = None if ttl is None else max(ttl, 1)

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
        session._bind(storage, request)
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
    session._bind(storage, request)
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

        data: SessionData = {
            "created": session.created,
            "session": session._mapping,
        }
        if session.expires_at is not None:
            data["expires_at"] = session.expires_at
        return data

    async def new_session(self) -> Session:
        return Session(None, data=None, new=True, max_age=self.max_age)

    def _new_identity(self) -> str:
        raise IdentityRotationUnsupported(
            f"{type(self).__name__} is a self-contained cookie storage "
            "and cannot rotate identities server-side"
        )

    async def rotate_identity(
        self,
        request: web.Request,
        session: Session,
        migrated: dict[str, Any],
        *,
        max_age: int | None,
        reset_ttl: bool,
    ) -> RotatedIdentity:
        """Rotate *session*'s identity, atomically migrating *migrated*.

        Self-contained cookie storages raise
        :class:`IdentityRotationUnsupported`: with no server-side record
        there is nothing to revoke, so an old cookie keeps working.

        Server-side storages (:class:`~aiohttp_session.redis_storage.RedisStorage`,
        :class:`~aiohttp_session.memcached_storage.MemcachedStorage`)
        override this method.  Implementations must perform the
        create-new / revoke-old transition as a single atomic backend
        operation: fail without writing anything if the old identity is
        missing or already a tombstone, store the migrated data under the
        new identity, and replace the old identity with a tombstone that
        rejects subsequent late saves.
        """
        raise IdentityRotationUnsupported(
            f"{type(self).__name__} is a self-contained cookie storage "
            "and cannot rotate identities server-side"
        )

    def _build_rotation(
        self,
        session: Session,
        migrated: dict[str, Any],
        *,
        max_age: int | None,
        reset_ttl: bool,
    ) -> tuple[str, str, str, int | None, int | None]:
        """Prepare identity, payloads and TTLs for :meth:`_rotate_record`.

        Pure (no backend I/O), so a failing backend call leaves the
        session and its cookie untouched.
        """
        new_identity = self._new_identity()
        now = int(time.time())
        if reset_ttl or max_age is not None:
            ttl = self.max_age if max_age is None else max_age
            new_expires_at: int | None = None if ttl is None else now + ttl
        elif session.expires_at is not None:
            ttl = max(session.expires_at - now, 1)
            new_expires_at = session.expires_at
        else:
            ttl = self.max_age
            new_expires_at = None
        record: SessionData = {
            "created": session.created,
            "session": migrated,
        }
        if new_expires_at is not None:
            record["expires_at"] = new_expires_at
        tombstone_record: SessionData = {
            "created": session.created,
            "session": {},
        }
        if new_expires_at is not None:
            tombstone_record["expires_at"] = new_expires_at
        # Tombstones live until the old record would have expired (at
        # least the storage max_age), so a late save cannot resurrect it.
        if session.expires_at is not None:
            tombstone_ttl = max(session.expires_at - now, 1)
        elif ttl is not None:
            tombstone_ttl = ttl
        elif self.max_age is not None:
            tombstone_ttl = self.max_age
        else:
            # Persistent records: block the old identity long enough to
            # outlive late in-flight requests.
            tombstone_ttl = 30 * 24 * 60 * 60
        return (
            new_identity,
            self._encoder(record),
            TOMBSTONE_MARKER + self._encoder(tombstone_record),
            ttl,
            tombstone_ttl,
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
