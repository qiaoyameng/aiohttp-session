import json
import uuid
from collections.abc import Callable
from typing import Any

from aiohttp import web

from . import (
    TOMBSTONE_MARKER,
    AbstractStorage,
    IdentityRevokedError,
    RotatedIdentity,
    Session,
)

try:
    from redis import VERSION as REDIS_VERSION, asyncio as aioredis
except ImportError:  # pragma: no cover
    try:
        import aioredis  # type: ignore[import-not-found, no-redef]
    except ImportError:
        aioredis = None  # type: ignore[assignment]
    else:
        import warnings

        warnings.warn(
            "aioredis library is deprecated, please replace with redis.",
            DeprecationWarning,
            stacklevel=1,
        )
        REDIS_VERSION = (4, 3)


class RedisStorage(AbstractStorage):
    """Redis storage"""

    def __init__(
        self,
        redis_pool: "aioredis.Redis",
        *,
        cookie_name: str = "AIOHTTP_SESSION",
        domain: str | None = None,
        max_age: int | None = None,
        path: str = "/",
        secure: bool | None = None,
        httponly: bool = True,
        samesite: str | None = None,
        key_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
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
        if aioredis is None:
            raise RuntimeError("Please install redis")
        # May have installed aioredis separately (without aiohttp-session[aioredis]).
        if REDIS_VERSION < (4, 3):
            raise RuntimeError("redis<4.3 is not supported")
        self._key_factory = key_factory
        if not isinstance(redis_pool, aioredis.Redis):
            raise TypeError(f"Expected redis.asyncio.Redis got {type(redis_pool)}")
        self._redis = redis_pool
        # Atomic rotation: abort unless the old key holds a *live* record
        # (missing and tombstone are both rejected), then create the new
        # key and replace the old key with a tombstone in one round trip.
        # KEYS[1]=old, KEYS[2]=new; ARGV: new_data, tombstone,
        # new_ttl_ms (<=0 -> keep without expiry), tombstone_ttl_ms.
        # Returns the old PTTL in milliseconds (>=0), or status strings
        # "missing" / "revoked".
        self._rotate_script = self._redis.register_script(
            """
            local old = KEYS[1]
            local value = redis.call('GET', old)
            if value == false then
                return 'missing'
            end
            if string.sub(value, 1, %d) == %s then
                return 'revoked'
            end
            local pttl = redis.call('PTTL', old)
            local new_ttl = tonumber(ARGV[3])
            if new_ttl > 0 then
                if redis.call('SET', KEYS[2], ARGV[1], 'PX', new_ttl, 'NX')
                        == false then
                    return 'exists'
                end
            else
                if redis.call('SET', KEYS[2], ARGV[1], 'NX') == false then
                    return 'exists'
                end
            end
            redis.call('PSETEX', old, tonumber(ARGV[4]), ARGV[2])
            if pttl >= 0 then
                return pttl
            end
            return -1
            """
            % (len(TOMBSTONE_MARKER), json.dumps(TOMBSTONE_MARKER))
        )
        # Guarded save: refuse to write over a tombstone, so a request that
        # loaded the old identity before rotation cannot save afterwards and
        # resurrect the revoked key.
        # KEYS[1]=key; ARGV: data, ttl_ms (<=0 -> persist).
        self._save_script = self._redis.register_script(
            """
            local value = redis.call('GET', KEYS[1])
            if type(value) == 'string'
                    and string.sub(value, 1, %d) == %s then
                return 'revoked'
            end
            local ttl = tonumber(ARGV[2])
            if ttl > 0 then
                redis.call('PSETEX', KEYS[1], ttl, ARGV[1])
            else
                redis.call('SET', KEYS[1], ARGV[1])
            end
            return 'ok'
            """
            % (len(TOMBSTONE_MARKER), json.dumps(TOMBSTONE_MARKER))
        )

    def _key(self, identity: str) -> str:
        return self.cookie_name + "_" + identity

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
        result = await self._rotate_script(
            keys=[self._key(old_identity), self._key(new_identity)],
            args=[
                new_data,
                tombstone,
                (new_ttl or 0) * 1000,
                tombstone_ttl * 1000,
            ],
        )
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        if result in ("missing", "revoked", "exists"):
            raise IdentityRevokedError(
                f"Session identity {old_identity!r} is no longer valid"
            )
        old_pttl = int(result)
        old_ttl = None if old_pttl < 0 else max(old_pttl // 1000, 1)
        return RotatedIdentity(new_identity, old_ttl)

    async def load_session(self, request: web.Request) -> Session:
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        else:
            key = str(cookie)
            data_bytes = await self._redis.get(self._key(key))
            if data_bytes is None:
                return Session(None, data=None, new=True, max_age=self.max_age)
            data_str = data_bytes.decode("utf-8")
            if data_str.startswith(TOMBSTONE_MARKER):
                return Session(None, data=None, new=True, max_age=self.max_age)
            try:
                data = self._decoder(data_str)
            except ValueError:
                data = None
            return Session(key, data=data, new=False, max_age=self.max_age)

    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        identity = session.identity
        is_new_identity = identity is None
        if is_new_identity:
            identity = self._key_factory()
        key = str(identity)
        if session.empty:
            # Invalidation: drop the cookie and delete the server-side
            # record (if it ever existed).
            self.save_cookie(response, "", max_age=session.max_age)
            if not is_new_identity:
                await self._redis.delete(self._key(key))
            return
        # Always (re)issue the cookie: the identity can have changed via
        # rotation even for a non-new, non-empty session.
        self.save_cookie(response, key, max_age=session.max_age)

        data_str = self._encoder(self._get_session_data(session))
        ttl_ms = (session.max_age or 0) * 1000
        result = await self._save_script(
            keys=[self._key(key)], args=[data_str, ttl_ms]
        )
        if isinstance(result, bytes):
            result = result.decode("utf-8")
        if result == "revoked":
            raise IdentityRevokedError(
                f"Cannot save revoked session identity {key!r}: the identity "
                "was rotated; this late request cannot resurrect it"
            )
