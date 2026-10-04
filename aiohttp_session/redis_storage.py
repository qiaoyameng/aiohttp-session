import json
import uuid
from collections.abc import Callable
from typing import Any

from aiohttp import web

from . import (
    AbstractStorage,
    FieldsSelector,
    IdentityRevokedError,
    RotationResult,
    Session,
    TTLPolicy,
)
from .log import log

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


# Atomic rotation:
#   mode "rotate": old key must hold a live record whose gen matches; mode
#   "mint": the new key must not exist (first identity for a fresh session).
#   Returns the effective new TTL (>=0) on success, 1 if the old key is
#   gone/expired, 2 on a generation mismatch (tombstone from a previous
#   rotation or invalidation), 3 if the fresh new key already exists (uuid
#   collision -- retry).
#   ARGV: mode, new_record, new_gen, policy, storage_ttl, expected_old_gen
#   TTLs and the authoritative "created" timestamp are derived from the
#   stored old record and the server clock, so PRESERVE keeps the exact
#   absolute deadline instead of trusting the client-loaded snapshot.
_ROTATE_SCRIPT = """
local mode = ARGV[1]
local old_key = KEYS[1]
local new_key = KEYS[2]
local new_record = ARGV[2]
local new_gen = ARGV[3]
local policy = ARGV[4]
local storage_ttl = tonumber(ARGV[5])
local now = redis.call("TIME")[1]

local function set_ttl(key, value, data)
    if value > 0 then
        redis.call("SET", key, data, "EX", value)
    else
        redis.call("SET", key, data)
    end
end

if mode == "rotate" then
    local old_raw = redis.call("GET", old_key)
    if old_raw == false then
        return 1
    end
    local ok, old = pcall(cjson.decode, old_raw)
    if not ok then
        return 1
    end
    if old.tombstone or tostring(old.gen or "") ~= ARGV[6] then
        return 2
    end
    if redis.call("EXISTS", new_key) == 1 then
        return 3
    end
    local new_ttl = storage_ttl
    local tomb_ttl = 0
    local migrated = cjson.decode(new_record)
    if storage_ttl > 0 then
        local remaining = old.created + storage_ttl - now
        if remaining < 1 then
            remaining = 1
        end
        tomb_ttl = remaining
        if policy == "preserve" then
            new_ttl = remaining
            migrated.created = old.created
        else
            new_ttl = storage_ttl
            migrated.created = now
        end
    end
    new_record = cjson.encode(migrated)
    local tombstone = cjson.encode(
        {tombstone = true, gen = new_gen, created = old.created}
    )
    set_ttl(new_key, new_ttl, new_record)
    set_ttl(old_key, tomb_ttl, tombstone)
    return new_ttl
end

if redis.call("EXISTS", new_key) == 1 then
    return 3
end
set_ttl(new_key, storage_ttl, new_record)
return storage_ttl
"""


# Atomic save of an existing identity. A tombstone or a record carrying a
# different generation means the caller holds a revoked generation: the write
# is refused instead of resurrecting or overwriting the new identity.
# Returns 0 on success, 1 if the key is gone, 2 on generation mismatch.
_SAVE_SCRIPT = """
local key = KEYS[1]
local data = ARGV[1]
local expected_gen = ARGV[2]
local ttl = tonumber(ARGV[3])

local raw = redis.call("GET", key)
if raw == false then
    return 1
end
local ok, stored = pcall(cjson.decode, raw)
if not ok then
    return 1
end
if stored.tombstone or tostring(stored.gen or "") ~= expected_gen then
    return 2
end
if ttl > 0 then
    redis.call("SET", key, data, "EX", ttl)
else
    redis.call("SET", key, data)
end
return 0
"""


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
        self._rotate_script = self._redis.register_script(_ROTATE_SCRIPT)
        self._save_script = self._redis.register_script(_SAVE_SCRIPT)

    def _stored_key(self, key: str) -> str:
        return self.cookie_name + "_" + key

    def _record_ttl(self, ttl: int | None) -> int:
        if ttl is None:
            return 0
        return ttl

    async def load_session(self, request: web.Request) -> Session:
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        else:
            key = str(cookie)
            data_bytes = await self._redis.get(self._stored_key(key))
            if data_bytes is None:
                return Session(None, data=None, new=True, max_age=self.max_age)
            data_str = data_bytes.decode("utf-8")
            try:
                data = self._decoder(data_str)
            except ValueError:
                data = None
            if isinstance(data, dict) and data.get("tombstone"):
                return Session(None, data=None, new=True, max_age=self.max_age)
            gen = data.get("gen") if isinstance(data, dict) else None
            return Session(key, data=data, new=False, max_age=self.max_age, gen=gen)

    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        if session.identity is None:
            # New identity for a fresh session.
            key = self._key_factory()
            data_str = self._encoder(self._get_session_data(session))
            await self._redis.set(
                self._stored_key(key),
                data_str,
                ex=session.max_age,
            )
            self.save_cookie(
                response,
                key if not session.empty else "",
                max_age=session.max_age,
            )
            return

        key = str(session.identity)
        if session.empty:
            # Late invalidation of a rotated-away generation is a no-op:
            # keep the tombstone and leave the (possibly new) cookie alone.
            if not await self._generation_alive(key, session.gen):
                log.warning(
                    "Refusing late invalidation of revoked session %r", key
                )
                return
            await self._redis.delete(self._stored_key(key))
            self.save_cookie(response, "", max_age=session.max_age)
            return

        data_str = self._encoder(self._get_session_data(session))
        result = await self._save_script(
            keys=[self._stored_key(key)],
            args=[data_str, str(session.gen or ""), self._record_ttl(session.max_age)],
        )
        if result in (1, 2):
            # Revoked (tombstone/generation mismatch) or expired between load
            # and save. Silently refuse to write or touch the cookie so a late
            # request can neither resurrect the old identity nor clobber the
            # current (possibly rotated) session.
            log.warning(
                "Refusing late save of revoked session %r (code=%s)",
                key,
                result,
            )
            return
        self.save_cookie(response, key, max_age=session.max_age)

    async def _generation_alive(self, key: str, gen: str | None) -> bool:
        raw = await self._redis.get(self._stored_key(key))
        if raw is None:
            return False
        try:
            stored = self._decoder(raw.decode("utf-8"))
        except ValueError:
            return False
        if isinstance(stored, dict) and (
            stored.get("tombstone") or stored.get("gen") != gen
        ):
            return False
        return True

    async def rotate_identity(
        self,
        request: web.Request,
        session: Session,
        *,
        fields: FieldsSelector = None,
        ttl: TTLPolicy | str = TTLPolicy.PRESERVE,
    ) -> RotationResult:
        policy = self._ttl_policy(ttl)
        migrated = self._select_fields(session._mapping, fields)

        old_key = session.identity
        mode = "rotate" if old_key is not None else "mint"
        effective_ttl = self._record_ttl(self.max_age)

        for _attempt in range(5):
            new_key = self._key_factory()
            new_gen = uuid.uuid4().hex
            args: list[Any] = [
                mode,
                self._encoder(
                    {"created": session.created, "gen": new_gen, "session": migrated}
                ),
                new_gen,
                policy.value,
                self._record_ttl(self.max_age),
            ]
            if mode == "rotate":
                args.append(str(session.gen or ""))
            result = await self._rotate_script(
                keys=[self._stored_key(str(old_key)), self._stored_key(new_key)],
                args=args,
            )
            if result >= 0:
                effective_ttl = int(result)
                break
            if result == 3:
                continue
            if result == 2:
                raise IdentityRevokedError(
                    "Session identity was revoked by another rotation or "
                    "invalidation"
                )
            raise IdentityRevokedError(
                "Session identity has expired or no longer exists"
            )
        else:  # pragma: no cover -- practically impossible uuid collisions
            raise IdentityRevokedError(
                "Could not allocate a fresh session identity"
            )

        return RotationResult(
            identity=new_key,
            gen=new_gen,
            data=migrated,
            max_age=effective_ttl if self.max_age is not None else None,
        )
