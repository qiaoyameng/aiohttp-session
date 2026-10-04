import json
import uuid
from collections.abc import Callable
from time import time
from typing import Any

import aiomcache
from aiohttp import web

from . import (
    TOMBSTONE_MARKER,
    AbstractStorage,
    IdentityRevokedError,
    RotatedIdentity,
    Session,
)

#: Number of compare-and-swap retries before declaring a conflict.
_CAS_ATTEMPTS = 5


class MemcachedStorage(AbstractStorage):
    """Memcached storage"""

    def __init__(
        self,
        memcached_conn: aiomcache.Client,
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
        decoder: Callable[[str], Any] = json.loads
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
        self._key_factory = key_factory
        self.conn = memcached_conn

    def _stored_key(self, identity: str) -> bytes:
        return (self.cookie_name + "_" + identity).encode("utf-8")

    @staticmethod
    def _exptime(max_age: int | None) -> int:
        # https://github.com/memcached/memcached/wiki/Programming#expiration
        # "Expiration times can be set from 0, meaning "never expire", to
        # 30 days. Any time higher than 30 days is interpreted as a Unix
        # timestamp date."
        if max_age is None:
            return 0
        if max_age > 30 * 24 * 60 * 60:
            return int(time()) + max_age
        return max_age

    @staticmethod
    def _gets_value(fetched: Any) -> tuple[bytes | None, int]:
        # aiomcache.Client.gets() returns (value, cas_token); tolerate
        # versions/shapes that return only the value.
        if isinstance(fetched, tuple):
            value, cas = fetched
            return value, int(cas or 0)
        return fetched, 0

    async def rotate_identity(
        self,
        request: web.Request,
        session: Session,
        migrated: dict[str, Any],
        *,
        max_age: int | None,
        reset_ttl: bool,
    ) -> RotatedIdentity:
        old_stored_key = self._stored_key(str(session.identity))
        marker = TOMBSTONE_MARKER.encode("utf-8")
        for _attempt in range(_CAS_ATTEMPTS):
            value, cas = self._gets_value(await self.conn.gets(old_stored_key))
            if value is None:
                # Never stored or already invalidated: no identity to
                # rotate, and nothing must be created.
                raise IdentityRevokedError(
                    f"Session identity {session.identity!r} is no longer valid"
                )
            if value.startswith(marker):
                raise IdentityRevokedError(
                    f"Session identity {session.identity!r} has been revoked"
                )
            (
                new_identity,
                new_data,
                tombstone,
                new_ttl,
                tombstone_ttl,
            ) = self._build_rotation(
                session, migrated, max_age=max_age, reset_ttl=reset_ttl
            )
            new_stored_key = self._stored_key(new_identity)
            # The new random identity is unique; ``add`` only succeeds while
            # it does not exist.  A clash with a previous rotation retries.
            try:
                added = await self.conn.add(
                    new_stored_key,
                    new_data.encode("utf-8"),
                    exptime=self._exptime(new_ttl),
                )
            except aiomcache.exceptions.ClientException:
                added = False
            if not added:
                continue
            try:
                swapped = await self.conn.cas(
                    old_stored_key,
                    tombstone.encode("utf-8"),
                    cas,
                    exptime=self._exptime(tombstone_ttl),
                )
            except aiomcache.exceptions.ClientException:
                swapped = False
            if not swapped:
                # Lost the CAS race (a concurrent rotation or a save).
                # Roll back the new record and re-read.
                await self.conn.delete(new_stored_key)
                continue
            now = int(time())
            if session.expires_at is not None:
                old_ttl: int | None = max(session.expires_at - now, 1)
            else:
                old_ttl = new_ttl
            return RotatedIdentity(new_identity, old_ttl)
        raise IdentityRevokedError(
            f"Cannot rotate session identity {session.identity!r}: "
            "persistent storage conflict"
        )

    async def _guarded_set(
        self, stored_key: bytes, data: bytes, exptime: int
    ) -> None:
        """Write *data* unless *stored_key* is a tombstone.

        Uses gets/cas so a rotation that revoked the identity between the
        load and this save cannot be overwritten.
        """
        marker = TOMBSTONE_MARKER.encode("utf-8")
        for _attempt in range(_CAS_ATTEMPTS):
            value, cas = self._gets_value(await self.conn.gets(stored_key))
            if value is not None and value.startswith(marker):
                raise IdentityRevokedError(
                    "Cannot save a revoked session identity: the identity "
                    "was rotated; this late request cannot resurrect it"
                )
            try:
                if value is None:
                    ok = await self.conn.add(stored_key, data, exptime=exptime)
                else:
                    ok = await self.conn.cas(
                        stored_key, data, cas, exptime=exptime
                    )
            except aiomcache.exceptions.ClientException:
                ok = False
            if ok:
                return
        raise RuntimeError(  # pragma: no cover
            "Memcached CAS contention while saving session"
        )

    async def load_session(self, request: web.Request) -> Session:
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        else:
            key = str(cookie)
            stored_key = (self.cookie_name + "_" + key).encode("utf-8")
            data_b = await self.conn.get(stored_key)
            if data_b is None:
                return Session(None, data=None, new=True, max_age=self.max_age)
            if data_b.startswith(TOMBSTONE_MARKER.encode("utf-8")):
                return Session(None, data=None, new=True, max_age=self.max_age)
            data = data_b.decode("utf-8")
            try:
                sess_data = self._decoder(data)
            except ValueError:
                sess_data = None
            return Session(key, data=sess_data, new=False, max_age=self.max_age)

    async def save_session(
        self, request: web.Request, response: web.StreamResponse, session: Session
    ) -> None:
        identity = session.identity
        is_new_identity = identity is None
        if is_new_identity:
            identity = self._key_factory()
        key = str(identity)
        data = self._encoder(self._get_session_data(session))
        if session.empty:
            # Invalidation: drop the cookie and delete the server-side
            # record (if it ever existed).
            self.save_cookie(response, "", max_age=session.max_age)
            if not is_new_identity:
                stored_key = self._stored_key(key)
                await self.conn.delete(stored_key)
            return
        # Always (re)issue the cookie -- the identity may have changed
        # through rotation.
        self.save_cookie(response, str(key), max_age=session.max_age)
        payload = data.encode("utf-8")
        # A newly generated identity is the only non-empty case that was
        # never stored, so no tombstone can guard it; plain set() is fine.
        stored_key = self._stored_key(key)
        if is_new_identity or session.new:
            await self.conn.set(
                stored_key, payload, exptime=self._exptime(session.max_age)
            )
        else:
            await self._guarded_set(
                stored_key, payload, self._exptime(session.max_age)
            )
