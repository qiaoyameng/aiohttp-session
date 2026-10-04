"""In-memory server side storage implementing the rotate_identity protocol.

Used by rotation tests: unlike the self-contained cookie storages it keeps
server side state (records + tombstones) and performs the
mint/migrate/revoke step under a single lock, emulating the atomic backend
protocol guaranteed by the Redis implementation.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any

from aiohttp import web

from aiohttp_session import (
    AbstractStorage,
    FieldsSelector,
    IdentityRevokedError,
    RotationResult,
    Session,
    TTLPolicy,
)
from aiohttp_session.log import log


class AtomicMemoryStorage(AbstractStorage):
    """Dict backed storage with generation tags and tombstones."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._records: dict[str, dict[str, Any]] = {}
        self._expires: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self.fail_next_rotation = False
        self.rotation_calls = 0

    def _stored_key(self, key: str) -> str:
        return self.cookie_name + "_" + key

    def _alive_record(self, stored_key: str) -> dict[str, Any] | None:
        record = self._records.get(stored_key)
        if record is None:
            return None
        expiry = self._expires.get(stored_key)
        if expiry is not None and expiry <= time.time():
            self._records.pop(stored_key, None)
            self._expires.pop(stored_key, None)
            return None
        return record

    async def load_session(self, request: web.Request) -> Session:
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        stored_key = self._stored_key(str(cookie))
        async with self._lock:
            record = self._alive_record(stored_key)
        if record is None or record.get("tombstone"):
            return Session(None, data=None, new=True, max_age=self.max_age)
        return Session(
            str(cookie),
            data=record,
            new=False,
            max_age=self.max_age,
            gen=record.get("gen"),
        )

    async def save_session(
        self,
        request: web.Request,
        response: web.StreamResponse,
        session: Session,
    ) -> None:
        data_str = self._encoder(self._get_session_data(session))
        data = json.loads(data_str)

        if session.identity is None:
            key = uuid.uuid4().hex
            stored_key = self._stored_key(key)
            async with self._lock:
                self._records[stored_key] = data
                if session.max_age is not None:
                    self._expires[stored_key] = time.time() + session.max_age
            self.save_cookie(
                response,
                key if not session.empty else "",
                max_age=session.max_age,
            )
            return

        key = str(session.identity)
        stored_key = self._stored_key(key)
        async with self._lock:
            record = self._alive_record(stored_key)
            revoked = record is None or record.get("tombstone") or record.get(
                "gen"
            ) != session.gen
            if revoked:
                # Late save of a revoked/expired generation: refuse silently,
                # never resurrect the old identity and leave cookies alone.
                log.warning(
                    "Refusing late save of revoked session %r", key
                )
                return
            if session.empty:
                self._records.pop(stored_key, None)
                self._expires.pop(stored_key, None)
            else:
                self._records[stored_key] = data
                if session.max_age is not None:
                    self._expires[stored_key] = time.time() + session.max_age
        if session.empty:
            self.save_cookie(response, "", max_age=session.max_age)
        else:
            self.save_cookie(response, key, max_age=session.max_age)

    async def rotate_identity(
        self,
        request: web.Request,
        session: Session,
        *,
        fields: FieldsSelector = None,
        ttl: TTLPolicy | str = TTLPolicy.PRESERVE,
    ) -> RotationResult:
        policy = self._ttl_policy(ttl)
        self.rotation_calls += 1
        if self.fail_next_rotation:
            self.fail_next_rotation = False
            raise RuntimeError("backend failure injected")

        migrated = self._select_fields(session._mapping, fields)

        async with self._lock:
            now = time.time()
            old_stored_key = (
                self._stored_key(str(session.identity))
                if session.identity is not None
                else None
            )
            old_record = None
            if old_stored_key is not None:
                old_record = self._alive_record(old_stored_key)
                if old_record is None:
                    raise IdentityRevokedError(
                        "Session identity has expired or no longer exists"
                    )
                if old_record.get("tombstone") or old_record.get(
                    "gen"
                ) != session.gen:
                    raise IdentityRevokedError(
                        "Session identity was revoked by another rotation or "
                        "invalidation"
                    )

            for _attempt in range(5):
                new_key = uuid.uuid4().hex
                new_stored_key = self._stored_key(new_key)
                if new_stored_key not in self._records:
                    break
            else:  # pragma: no cover
                raise IdentityRevokedError(
                    "Could not allocate a fresh session identity"
                )

            new_gen = uuid.uuid4().hex
            authoritative_created = (
                old_record.get("created", session.created)
                if old_record is not None
                else session.created
            )
            storage_ttl = self.max_age
            if storage_ttl is None:
                new_ttl = None
                tomb_ttl = None
            else:
                remaining = int(authoritative_created + storage_ttl - now)
                if remaining < 1:
                    remaining = 1
                tomb_ttl = remaining
                new_ttl = storage_ttl if policy is TTLPolicy.RESET else remaining
            created = (
                int(now)
                if policy is TTLPolicy.RESET
                else authoritative_created
            )
            new_record = {
                "created": created,
                "gen": new_gen,
                "session": migrated,
            }
            self._records[new_stored_key] = new_record
            if new_ttl is not None:
                self._expires[new_stored_key] = time.time() + new_ttl

            if old_stored_key is not None:
                # Revoke the old identity with a tombstone tagged with the
                # new generation. Everything above runs under one lock.
                self._records[old_stored_key] = {
                    "tombstone": True,
                    "gen": new_gen,
                    "created": session.created,
                }
                if tomb_ttl is not None:
                    self._expires[old_stored_key] = time.time() + tomb_ttl

        return RotationResult(
            identity=new_key,
            gen=new_gen,
            data=migrated,
            max_age=new_ttl,
        )

    # Test helpers -------------------------------------------------------
    def raw_record(self, key: str) -> dict[str, Any] | None:
        return self._records.get(self._stored_key(key))
