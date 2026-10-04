from __future__ import annotations

from unittest import mock

import pytest
from aiohttp import web

from aiohttp_session import (
    SESSION_KEY,
    STORAGE_KEY,
    IdentityRevokedError,
    RotationNotSupportedError,
    Session,
    SimpleCookieStorage,
    TTLPolicy,
    get_session,
    session_middleware,
)
from aiohttp_session.cookie_storage import EncryptedCookieStorage

from ._memory_storage import AtomicMemoryStorage


class FakeRequest:
    def __init__(self, storage: AtomicMemoryStorage, cookie: str | None = None):
        self._mapping: dict[object, object] = {STORAGE_KEY: storage}
        self.cookies: dict[str, str] = {}
        if cookie is not None:
            self.cookies[storage.cookie_name] = cookie

    def __getitem__(self, key: object) -> object:
        return self._mapping[key]

    def __setitem__(self, key: object, value: object) -> None:
        self._mapping[key] = value

    def get(self, key: object, default: object = None) -> object:
        return self._mapping.get(key, default)


@pytest.fixture
def storage() -> AtomicMemoryStorage:
    return AtomicMemoryStorage(max_age=1000)


async def _login(storage: AtomicMemoryStorage) -> tuple[Session, FakeRequest, str]:
    """Create a cart session and save it, return (session, request, key)."""
    mint_request = FakeRequest(storage)
    minted = await storage.load_session(mint_request)
    minted["cart"] = ["apple"]
    minted["token"] = "anon-token"
    response = web.Response()
    await storage.save_session(mint_request, response, minted)
    key = response.cookies[storage.cookie_name].value

    # Real clients come back with the cookie: reload the persisted session.
    request = FakeRequest(storage, cookie=key)
    session = await storage.load_session(request)
    assert session.identity == key
    return session, request, key


async def test_rotate_migrates_cart_and_revokes_old_identity(
    storage: AtomicMemoryStorage,
) -> None:
    session, request, old_key = await _login(storage)

    # Login: keep the cart, drop the anonymous token.
    new_identity = await session.rotate_identity(request, fields={"cart"})

    assert new_identity != old_key
    assert session.identity == new_identity
    assert dict(session) == {"cart": ["apple"]}
    assert "token" not in session

    old_record = storage.raw_record(old_key)
    assert old_record is not None
    assert old_record["tombstone"] is True
    assert old_record["gen"] == session.gen

    new_record = storage.raw_record(new_identity)
    assert new_record["session"] == {"cart": ["apple"]}
    assert new_record["gen"] == session.gen


async def test_old_cookie_cannot_retrieve_login_state(
    storage: AtomicMemoryStorage,
) -> None:
    session, request, old_key = await _login(storage)
    await session.rotate_identity(request, fields={"cart"})

    old_request = FakeRequest(storage, cookie=old_key)
    reloaded = await storage.load_session(old_request)
    assert reloaded.new is True
    assert reloaded.identity is None
    assert dict(reloaded) == {}


async def test_late_save_of_old_generation_does_not_resurrect(
    storage: AtomicMemoryStorage,
) -> None:
    # Request A and B both load the same pre-rotation identity.
    session_a, request_a, old_key = await _login(storage)
    request_b = FakeRequest(storage, cookie=old_key)
    stale_session = await storage.load_session(request_b)
    assert stale_session.identity == old_key

    # Request A performs the login rotation and gets its new cookie.
    await session_a.rotate_identity(request_a, fields={"cart"})
    response_a = web.Response()
    await storage.save_session(request_a, response_a, session_a)
    new_key = response_a.cookies[storage.cookie_name].value
    assert new_key != old_key

    # Request B finishes late and tries to save its loaded (old) snapshot.
    stale_session["cart"] = ["pwnd"]
    response_b = web.Response()
    await storage.save_session(request_b, response_b, stale_session)

    # Nothing was written under the old identity and no stale cookie set.
    assert storage.raw_record(old_key)["tombstone"] is True
    assert storage.raw_record(new_key)["session"] == {"cart": ["apple"]}
    assert storage.cookie_name not in response_b.cookies

    # The old cookie still resolves to an empty (logged out) session.
    again = await storage.load_session(FakeRequest(storage, cookie=old_key))
    assert again.new is True
    assert dict(again) == {}


async def test_rotate_defaults_to_migrating_all_fields(
    storage: AtomicMemoryStorage,
) -> None:
    session, request, _old_key = await _login(storage)
    await session.rotate_identity(request)
    assert dict(session) == {"cart": ["apple"], "token": "anon-token"}


async def test_rotate_single_string_field(storage: AtomicMemoryStorage) -> None:
    session, request, _ = await _login(storage)
    await session.rotate_identity(request, fields="cart")
    assert dict(session) == {"cart": ["apple"]}


async def test_rotate_predicate_fields(storage: AtomicMemoryStorage) -> None:
    session, request, _ = await _login(storage)
    await session.rotate_identity(request, fields=lambda key: key.startswith("c"))
    assert dict(session) == {"cart": ["apple"]}


async def test_rotate_preserves_absolute_deadline(
    storage: AtomicMemoryStorage,
) -> None:
    with mock.patch("aiohttp_session.time.time", return_value=0.0):
        session, request, old_key = await _login(storage)
        original_deadline = 1000.0

        await session.rotate_identity(request, ttl=TTLPolicy.PRESERVE)

        new_key = session.identity
        assert storage._expires[storage._stored_key(new_key)] == pytest.approx(
            original_deadline, abs=1
        )
        assert storage._expires[storage._stored_key(old_key)] == pytest.approx(
            original_deadline, abs=1
        )
    # max_age is carried over unchanged from the caller's perspective.
    assert session.max_age == 1000


async def test_rotate_reset_ttl(storage: AtomicMemoryStorage) -> None:
    with mock.patch("aiohttp_session.time.time", return_value=0.0):
        session, request, old_key = await _login(storage)

    # 600s into the old session: RESET grants a fresh full lifetime, but the
    # tombstone must still expire at the old absolute deadline.
    with mock.patch("aiohttp_session.time.time", return_value=600.0):
        await session.rotate_identity(request, ttl="reset")

        new_key = str(session.identity)
        assert storage._expires[storage._stored_key(new_key)] == pytest.approx(
            1600.0, abs=1
        )
        assert storage._expires[storage._stored_key(old_key)] == pytest.approx(
            1000.0, abs=1
        )
    assert session.max_age == 1000


async def test_preserve_rotation_subsequent_save_does_not_extend_deadline(
    storage: AtomicMemoryStorage,
) -> None:
    with mock.patch("aiohttp_session.time.time", return_value=0.0):
        session, request, _ = await _login(storage)

    with mock.patch("aiohttp_session.time.time", return_value=600.0):
        await session.rotate_identity(request, ttl=TTLPolicy.PRESERVE)
        assert session.max_age == 400
        new_key = str(session.identity)

        # A normal post-login save must keep the remaining 400s deadline.
        session["user"] = "bob"
        response = web.Response()
        await storage.save_session(request, response, session)
        assert storage._expires[storage._stored_key(new_key)] == pytest.approx(
            1000.0, abs=1
        )
        cookie = response.cookies[storage.cookie_name]
        assert "00:16:40" in cookie["expires"]


async def test_duplicate_concurrent_rotation_second_fails(
    storage: AtomicMemoryStorage,
) -> None:
    session_a, request_a, old_key = await _login(storage)
    request_b = FakeRequest(storage, cookie=old_key)
    session_b = await storage.load_session(request_b)

    await session_a.rotate_identity(request_a, fields={"cart"})

    with pytest.raises(IdentityRevokedError):
        await session_b.rotate_identity(request_b, fields={"cart"})


async def test_sequential_rotation_chains_tombstones(
    storage: AtomicMemoryStorage,
) -> None:
    session, request, first_key = await _login(storage)
    second_key = await session.rotate_identity(request, fields={"cart"})
    third_key = await session.rotate_identity(request, fields={"cart"})
    assert len({first_key, second_key, third_key}) == 3
    assert storage.raw_record(first_key)["tombstone"] is True
    assert storage.raw_record(second_key)["tombstone"] is True
    assert storage.raw_record(third_key).get("tombstone") is None


async def test_rotate_then_late_invalidate_is_noop(
    storage: AtomicMemoryStorage,
) -> None:
    session, request, old_key = await _login(storage)
    stale_request = FakeRequest(storage, cookie=old_key)
    stale_session = await storage.load_session(stale_request)

    new_key = await session.rotate_identity(request, fields={"cart"})

    stale_session.invalidate()
    stale_response = web.Response()
    await storage.save_session(
        stale_request, stale_response, stale_session
    )
    assert storage.raw_record(old_key)["tombstone"] is True
    assert storage.raw_record(new_key) is not None
    assert storage.cookie_name not in stale_response.cookies


async def test_invalidate_then_rotate_raises(storage: AtomicMemoryStorage) -> None:
    session, request, _ = await _login(storage)
    session.invalidate()
    with pytest.raises(RuntimeError):
        await session.rotate_identity(request)


async def test_backend_failure_is_atomic(storage: AtomicMemoryStorage) -> None:
    session, request, old_key = await _login(storage)
    storage.fail_next_rotation = True
    with pytest.raises(RuntimeError, match="backend failure"):
        await session.rotate_identity(request, fields={"cart"})

    # Identity, generation and data are untouched after a failed rotation.
    assert session.identity == old_key
    assert dict(session) == {"cart": ["apple"], "token": "anon-token"}
    old_record = storage.raw_record(old_key)
    assert old_record.get("tombstone") is None

    # A retry after the failure succeeds.
    new_key = await session.rotate_identity(request, fields={"cart"})
    assert new_key != old_key


async def test_rotate_fresh_session_mints_identity(
    storage: AtomicMemoryStorage,
) -> None:
    request = FakeRequest(storage)
    session = await storage.load_session(request)
    session["user"] = "bob"
    new_key = await session.rotate_identity(request)
    assert storage.raw_record(new_key)["session"] == {"user": "bob"}


async def test_rotate_without_max_age_keeps_no_expiry() -> None:
    storage = AtomicMemoryStorage(max_age=None)
    mint_request = FakeRequest(storage)
    session = await storage.load_session(mint_request)
    session["user"] = "bob"
    response = web.Response()
    await storage.save_session(mint_request, response, session)
    old_key = response.cookies[storage.cookie_name].value

    request = FakeRequest(storage, cookie=old_key)
    loaded = await storage.load_session(request)
    new_key = await loaded.rotate_identity(request)

    assert storage._expires == {}
    assert storage.raw_record(old_key)["tombstone"] is True
    assert storage.raw_record(new_key)["session"] == {"user": "bob"}


async def test_cookie_storages_do_not_support_rotation() -> None:
    request = FakeRequest(SimpleCookieStorage())
    session = Session("old", data={"session": {"a": 1}, "created": 0}, new=False)
    request[STORAGE_KEY] = SimpleCookieStorage()
    with pytest.raises(RotationNotSupportedError):
        await session.rotate_identity(request)

    request[STORAGE_KEY] = EncryptedCookieStorage(
        b"Thirty  two  length  bytes  key."
    )
    with pytest.raises(RotationNotSupportedError):
        await session.rotate_identity(request)


async def test_cookie_only_set_after_successful_rotation(
    storage: AtomicMemoryStorage,
) -> None:
    session, request, old_key = await _login(storage)
    storage.fail_next_rotation = True
    response = web.Response()
    with pytest.raises(RuntimeError):
        await session.rotate_identity(request, fields={"cart"})
    # rotate itself never touches any response cookie.
    assert storage.cookie_name not in response.cookies


async def test_save_into_prepared_response_rejected(
    storage: AtomicMemoryStorage,
) -> None:
    request = FakeRequest(storage)
    request[STORAGE_KEY] = storage
    middleware = session_middleware(storage)

    async def handler(req: web.Request) -> web.StreamResponse:
        session = await get_session(req)
        session["cart"] = ["apple"]
        await session.rotate_identity(req, fields={"cart"})
        response = web.Response()
        response._payload_writer = mock.Mock()
        with mock.patch.object(
            type(response),
            "prepared",
            new_callable=mock.PropertyMock,
        ) as prepared:
            prepared.return_value = True
            return response

    with pytest.raises(RuntimeError, match="prepared response"):
        await middleware(request, handler)


def test_unknown_ttl_policy_rejected(storage: AtomicMemoryStorage) -> None:
    with pytest.raises(ValueError):
        storage._ttl_policy("banana")  # type: ignore[arg-type]


def test_rotation_ttl_helper_preserve_and_reset() -> None:
    storage = AtomicMemoryStorage(max_age=1000)
    with mock.patch("aiohttp_session.time.time", return_value=600):
        old = Session("k", data={"created": 0, "session": {"a": 1}}, new=False)
        old.max_age = 1000
        new_ttl, tomb_ttl = storage._rotation_ttls(old, TTLPolicy.PRESERVE)
        assert new_ttl == 400
        assert tomb_ttl == 400
        new_ttl, tomb_ttl = storage._rotation_ttls(old, TTLPolicy.RESET)
        assert new_ttl == 1000
        assert tomb_ttl == 400

    no_ttl = AtomicMemoryStorage(max_age=None)
    old.max_age = None
    assert no_ttl._rotation_ttls(old, TTLPolicy.PRESERVE) == (None, None)


def test_get_session_helper_unchanged(storage: AtomicMemoryStorage) -> None:
    # Sanity: SESSION_KEY/STORAGE_KEY contracts still line up for FakeRequest.
    request = FakeRequest(storage)
    assert request.get(STORAGE_KEY) is storage
    assert request.get(SESSION_KEY) is None
