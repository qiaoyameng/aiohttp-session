.. _aiohttp-session-reference:


===========
 Reference
===========

.. module:: aiohttp_session
.. currentmodule:: aiohttp_session
.. highlight:: python


Public functions
================

.. function:: get_session(request)

   A :ref:`coroutine<coroutine>` for getting session instance from
   request object.

   See example below in :ref:`Session<aiohttp-session-session>`
   section for :func:`get_session` usage.

.. function:: new_session(request)

   A :ref:`coroutine<coroutine>` for getting a new session regardless
   of whether a cookie exists.

   .. warning::

      Always use :func:`new_session` instead of :func:`get_session`
      in your login views to guard against Session Fixation attacks!

   Example usage::

      from aiohttp_session import new_session

      async def handler(request):
          session = await new_session(request)
          session.new == True # This will always be True

.. function:: session_middleware(storage)

   Session middleware factory.

   Create session middleware to pass into
   :class:`aiohttp.web.Application` constructor.

   *storage* is a session storage instance (object used to store
   session data into cookies, Redis, database etc., class is derived
   from :class:`AbstractStorage`).

   .. seealso:: :ref:`aiohttp-session-storage`

   .. note:: :func:`setup` is new-fashion way for library setup.

.. function:: setup(app, storage)

   Setup session support for given *app*.

   The function is shortcut for::

      app.middlewares.append(session_middleware(storage))

   *app* is :class:`aiohttp.web.Application` instance.

   *storage* is a session storage instance (object used to store
   session data into cookies, Redis, database etc., class is derived
   from :class:`AbstractStorage`).

   .. seealso:: :ref:`aiohttp-session-storage`


.. _aiohttp-session-session:

Session
=======

.. class:: Session

   Client's session, a namespace that is valid for some period of
   continual activity that can be used to represent a user's
   interaction with a web application.

   .. warning::

      Never create :class:`Session` instances by hands, retieve those
      by :func:`get_session` call.

   The :class:`Session` is a :class:`MutableMapping`, thus it supports
   all dictionary methods, along with some extra attributes and
   methods::

      from aiohttp_session import get_session

      async def handler(request):
          session = await get_session(request)
          session['key1'] = 'value 1'
          assert 'key2' in session
          assert session['key2'] == 'value 2'
          # ...

   .. attribute:: created

      Creation UNIX TIMESTAMP, the value returned by :func:`time.time`
      for very first access to the session object.

   .. attribute:: identity

      Client's identity. It may be cookie name or database
      key. Read-only property. For change use :func:`Session.set_new_identity`.

   .. attribute:: new

      A boolean. If new is ``True``, this session is new. Otherwise,
      it has been constituted from data that was already serialized.

   .. method:: changed()

      Call this when you mutate a mutable value in the session
      namespace. See the note below for details on when, and why
      you should call this.

      .. note::

         Keys and values of session data must be JSON serializable
         when using one of the included storage backends. This
         means, typically, that they are instances of basic types of
         objects, such as strings, lists, dictionaries, tuples,
         integers, etc. If you place an object in a session data key
         or value that is not JSON serializable, an error will be raised
         when the session is serialized.

         If you place a mutable value (for example, a list or a
         dictionary) in a session object, and you subsequently mutate
         that value, you must call the changed() method of the session
         object. In this case, the session has no way to know that is
         was modified. However, when you modify a session object
         directly, such as setting a value (i.e., ``__setitem__``), or
         removing a key (e.g., ``del`` or ``pop``), the session will
         automatically know that it needs to re-serialize its data,
         thus calling :meth:`changed` is unnecessary. There is no harm
         in calling :meth:`changed` in either case, so when in doubt,
         call it after you've changed sessioning data.

   .. method:: invalidate()

      Call this when you want to invalidate the session (dump all
      data, and -- perhaps -- set a clearing cookie).

   .. method:: set_new_identity(identity)

      Call this when you want to change the :py:attr:`identity`.

      .. warning::

         Never change :py:attr:`identity` of a session which is not new.

   .. method:: rotate_identity(request, *, fields=None, ttl=TTLPolicy.PRESERVE)

      A :ref:`coroutine<coroutine>` for atomically rotating the session
      identity after a login or privilege elevation while keeping selected
      data (e.g. the shopping cart). The backend storage performs the whole
      operation atomically:

      1. a new identity with a fresh generation tag is minted;
      2. the selected *fields* are migrated to it;
      3. the old identity is revoked and keeps a **tombstone** tagged with
         the new generation.

      The tombstone blocks late saves from requests that loaded the old
      identity before the rotation: such a save is refused, so the old
      identity can never be resurrected and an old cookie can never retrieve
      the logged-in state again.

      *fields* selects which session keys migrate:

      * ``None`` (default) -- migrate every key;
      * a :class:`str` -- migrate a single key;
      * a collection of :class:`str` -- migrate the listed keys;
      * a callable ``key -> bool`` -- migrate the keys it accepts.

      *ttl* chooses the lifetime policy:

      * :data:`TTLPolicy.PRESERVE` (default) keeps the old **absolute**
        expiry deadline. The returned session and subsequent saves use the
        remaining lifetime, so rotation never extends the security
        deadline;
      * :data:`TTLPolicy.RESET` (or the string ``"reset"``) starts a fresh
        storage lifetime for the new identity. The tombstone still honours
        the old deadline.

      The session cookie is **not** written by this call. It is updated by
      :func:`session_middleware` only after the rotation succeeds and only
      when the session is saved into a non-prepared response. If the
      backend rotation fails, neither the identity nor the cookie changes.

      Calling rotation a second time with a session that still holds the
      old generation (duplicate/concurrent requests, or a request
      interleaved with :meth:`invalidate`) raises
      :class:`IdentityRevokedError`. Use :meth:`invalidate` first to log
      out without migrating data; rotating an already invalidated/empty
      session raises :class:`RuntimeError`.

      Self-contained cookie storages
      (:class:`SimpleCookieStorage`, :class:`~aiohttp_session.cookie_storage.EncryptedCookieStorage`,
      :class:`~aiohttp_session.nacl_storage.NaClCookieStorage`) keep no
      server side state: they cannot revoke an already issued cookie and
      therefore raise :class:`RotationNotSupportedError`. Use a server side
      storage (:class:`~aiohttp_session.redis_storage.RedisStorage`) for
      privilege changing operations.

      Example::

         from aiohttp_session import get_session, TTLPolicy

         async def login(request):
             session = await get_session(request)
             # ... authenticate the user ...
             await session.rotate_identity(
                 request,
                 fields={"cart"},          # keep the shopping cart
                 ttl=TTLPolicy.PRESERVE,
             )
             session["user_id"] = user.id
             return web.Response(text="welcome")

      .. versionadded:: 2.13

   .. attribute:: gen

      Opaque generation tag (:class:`str` or ``None``) of the current
      identity, assigned by server side storages. It changes on every
      :meth:`rotate_identity` and is used internally to reject stale
      saves. ``None`` for new sessions and cookie storages.

      .. versionadded:: 2.13

.. _aiohttp-session-storage:

Session storages
================

:mod:`aiohttp_session` uses storages to save/load persistend session data.

Abstract Storage
----------------

All storages should be derived from :class:`AbstractStorage` and
implement both :meth:`~AbstractStorage.load_session` and
:meth:`~AbstractStorage.save_session` methods.

.. class:: AbstractStorage(cookie_name="AIOHTTP_SESSION", *, \
                           domain=None, max_age=None, path='/', \
                           secure=None, httponly=True, samesite=None, \
                           encoder=json.dumps, decoder=json.loads)

   Base class for session storage implementations.

   It uses HTTP cookie for storing at least the key for session data, but
   some implementations may save all session info into cookies.

   *cookie_name* -- name of cookie used for saving session data.

   *domain* -- cookie's domain, :class:`str` or ``None``.

   *max_age* -- cookie's max age, :class:`int` or ``None``.

   *path* -- cookie's path, :class:`str` or ``None``.

   *secure* -- cookie's secure flag, :class:`bool` or ``None`` (the
   same as ``False``).

   *httponly* -- cookie's http-only flag, :class:`bool` or ``None`` (the
   same as ``False``).

   *samesite* -- cookie's samesite flag, :class:`str` or ``None``

   *encoder* -- session serializer.  A callable with the following
   signature: `def encode(param: Any) -> str: ...`.  Default is
   :func:`json.dumps`.

   *decoder* -- session deserializer.  A callable with the following
   signature: `def decode(param: str) -> Any: ...`.  Default is
   :func:`json.loads`.

   .. versionadded:: 2.3

      Added *encoder* and *decoder* parameters.

   .. attribute:: max_age

      Maximum age for session data, :class:`int` seconds or ``None``
      for "session cookie" which last until you close your browser.

   .. attribute:: cookie_name

      Name of cookie used for saving session data.

   .. attribute:: cookie_params

      :class:`dict` of cookie params: *domain*, *max_age*, *path*,
      *secure*, *httponly* and *samesite*.

   .. attribute:: encoder

      The JSON serializer that will be used to dump session cookie data.

      .. versionadded:: 2.3

   .. attribute:: decoder

      The JSON deserializer that will be used to load session cookie data.

      .. versionadded:: 2.3

   .. method:: new_session()

      A :ref:`coroutine<coroutine>` for getting a new session regardless
      of whether a cookie exists.

      Return :class:`Session` instance.

   .. method:: load_session(request)

      An *abstract* :ref:`coroutine<coroutine>`, called by internal
      machinery for retrieving :class:`Session` object for given
      *request* (:class:`aiohttp.web.Request` instance).

      Return :class:`Session` instance.

   .. method:: save_session(request, response, session)

      An *abstract* :ref:`coroutine<coroutine>`, called by internal
      machinery for storing *session* (:class:`Session`) instance for
      given *request* (:class:`aiohttp.web.Request`) using *response*
      (:class:`aiohttp.web.StreamResponse` or descendants).

      Implementations that participate in identity rotation must refuse
      a save whose generation no longer matches the backend state (a
      tombstone created by :meth:`rotate_identity` or :meth:`Session.invalidate`):
      the late write must be ignored instead of resurrecting the revoked
      identity, and no cookie should be emitted for it.

   .. method:: rotate_identity(request, session, *, fields=None, ttl=TTLPolicy.PRESERVE)

      A :ref:`coroutine<coroutine>` for the atomic identity rotation
      protocol. Server side storages override this to perform, in one
      backend atomic operation: mint a new identity with a fresh
      generation, copy the keys selected by *fields* (``None``, a
      :class:`str`, a collection of :class:`str` or a predicate) and
      revoke the old identity with a tombstone carrying the new
      generation.

      *ttl* is a :class:`TTLPolicy`: :data:`TTLPolicy.PRESERVE` keeps the
      old absolute deadline (including on later saves),
      :data:`TTLPolicy.RESET` grants a fresh lifetime while the tombstone
      still expires at the old deadline.

      Returns a :class:`RotationResult` mapping with the new ``identity``,
      ``gen``, migrated ``data`` and the effective cookie ``max_age``.
      Raises :class:`IdentityRevokedError` when the old identity was
      already rotated or invalidated.

      The default implementation raises
      :class:`RotationNotSupportedError`, which is the correct behaviour
      for self-contained cookie storages: they make no server side
      revocation promise.

      .. versionadded:: 2.13

   .. class:: TTLPolicy

      Enum with two members:

      * :data:`TTLPolicy.PRESERVE` -- keep the absolute expiry deadline
        across rotation;
      * :data:`TTLPolicy.RESET` -- start a fresh lifetime for the new
        identity.

      .. versionadded:: 2.13

   .. exception:: SessionRotationError

      Base class for identity rotation failures (a :class:`RuntimeError`).

      .. versionadded:: 2.13

   .. exception:: IdentityRevokedError

      The target identity was revoked by a rotation or invalidation, or
      expired. Raised on duplicate rotations and used to guard stale
      writes. Subclass of :class:`SessionRotationError`.

      .. versionadded:: 2.13

   .. exception:: RotationNotSupportedError

      The storage has no server side state and cannot revoke identities
      (self-contained cookie storages). Subclass of
      :class:`SessionRotationError`.

      .. versionadded:: 2.13

   .. method:: load_cookie(request)

      A helper for loading cookie (:class:`http.cookies.SimpleCookie`
      instance) from *request* (:class:`aiohttp.web.Request`).

   .. method:: save_cookie(response, cookie_data, *, max_age=None)

      A helper for saving *cookie_data* (:class:`str`) into *response*
      (:class:`aiohttp.web.StreamResponse` or descendants).

      *max_age* is cookie lifetime given from session. Storage default
      is used if the value is ``None``.

Simple Storage
--------------

For testing purposes there is :class:`SimpleCookieStorage`. It stores
session data as unencrypted and unsigned JSON data in browser cookies,
so it's totally insecure.

.. warning:: Never use this storage on production!!! It's highly insecure!!!

To use the storage you should push it into
:func:`session_middleware`::

   aiohttp_session.setup(app, aiohttp_session.SimpleCookieStorage())

.. class:: SimpleCookieStorage(*, \
                               cookie_name="AIOHTTP_SESSION", \
                               domain=None, max_age=None, path='/', \
                               secure=None, httponly=True, samesite=None, \
                               encoder=json.dumps, decoder=json.loads)

   Create unencrypted cookie storage.

   The class is inherited from :class:`AbstractStorage`.

   Parameters are the same as for :class:`AbstractStorage`
   constructor.


.. module:: aiohttp_session.cookie_storage
.. currentmodule:: aiohttp_session.cookie_storage


Cookie Storage
--------------

The storage that saves session data in HTTP cookies as
:class:`~cryptography.fernet.Fernet` encrypted data.

To use the storage you should push it into
:func:`~aiohttp_session.session_middleware`::

   app = aiohttp.web.Application(middlewares=[
       aiohttp_session.cookie_storage.EncryptedCookieStorage(
           b'Thirty  two  length  bytes  key.')])

.. class:: EncryptedCookieStorage(secret_key, *, \
                                  cookie_name="AIOHTTP_SESSION", \
                                  domain=None, max_age=None, path='/', \
                                  secure=None, httponly=True, samesite=None, \
                                  encoder=json.dumps, decoder=json.loads)

   Create encryted cookies storage.

   The class is inherited from :class:`~aiohttp_session.AbstractStorage`.

   *secret_key* is :class:`bytes` secret key with length of 32, used
   for encoding or base-64 encoded :class:`str` one.

   Other parameters are the same as for
   :class:`~aiohttp_session.AbstractStorage` constructor.

   The storage implements :meth:`~AbstractStorage.rotate_identity`
   atomically with a Redis Lua script: the new record and the old
   identity tombstone are written in one round trip, and every save uses
   a generation compare-and-swap so that a request holding the pre-
   rotation identity cannot overwrite the new record. Call
   :meth:`Session.rotate_identity` after login or privilege elevation.

   .. versionadded:: 2.13

      Identity rotation protocol with generation tags and tombstones.

   .. note::

      For key generation use
      :meth:`cryptography.fernet.Fernet.generate_key` method.


.. module:: aiohttp_session.cookie_storage
.. currentmodule:: aiohttp_session.cookie_storage


NaCl Storage
--------------

The storage that saves session data in HTTP cookies as
:class:`~nacl.secret.SecretBox` encrypted data.

To use the storage you should push it into
:func:`~aiohttp_session.session_middleware`::

   app = aiohttp.web.Application(middlewares=[
       aiohttp_session.nacl_storage.NaClCookieStorage(
           b'Thirty  two  length  bytes  key.'])

.. class:: NaClCookieStorage(secret_key, *, \
                                  cookie_name="AIOHTTP_SESSION", \
                                  domain=None, max_age=None, path='/', \
                                  secure=None, httponly=True, samesite=None, \
                                  encoder=json.dumps, decoder=json.loads)

   Create encryted cookies storage.

   The class is inherited from :class:`~aiohttp_session.AbstractStorage`.

   *secret_key* is :class:`bytes` secret key with length of 32, used
   for encoding.

   Other parameters are the same as for
   :class:`~aiohttp_session.AbstractStorage` constructor.


.. module:: aiohttp_session.redis_storage
.. currentmodule:: aiohttp_session.redis_storage


Redis Storage
-------------

The storage that stores session data in Redis database and
keeps only Redis keys (UUIDs actually) in HTTP cookies.

It operates with Redis database via :class:`redis.asyncio.Redis`.

To use the storage you need setup it first::

   redis = await aioredis.from_url("redis://127.0.0.1:6379")
   storage = aiohttp_session.redis_storage.RedisStorage(redis)
   aiohttp_session.setup(app, storage)


.. class:: RedisStorage(redis_pool, *, \
                        cookie_name="AIOHTTP_SESSION", \
                        domain=None, max_age=None, path='/', \
                        secure=None, httponly=True, samesite=None, \
                        key_factory=lambda: uuid.uuid4().hex, \
                        encoder=json.dumps, decoder=json.loads)

   Create Redis storage for user session data.

   The class is inherited from :class:`~aiohttp_session.AbstractStorage`.

   *redis_pool* is a :class:`~redis.asyncio.Redis` which should be
   created by :func:`~redis.asyncio.from_url` call, e.g.::

      redis = await aioredis.from_url("redis://localhost:6379")
      storage = aiohttp_session.redis_storage.RedisStorage(redis)

   Other parameters are the same as for
   :class:`~aiohttp_session.AbstractStorage` constructor.


Memcached Storage
-----------------

The storage that stores session data in Memcached and
keeps only keys (UUIDs actually) in HTTP cookies.

It operates with Memcached database via :class:`aiomcache.Client`.

To use the storage you need setup it first::

   mc = aiomcache.Client('localhost', 11211)
   storage = aiohttp_session.memcached_storage.Client(mc)
   aiohttp_session.setup(app, storage)

.. versionadded:: 1.2

.. class:: MemcachedStorage(memcached_conn, *, \
                            cookie_name="AIOHTTP_SESSION", \
                            domain=None, max_age=None, path='/', \
                            secure=None, httponly=True, samesite=None, \
                            key_factory=lambda: uuid.uuid4().hex, \
                            encoder=json.dumps, decoder=json.loads)

   Create Memcached storage for user session data.

   The class is inherited from :class:`~aiohttp_session.AbstractStorage`.

   *memcached_conn* is a :class:`~aiomcache.Client` instance::

      mc = await aiomcache.Client('localhost', 6379)
      storage = aiohttp_session.memcached_storage.MemcachedStorage(mc)

   Other parameters are the same as for
   :class:`~aiohttp_session.AbstractStorage` constructor.
