aiohttp_session_dynamodb
===============
.. image:: https://travis-ci.com/alexpantyukhin/aiohttp-session-dynamodb.svg?branch=master
    :target: https://travis-ci.org/alexpantyukhin/aiohttp-session-dynamodb
.. image:: https://codecov.io/github/alexpantyukhin/aiohttp-session-dynamodb/coverage.svg?branch=master
    :target: https://codecov.io/github/alexpantyukhin/aiohttp-session-dynamodb

The library provides dynamo sessions store for `aiohttp.web`__.

.. _aiohttp_web: https://aiohttp.readthedocs.io/en/latest/web.html

__ aiohttp_web_

Usage
-----

A trivial usage example:

.. code:: python

    import time
    import base64
    from cryptography import fernet
    from aiohttp import web
    from aiohttp_session import setup, get_session
    from aiohttp_session_dynamodb import DynamoDBStorage
    import motor.motor_asyncio as aiomotor
    import asyncio


    async def handler(request):
        session = await get_session(request)
        last_visit = session['last_visit'] if 'last_visit' in session else None
        session['last_visit'] = time.time()
        text = 'Last visited: {}'.format(last_visit)
        return web.Response(text=text)


    def init_dynamo(loop):

        async def init_dynamo(loop):
            url = "dynamodb://localhost:27017"
            conn = aiomotor.AsyncIOMotorClient(
                url, maxPoolSize=2, io_loop=loop)
            return conn

        conn = loop.run_until_complete(init_dynamo(loop))

        db = 'my_db'
        return conn[db]


    async def setup_dynamo(app, loop):
        dynamo = init_dynamo(loop)

        async def close_dynamo(app):
            dynamo.client.close()

        app.on_cleanup.append(close_dynamo)
        return dynamo


    def make_app():
        app = web.Application()
        loop = asyncio.get_event_loop()

        dynamo = setup_dynamo(app, loop)
        session_collection = dynamo['sessions']

        setup(app, DynamoDBStorage(session_collection,
                                max_age=max_age,
                                key_factory=lambda: uuid.uuid4().hex)
                                )

        app.router.add_get('/', handler)
        return app


    web.run_app(make_app())

