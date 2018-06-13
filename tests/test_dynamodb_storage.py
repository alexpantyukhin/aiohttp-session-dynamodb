import uuid

from aiohttp import web
from aiohttp_session import Session, session_middleware, get_session
from aiohttp_session_dynamodb import DynamoDBStorage
import json


def create_app(handler, dynamodb_client, max_age=None,
               key_factory=lambda: uuid.uuid4().hex):

    dynamo_storage = DynamoDBStorage(dynamodb_client, 'sessions',
                                     max_age=max_age, key_factory=key_factory)

    middleware = session_middleware(dynamo_storage)
    app = web.Application(middlewares=[middleware])

    app.router.add_route('GET', '/', handler)
    return app


async def make_cookie(client, dynamodb_client, data):
    session_data = json.dumps(data)
    key = uuid.uuid4().hex
    storage_key = ('AIOHTTP_SESSION_' + key)
    await dynamodb_client.update_item(
        TableName='sessions',
        Key={'key': {'S': storage_key}},
        UpdateExpression=(
            'SET session_data = :session_data'
        ),
        ExpressionAttributeValues={
            ':session_data': {'S': session_data},
        }
    )
    client.session.cookie_jar.update_cookies({'AIOHTTP_SESSION': key})


async def make_cookie_with_bad_value(client, dynamodb_client):
    key = uuid.uuid4().hex
    storage_key = ('AIOHTTP_SESSION_' + key)
    await dynamodb_client.update_item(
        TableName='sessions',
        Key={'key': {'S': storage_key}},
        UpdateExpression=(
            'SET session_data = :session_data'
        ),
        ExpressionAttributeValues={
            ':session_data': {'S': '{}'},
        }
    )
    client.session.cookie_jar.update_cookies({'AIOHTTP_SESSION': key})


async def load_cookie(client, dynamodb_client):
    cookies = client.session.cookie_jar.filter_cookies(client.make_url('/'))
    key = cookies['AIOHTTP_SESSION']
    storage_key = ('AIOHTTP_SESSION_' + key.value)
    data_row = await dynamodb_client.get_item(
        TableName='sessions',
        Key={'key': {'S': storage_key}}
    )

    return json.loads(data_row['Item']['session_data']['S'])


async def test_create_new_session(aiohttp_client, dynamodb_client):
    async def handler(request):
        session = await get_session(request)
        assert isinstance(session, Session)
        assert session.new
        assert not session._changed
        assert {} == session
        return web.Response(body=b'OK')

    client = await aiohttp_client(create_app(handler, dynamodb_client))
    resp = await client.get('/')
    assert resp.status == 200


async def test_load_existing_session(aiohttp_client, dynamodb_client):
    async def handler(request):
        session = await get_session(request)
        assert isinstance(session, Session)
        assert not session.new
        assert not session._changed
        assert {'a': 1, 'b': 12} == session
        return web.Response(body=b'OK')

    client = await aiohttp_client(create_app(handler, dynamodb_client))
    await make_cookie(client, dynamodb_client, {'a': 1, 'b': 12})
    resp = await client.get('/')
    assert resp.status == 200


async def test_load_bad_session(aiohttp_client, dynamodb_client):
    async def handler(request):
        session = await get_session(request)
        assert isinstance(session, Session)
        assert not session.new
        assert not session._changed
        assert {} == session
        return web.Response(body=b'OK')

    client = await aiohttp_client(create_app(handler, dynamodb_client))
    await make_cookie_with_bad_value(client, dynamodb_client)
    resp = await client.get('/')
    assert resp.status == 200


async def test_change_session(aiohttp_client, dynamodb_client):
    async def handler(request):
        session = await get_session(request)
        session['c'] = 3
        return web.Response(body=b'OK')

    client = await aiohttp_client(create_app(handler, dynamodb_client))
    await make_cookie(client, dynamodb_client, {'a': 1, 'b': 2})
    resp = await client.get('/')
    assert resp.status == 200

    value = await load_cookie(client, dynamodb_client)
    assert 'session' in value
    assert 'a' in value['session']
    assert 'b' in value['session']
    assert 'c' in value['session']
    assert 'created' in value
    assert value['session']['a'] == 1
    assert value['session']['b'] == 2
    assert value['session']['c'] == 3
    morsel = resp.cookies['AIOHTTP_SESSION']
    assert morsel['httponly']
    assert '/' == morsel['path']


async def test_clear_cookie_on_session_invalidation(
        aiohttp_client, dynamodb_client):
    async def handler(request):
        session = await get_session(request)
        session.invalidate()
        return web.Response(body=b'OK')

    client = await aiohttp_client(create_app(handler, dynamodb_client))
    await make_cookie(client, dynamodb_client, {'a': 1, 'b': 2})
    resp = await client.get('/')
    assert resp.status == 200

    value = await load_cookie(client, dynamodb_client)
    assert {} == value
    morsel = resp.cookies['AIOHTTP_SESSION']
    assert morsel['path'] == '/'
    assert morsel['expires'] == "Thu, 01 Jan 1970 00:00:00 GMT"
    assert morsel['max-age'] == "0"


async def test_create_cookie_in_handler(aiohttp_client, dynamodb_client):
    async def handler(request):
        session = await get_session(request)
        session['a'] = 1
        session['b'] = 2
        return web.Response(body=b'OK', headers={'HOST': 'example.com'})

    client = await aiohttp_client(create_app(handler, dynamodb_client))
    resp = await client.get('/')
    assert resp.status == 200

    value = await load_cookie(client, dynamodb_client)
    assert 'session' in value
    assert 'a' in value['session']
    assert 'b' in value['session']
    assert 'created' in value
    assert value['session']['a'] == 1
    assert value['session']['b'] == 2
    morsel = resp.cookies['AIOHTTP_SESSION']
    assert morsel['httponly']
    assert morsel['path'] == '/'

    storage_key = ('AIOHTTP_SESSION_' + morsel.value)
    data_row = await dynamodb_client.get_item(
        TableName='sessions',
        Key={'key': {'S': storage_key}}
    )

    assert 'Item' in data_row


async def test_create_new_session_if_key_doesnt_exists_in_dynamodb(
        aiohttp_client, dynamodb_client):
    async def handler(request):
        session = await get_session(request)
        assert session.new
        return web.Response(body=b'OK')

    client = await aiohttp_client(create_app(handler, dynamodb_client))
    client.session.cookie_jar.update_cookies(
        {'AIOHTTP_SESSION': 'invalid_key'})
    resp = await client.get('/')
    assert resp.status == 200


async def test_create_storage_with_custom_key_factory(
        aiohttp_client, dynamodb_client):
    async def handler(request):
        session = await get_session(request)
        session['key'] = 'value'
        assert session.new
        return web.Response(body=b'OK')

    def key_factory():
        return 'test-key'

    client = await aiohttp_client(
        create_app(handler, dynamodb_client, 8, key_factory)
    )
    resp = await client.get('/')
    assert resp.status == 200

    assert resp.cookies['AIOHTTP_SESSION'].value == 'test-key'

    value = await load_cookie(client, dynamodb_client)
    assert 'key' in value['session']
    assert value['session']['key'] == 'value'
