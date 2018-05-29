import asyncio
import gc
import pytest
import sys
import time
import uuid
from docker import from_env as docker_from_env
import socket
import aiobotocore
from botocore.exceptions import ClientError


@pytest.fixture(scope='session')
def unused_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope='session')
def loop(request):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    yield loop

    if not loop._closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()
    gc.collect()
    asyncio.set_event_loop(None)


@pytest.fixture(scope='session')
def session_id():
    """Unique session identifier, random string."""
    return str(uuid.uuid4())


@pytest.fixture(scope='session')
def docker():
    client = docker_from_env(version='auto')
    return client


@pytest.fixture(scope='session')
def dynamodb_server(docker, session_id, loop, request):
    image = 'dwmkerr/dynamodb:{}'.format('latest')

    if sys.platform.startswith('darwin'):
        port = unused_port()
    else:
        port = None

    container = docker.containers.run(
        image=image,
        detach=True,
        name='dynamodb-test-server-{}-{}'.format('latest', session_id),
        ports={
            '8000/tcp': port,
        },
        environment={
            'http.host': '0.0.0.0',
            'transport.host': '127.0.0.1',
        },
    )

    if sys.platform.startswith('darwin'):
        host = '0.0.0.0'
    else:
        inspection = docker.api.inspect_container(container.id)
        host = inspection['NetworkSettings']['IPAddress']
        port = 8000

    delay = 0.1
    for i in range(20):
        try:
            session = aiobotocore.get_session(loop=loop)
            client = session.create_client(
                'dynamodb',
                region_name='eu-west-1',
                endpoint_url="http://{}:{}".format(host, port),
                aws_access_key_id='',
                aws_secret_access_key='')

            loop.run_until_complete(client.list_tables())
            break
        except ClientError as e:
            time.sleep(delay)
            delay *= 2
    else:
        pytest.fail("Cannot start dynamodb server")

    yield {'host': host,
           'port': port,
           'container': container}

    container.kill(signal=9)
    container.remove(force=True)


@pytest.fixture
def dynamodb_params(dynamodb_server):
    return dict(host=dynamodb_server['host'],
                port=dynamodb_server['port'])


@pytest.fixture
def dynamodb(loop, dynamodb_params):
    async def init_dynamodb(loop):
        session = aiobotocore.get_session(loop=loop)
        client = session.create_client(
            'dynamodb',
            region_name='eu-west-1',
            endpoint_url="http://{}:{}"
            .format(dynamodb_params['host'], dynamodb_params['port']),
            aws_access_key_id='',
            aws_secret_access_key=''
        )

        return client

    client = loop.run_until_complete(init_dynamodb(loop))

    return client
