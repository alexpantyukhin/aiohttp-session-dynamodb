from aiohttp_session import AbstractStorage, Session
# from datetime import datetime, timedelta
import json
import uuid

__version__ = '0.0.1'


async def create_session_table(dynamodb_client, table_name):
    await dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {
                'AttributeName': 'key',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'expires_at',
                'AttributeType': 'S'
            }
        ],
        KeySchema=[
            {
                'AttributeName': 'key',
                'KeyType': 'HASH'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    waiter = dynamodb_client.get_waiter('table_exists')
    await waiter.wait(TableName=table_name)

    await dynamodb_client.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={
            'Enabled': True,
            'AttributeName': 'expires_at'
        }
    )


class DynamoDBStorage(AbstractStorage):
    def __init__(self, client, table_name, *, cookie_name="AIOHTTP_SESSION",
                 domain=None, max_age=None, path='/',
                 secure=None, httponly=True,
                 key_factory=lambda: uuid.uuid4().hex,
                 encoder=json.dumps, decoder=json.loads):
        super().__init__(cookie_name=cookie_name, domain=domain,
                         max_age=max_age, path=path, secure=secure,
                         httponly=httponly,
                         encoder=encoder, decoder=decoder)

        self._client = client
        self._table_name = table_name
        self._key_factory = key_factory
        self._expire_index_created = False

    async def load_session(self, request):
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        else:
            key = str(cookie)
            stored_key = (self.cookie_name + '_' + key).encode('utf-8')
            data_row = await self._client.get_item(
                TableName=self._table_name,
                Key={'pk': {'S': stored_key}}
            )

            if data_row is None:
                return Session(None, data=None,
                               new=True, max_age=self.max_age)

            try:
                data = self._decoder(data_row['data'])
            except ValueError:
                data = None
            return Session(key, data=data, new=False, max_age=self.max_age)

    async def save_session(self, request, response, session):
        key = session.identity
        if key is None:
            key = self._key_factory()
            self.save_cookie(response, key,
                             max_age=session.max_age)
        else:
            if session.empty:
                self.save_cookie(response, '',
                                 max_age=session.max_age)
            else:
                key = str(key)
                self.save_cookie(response, key,
                                 max_age=session.max_age)

        data = self._encoder(self._get_session_data(session))
        # expire = datetime.utcnow() + timedelta(seconds=session.max_age) \
        #    if session.max_age is not None else None
        stored_key = (self.cookie_name + '_' + key).encode('utf-8')
        await self._table.update_item(Item={'pk': stored_key, 'data': data})
