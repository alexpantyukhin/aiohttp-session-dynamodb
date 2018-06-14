from aiohttp_session import AbstractStorage, Session
# from datetime import datetime, timedelta
import json
import uuid

__version__ = '0.0.1'


async def create_session_table(dynamodb_client, table_name,
                               add_update_ttl=True):
    await dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {
                'AttributeName': 'key',
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

    if add_update_ttl:
        await dynamodb_client.update_time_to_live(
            TableName=table_name,
            TimeToLiveSpecification={
                'Enabled': True,
                'AttributeName': 'expires_at'
            }
        )

async def get_table_names(dynamodb_client):
    resp = await dynamodb_client.list_tables()
    return resp["TableNames"]


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
        self._table_exists = False

    async def load_session(self, request):
        self.__create_table_if_not_exists()
        cookie = self.load_cookie(request)
        if cookie is None:
            return Session(None, data=None, new=True, max_age=self.max_age)
        else:
            key = str(cookie)
            stored_key = (self.cookie_name + '_' + key)
            data_row = await self._client.get_item(
                TableName=self._table_name,
                Key={'key': {'S': stored_key}}
            )

            if data_row is None or 'Item' not in data_row:
                return Session(None, data=None,
                               new=True, max_age=self.max_age)

            try:
                data = {
                    'session':
                        self._decoder(data_row['Item']['session_data']['S'])
                }
            except ValueError:
                data = None
            return Session(key, data=data, new=False, max_age=self.max_age)

    async def save_session(self, request, response, session):
        self.__create_table_if_not_exists()
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
        stored_key = (self.cookie_name + '_' + key)
        await self._client.update_item(
            TableName=self._table_name,
            Key={'key': {'S': stored_key}},
            UpdateExpression=(
                'SET session_data = :session_data'
            ),
            ExpressionAttributeValues={
                ':session_data': {'S': data},
            }
        )

    async __create_table_if_not_exists(self):
        if self._table_exists:
            return

        tables = await get_table_names()
        if self._table_name not in tables:
            await create_session_table(self.dynamodb_client,
                                       self._table_name)

        self._table_exists = True
