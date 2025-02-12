import uuid
from typing import Optional, Any, Dict, Union, List

import aiohttp

from surrealdb.connections.async_template import AsyncTemplate
from surrealdb.connections.url import Url
from surrealdb.connections.utils_mixin import UtilsMixin
from surrealdb.data.cbor import decode
from surrealdb.data.types.record_id import RecordID, QueryResult
from surrealdb.data.types.table import Table
from surrealdb.request_message.message import RequestMessage
from surrealdb.request_message.methods import RequestMethod


class AsyncHttpSurrealConnection(AsyncTemplate, UtilsMixin):
    """
    A single async connection to a SurrealDB instance using HTTP. To be used once and discarded.

    # Notes
    A new HTTP session is created for each query to send a request to the SurrealDB server.

    Attributes:
        url: The URL of the database to process queries for.
        id: The ID of the connection.
    """

    def __init__(
        self,
        url: str,
    ) -> None:
        """
        Constructor for the AsyncHttpSurrealConnection class.

        :param url: (str) The URL of the database to process queries for.
        """
        self.url: Url = Url(url)
        self.raw_url: str = self.url.raw_url
        self.host: str = self.url.hostname
        self.port: Optional[int] = self.url.port
        self.token: Optional[str] = None
        self.id: str = str(uuid.uuid4())
        self.namespace: Optional[str] = None
        self.database: Optional[str] = None
        self.vars = dict()

    async def _send(
        self,
        message: RequestMessage,
        operation: str,
        bypass: bool = False,
        get_result: bool = False,
    ) -> Any:
        """
        Sends an HTTP request to the SurrealDB server.

        :param endpoint: (str) The endpoint of the SurrealDB API to send the request to.
        :param method: (str) The HTTP method (e.g., "POST", "GET", "PUT", "DELETE").
        :param headers: (dict) Optional headers to include in the request.
        :param payload: (dict) Optional JSON payload to include in the request body.

        :return: (dict) The decoded JSON response from the server.
        """
        # json_body, method, endpoint = message.JSON_HTTP_DESCRIPTOR
        data = message.WS_CBOR_DESCRIPTOR
        url = f"{self.url.raw_url}/rpc"

        headers = dict()
        headers["Accept"] = "application/cbor"
        headers["content-type"] = "application/cbor"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.namespace:
            headers["Surreal-NS"] = self.namespace
        if self.database:
            headers["Surreal-DB"] = self.database

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method="POST",
                url=url,
                headers=headers,
                # json=json.dumps(json_body),
                data=data,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                response.raise_for_status()
                raw_cbor = await response.read()
                data = decode(raw_cbor)
                if bypass is False:
                    self.check_response_for_error(data, operation)
                if get_result:
                    return self.check_response_for_result(data, operation)
                return data

    def set_token(self, token: str) -> None:
        """
        Sets the token for authentication.

        :param token: (str) The token to use for the connection.
        """
        self.token = token

    async def authenticate(self) -> None:
        message = RequestMessage(self.id, RequestMethod.AUTHENTICATE, token=self.token)
        await self._send(message, "authenticating")

    async def invalidate(self) -> None:
        message = RequestMessage(self.id, RequestMethod.INVALIDATE)
        await self._send(message, "invalidating")
        self.token = None

    async def signup(self, vars: Dict) -> str:
        message = RequestMessage(self.id, RequestMethod.SIGN_UP, data=vars)
        response = await self._send(message, "signup", get_result=True)
        self.token = response
        return response

    async def signin(self, vars: dict) -> dict:
        message = RequestMessage(
            self.id,
            RequestMethod.SIGN_IN,
            username=vars.get("username"),
            password=vars.get("password"),
            access=vars.get("access"),
            database=vars.get("database"),
            namespace=vars.get("namespace"),
            variables=vars.get("variables"),
        )
        response = await self._send(message, "signing in", get_result=True)
        self.token = response
        return response

    async def info(self) -> dict:
        message = RequestMessage(self.id, RequestMethod.INFO)
        return await self._send(
            message, "getting database information", get_result=True
        )

    async def use(self, namespace: str, database: str) -> None:
        message = RequestMessage(
            self.token,
            RequestMethod.USE,
            namespace=namespace,
            database=database,
        )
        await self._send(message, "use", get_result=False)
        self.namespace = namespace
        self.database = database

    def _construct_query_message(
        self, query: str, params: Optional[dict] = None
    ) -> RequestMessage:
        if params is None:
            params = {}
        for key, value in self.vars.items():
            params[key] = value
        message = RequestMessage(
            self.id,
            RequestMethod.QUERY,
            query=query,
            params=params,
        )
        return message

    async def query(self, query: str, params: Optional[dict] = None) -> dict:
        message = self._construct_query_message(query, params)
        response = await self._send(message, "query", get_result=True)
        return response[0]["result"]

    async def query_raw(self, query: str, params: Optional[dict] = None) -> dict:
        message = self._construct_query_message(query, params)
        return await self._send(message, "query", bypass=True)

    async def create(
        self,
        thing: Union[str, RecordID, Table],
        data: Optional[QueryResult] = None,
    ) -> QueryResult:
        if isinstance(thing, str):
            if ":" in thing:
                buffer = thing.split(":")
                thing = RecordID(table_name=buffer[0], identifier=buffer[1])
        message = RequestMessage(
            self.id, RequestMethod.CREATE, collection=thing, data=data
        )
        return await self._send(message, "create", get_result=True)

    async def delete(self, thing: Union[str, RecordID, Table]) -> QueryResult:
        message = RequestMessage(self.id, RequestMethod.DELETE, record_id=thing)
        return await self._send(message, "delete", get_result=True)

    async def insert(self, table: Union[str, Table], data: QueryResult) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.INSERT, collection=table, params=data
        )
        return await self._send(message, "insert", get_result=True)

    async def insert_relation(
        self, table: Union[str, Table], data: QueryResult
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.INSERT_RELATION, table=table, params=data
        )
        return await self._send(message, "insert_relation", get_result=True)

    async def let(self, key: str, value: Any) -> None:
        self.vars[key] = value

    async def unset(self, key: str) -> None:
        self.vars.pop(key)

    async def merge(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.MERGE, record_id=thing, data=data
        )
        response = await self._send(message, "merge", get_result=True)

    async def patch(
        self, thing: Union[str, RecordID, Table], data: Optional[List[dict]] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.PATCH, collection=thing, params=data
        )
        return await self._send(message, "patch", get_result=True)

    async def select(self, thing: str) -> QueryResult:
        message = RequestMessage(self.id, RequestMethod.SELECT, params=[thing])
        return await self._send(message, "select", get_result=True)

    async def update(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.UPDATE, record_id=thing, data=data
        )
        return await self._send(message, "update", get_result=True)

    async def version(self) -> str:
        message = RequestMessage(self.id, RequestMethod.VERSION)
        return await self._send(message, "getting database version", get_result=True)

    async def upsert(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.UPSERT, record_id=thing, data=data
        )
        return await self._send(message, "upsert", get_result=True)

    async def __aenter__(self) -> "AsyncHttpSurrealConnection":
        """
        Asynchronous context manager entry.
        Initializes an aiohttp session and returns the connection instance.
        """
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        """
        Asynchronous context manager exit.
        Closes the aiohttp session upon exiting the context.
        """
        if hasattr(self, "_session"):
            await self._session.close()
