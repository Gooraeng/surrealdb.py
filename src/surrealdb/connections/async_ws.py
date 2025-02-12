"""
A basic async connection to a SurrealDB instance.
"""

import asyncio
import uuid
from asyncio import Queue
from typing import Optional, Any, Dict, Union, List, AsyncGenerator
from uuid import UUID

import websockets

from surrealdb.connections.async_template import AsyncTemplate
from surrealdb.connections.url import Url
from surrealdb.connections.utils_mixin import UtilsMixin
from surrealdb.data.cbor import decode
from surrealdb.data.types.record_id import RecordID, QueryResult
from surrealdb.data.types.table import Table
from surrealdb.request_message.message import RequestMessage
from surrealdb.request_message.methods import RequestMethod


class AsyncWsSurrealConnection(AsyncTemplate, UtilsMixin):
    """
    A single async connection to a SurrealDB instance. To be used once and discarded.

    Attributes:
        url: The URL of the database to process queries for.
        user: The username to login on.
        password: The password to login on.
        namespace: The namespace that the connection will stick to.
        database: The database that the connection will stick to.
        id: The ID of the connection.
    """

    def __init__(
        self,
        url: str,
    ) -> None:
        """
        The constructor for the AsyncSurrealConnection class.

        :param url: The URL of the database to process queries for.
        """
        self.url: Url = Url(url)
        self.raw_url: str = f"{self.url.raw_url}/rpc"
        self.host: Optional[str] = self.url.hostname
        self.port: Optional[int] = self.url.port
        self.id: str = str(uuid.uuid4())
        self.token: Optional[str] = None
        self.socket = None

    async def _send(
        self,
        message: RequestMessage,
        process: str,
        bypass: bool = False,
        get_result: bool = False,
    ) -> Any:

        # will always not be None as the self.connect ensures there's a connection.
        await self.connect()
        await self.socket.send(message.WS_CBOR_DESCRIPTOR)
        response = decode(await self.socket.recv())
        if bypass is False:
            self.check_response_for_error(response, process)
        if get_result:
            return self.check_response_for_result(response, process)
        return response

    async def connect(self, url: Optional[str] = None) -> None:
        # overwrite params if passed in
        if url is not None:
            self.url = Url(url)
            self.raw_url = f"{self.url.raw_url}/rpc"
            self.host = self.url.hostname
            self.port = self.url.port
        if self.socket is None:
            self.socket = await websockets.connect(
                self.raw_url,
                max_size=None,
                subprotocols=[websockets.Subprotocol("cbor")],
            )

    async def authenticate(self, token: str) -> dict:
        message = RequestMessage(self.id, RequestMethod.AUTHENTICATE, token=token)
        return await self._send(message, "authenticating", get_result=True)

    async def invalidate(self) -> None:
        message = RequestMessage(self.id, RequestMethod.INVALIDATE)
        await self._send(message, "invalidating")
        self.token = None

    async def signup(self, vars: Dict) -> str:
        message = RequestMessage(self.id, RequestMethod.SIGN_UP, data=vars)
        return await self._send(message, "signup", get_result=True)

    async def signin(self, vars: Dict[str, Any]) -> str:
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

    async def info(self) -> dict | None:
        message = RequestMessage(self.id, RequestMethod.INFO)
        return await self._send(
            message, "getting database information", get_result=True
        )

    async def use(self, namespace: str, database: str) -> None:
        message = RequestMessage(
            self.id,
            RequestMethod.USE,
            namespace=namespace,
            database=database,
        )
        return await self._send(message, "use")

    async def query(self, query: str, params: Optional[dict] = None) -> dict:
        if params is None:
            params = {}
        message = RequestMessage(
            self.id,
            RequestMethod.QUERY,
            query=query,
            params=params,
        )
        response = await self._send(message, "query", get_result=True)
        return response[0]["result"]

    async def query_raw(self, query: str, params: Optional[dict] = None) -> dict:
        if params is None:
            params = {}
        message = RequestMessage(
            self.id,
            RequestMethod.QUERY,
            query=query,
            params=params,
        )
        return await self._send(message, "query", bypass=True)

    async def version(self) -> str:
        message = RequestMessage(self.id, RequestMethod.VERSION)
        return await self._send(message, "getting database version", get_result=True)

    async def let(self, key: str, value: Any) -> None:
        message = RequestMessage(self.id, RequestMethod.LET, key=key, value=value)
        await self._send(message, "letting")

    async def unset(self, key: str) -> None:
        message = RequestMessage(self.id, RequestMethod.UNSET, params=[key])
        await self._send(message, "unsetting")

    async def select(self, thing: Union[str, RecordID, Table]) -> QueryResult:
        message = RequestMessage(self.id, RequestMethod.SELECT, params=[thing])
        return await self._send(message, "select", get_result=True)

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

    async def update(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.UPDATE, record_id=thing, data=data
        )
        return await self._send(message, "update", get_result=True)

    async def merge(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.MERGE, record_id=thing, data=data
        )
        return await self._send(message, "merge", get_result=True)

    async def patch(
        self, thing: Union[str, RecordID, Table], data: Optional[List[dict]] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.PATCH, collection=thing, params=data
        )
        return await self._send(message, "patch", get_result=True)

    async def delete(self, thing: Union[str, RecordID, Table]) -> QueryResult:
        message = RequestMessage(self.id, RequestMethod.DELETE, record_id=thing)
        return await self._send(message, "delete", get_result=True)

    async def insert(
        self, table: Union[str, Table], data: Union[QueryResult]
    ) -> QueryResult:
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

    async def live(self, table: Union[str, Table], diff: bool = False) -> UUID:
        message = RequestMessage(
            self.id,
            RequestMethod.LIVE,
            table=table,
        )
        return await self._send(message, "live", get_result=True)

    async def subscribe_live(
        self, query_uuid: Union[str, UUID]
    ) -> AsyncGenerator[dict, None]:
        result_queue = Queue()

        async def listen_live():
            """
            Listen for live updates from the WebSocket and put them into the queue.
            """
            try:
                while True:
                    response = decode(await self.socket.recv())
                    if response.get("result", {}).get("id") == query_uuid:
                        await result_queue.put(response["result"]["result"])
            except Exception as e:
                print("Error in live subscription:", e)
                await result_queue.put({"error": str(e)})

        asyncio.create_task(listen_live())

        while True:
            result = await result_queue.get()
            if "error" in result:
                raise Exception(f"Error in live subscription: {result['error']}")
            yield result

    async def kill(self, query_uuid: Union[str, UUID]) -> None:
        message = RequestMessage(self.id, RequestMethod.KILL, uuid=query_uuid)
        await self._send(message, "kill")

    async def upsert(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.UPSERT, record_id=thing, data=data
        )
        return await self._send(message, "upsert", get_result=True)

    async def close(self):
        await self.socket.close()

    async def __aenter__(self) -> "AsyncWsSurrealConnection":
        """
        Asynchronous context manager entry.
        Initializes a websocket connection and returns the connection instance.
        """
        self.socket = await websockets.connect(
            self.raw_url, max_size=None, subprotocols=[websockets.Subprotocol("cbor")]
        )
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        """
        Asynchronous context manager exit.
        Closes the websocket connection upon exiting the context.
        """
        if self.socket is not None:
            await self.socket.close()
