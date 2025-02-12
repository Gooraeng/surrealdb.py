"""
A basic blocking connection to a SurrealDB instance.
"""

import uuid
from typing import Optional, Any, Dict, Union, List, Generator
from uuid import UUID

import websockets
import websockets.sync.client as ws_sync

from surrealdb.connections.sync_template import SyncTemplate
from surrealdb.connections.url import Url
from surrealdb.connections.utils_mixin import UtilsMixin
from surrealdb.data.cbor import decode
from surrealdb.data.types.record_id import RecordID, QueryResult
from surrealdb.data.types.table import Table
from surrealdb.request_message.message import RequestMessage
from surrealdb.request_message.methods import RequestMethod


class BlockingWsSurrealConnection(SyncTemplate, UtilsMixin):
    """
    A single blocking connection to a SurrealDB instance. To be used once and discarded.

    Attributes:
        url: The URL of the database to process queries for.
        user: The username to login on.
        password: The password to login on.
        namespace: The namespace that the connection will stick to.
        database: The database that the connection will stick to.
        id: The ID of the connection.
    """

    def __init__(self, url: str) -> None:
        """
        The constructor for the BlockingWsSurrealConnection class.

        :param url: (str) the URL of the database to process queries for.
        """
        self.url: Url = Url(url)
        self.raw_url: str = f"{self.url.raw_url}/rpc"
        self.host: str = self.url.hostname
        self.port: int = self.url.port
        self.id: str = str(uuid.uuid4())
        self.token: Optional[str] = None
        self.socket = None

    def _send(
        self,
        message: RequestMessage,
        process: str,
        bypass: bool = False,
        get_result: bool = False,
    ) -> Any:

        # will always not be None as the self.connect ensures there's a connection.
        self.connect()
        self.socket.send(message.WS_CBOR_DESCRIPTOR)
        response = decode(self.socket.recv())
        if bypass is False:
            self.check_response_for_error(response, process)
        if get_result:
            return self.check_response_for_result(response, process)
        return response

    def connect(self, url: Optional[str] = None) -> None:
        # overwrite params if passed in
        if url is not None:
            self.url = Url(url)
            self.raw_url = f"{self.url.raw_url}/rpc"
            self.host = self.url.hostname
            self.port = self.url.port
        if self.socket is None:
            self.socket = ws_sync.connect(
                self.raw_url,
                max_size=None,
                subprotocols=[websockets.Subprotocol("cbor")],
            )

    def authenticate(self, token: str) -> dict:
        message = RequestMessage(self.id, RequestMethod.AUTHENTICATE, token=token)
        return self._send(message, "authenticating", get_result=True)

    def invalidate(self) -> None:
        message = RequestMessage(self.id, RequestMethod.INVALIDATE)
        self._send(message, "invalidating")
        self.token = None

    def signup(self, vars: Dict) -> str:
        message = RequestMessage(self.id, RequestMethod.SIGN_UP, data=vars)
        return self._send(message, "signup", get_result=True)

    def signin(self, vars: Dict[str, Any]) -> str:
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
        response = self._send(message, "signing in", get_result=True)
        self.token = response
        return response

    def info(self) -> dict:
        message = RequestMessage(self.id, RequestMethod.INFO)
        return self._send(message, "getting database information", get_result=True)

    def use(self, namespace: str, database: str) -> None:
        message = RequestMessage(
            self.id,
            RequestMethod.USE,
            namespace=namespace,
            database=database,
        )
        self._send(message, "use")

    def query(self, query: str, params: Optional[dict] = None) -> dict:
        if params is None:
            params = {}
        message = RequestMessage(
            self.id,
            RequestMethod.QUERY,
            query=query,
            params=params,
        )
        response = self._send(message, "query", get_result=True)
        return response[0]["result"]

    def query_raw(self, query: str, params: Optional[dict] = None) -> dict:
        if params is None:
            params = {}
        message = RequestMessage(
            self.id,
            RequestMethod.QUERY,
            query=query,
            params=params,
        )
        return self._send(message, "query", bypass=True)

    def version(self) -> str:
        message = RequestMessage(self.id, RequestMethod.VERSION)
        return self._send(message, "getting database version", get_result=True)

    def let(self, key: str, value: Any) -> None:
        message = RequestMessage(self.id, RequestMethod.LET, key=key, value=value)
        self._send(message, "letting")

    def unset(self, key: str) -> None:
        message = RequestMessage(self.id, RequestMethod.UNSET, params=[key])
        self._send(message, "unsetting")

    def select(self, thing: Union[str, RecordID, Table]) -> QueryResult:
        message = RequestMessage(self.id, RequestMethod.SELECT, params=[thing])
        return self._send(message, "select", get_result=True)

    def create(
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
        return self._send(message, "create", get_result=True)

    def live(self, table: Union[str, Table], diff: bool = False) -> UUID:
        message = RequestMessage(
            self.id,
            RequestMethod.LIVE,
            table=table,
        )
        return self._send(message, "live", get_result=True)

    def kill(self, query_uuid: Union[str, UUID]) -> None:
        message = RequestMessage(self.id, RequestMethod.KILL, uuid=query_uuid)
        self._send(message, "kill")

    def delete(self, thing: Union[str, RecordID, Table]) -> QueryResult:
        message = RequestMessage(self.id, RequestMethod.DELETE, record_id=thing)
        return self._send(message, "delete", get_result=True)

    def insert(self, table: Union[str, Table], data: QueryResult) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.INSERT, collection=table, params=data
        )
        return self._send(message, "insert", get_result=True)

    def insert_relation(
        self, table: Union[str, Table], data: QueryResult
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.INSERT_RELATION, table=table, params=data
        )
        return self._send(message, "insert_relation", get_result=True)

    def merge(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.MERGE, record_id=thing, data=data
        )
        return self._send(message, "merge", get_result=True)

    def patch(
        self, thing: Union[str, RecordID, Table], data: Optional[List[dict]] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.PATCH, collection=thing, params=data
        )
        return self._send(message, "patch", get_result=True)

    def subscribe_live(
        self, query_uuid: Union[str, UUID]
    ) -> Generator[dict, None, None]:
        """
        Subscribe to live updates for a given query UUID.

        Args:
            query_uuid (Union[str, UUID]): The query UUID to subscribe to.

        Yields:
            dict: The results of live updates.
        """
        try:
            while True:
                try:
                    # Receive a message from the WebSocket
                    response = decode(self.socket.recv())

                    # Check if the response matches the query UUID
                    if response.get("result", {}).get("id") == query_uuid:
                        yield response["result"]["result"]
                except Exception as e:
                    # Handle WebSocket or decoding errors
                    print("Error in live subscription:", e)
                    yield {"error": str(e)}
        except GeneratorExit:
            # Handle generator exit gracefully if needed
            pass

    def update(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.UPDATE, record_id=thing, data=data
        )
        return self._send(message, "update", get_result=True)

    def upsert(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.UPSERT, record_id=thing, data=data
        )
        return self._send(message, "upsert", get_result=True)

    def close(self):
        self.socket.close()

    def __enter__(self) -> "BlockingWsSurrealConnection":
        """
        Synchronous context manager entry.
        Initializes a websocket connection and returns the connection instance.
        """
        self.socket = ws_sync.connect(
            self.raw_url, max_size=None, subprotocols=[websockets.Subprotocol("cbor")]
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Synchronous context manager exit.
        Closes the websocket connection upon exiting the context.
        """
        if self.socket is not None:
            self.socket.close()
