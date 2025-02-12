import uuid
from typing import Optional, Any, Dict, Union, cast

import requests

from surrealdb.connections.sync_template import SyncTemplate
from surrealdb.connections.url import Url
from surrealdb.connections.utils_mixin import UtilsMixin
from surrealdb.data.cbor import decode
from surrealdb.data.types.record_id import RecordID, QueryResult
from surrealdb.data.types.table import Table
from surrealdb.request_message.message import RequestMessage
from surrealdb.request_message.methods import RequestMethod


class BlockingHttpSurrealConnection(SyncTemplate, UtilsMixin):
    def __init__(self, url: str) -> None:
        self.url: Url = Url(url)
        self.raw_url: str = url.rstrip("/")
        self.host: Optional[str] = self.url.hostname
        self.port: Optional[int] = self.url.port
        self.token: Optional[str] = None
        self.id: str = str(uuid.uuid4())
        self.namespace: Optional[str] = None
        self.database: Optional[str] = None
        self.vars: Dict[str, Any] = dict()

    def _send(
        self,
        message: RequestMessage,
        operation: str,
        bypass: bool = False,
        get_result: bool = False,
    ) -> Any:
        data = message.WS_CBOR_DESCRIPTOR
        url = f"{self.url.raw_url}/rpc"

        headers = {
            "Accept": "application/cbor",
            "Content-Type": "application/cbor",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.namespace:
            headers["Surreal-NS"] = self.namespace
        if self.database:
            headers["Surreal-DB"] = self.database

        response = requests.post(url, headers=headers, data=data, timeout=30)
        response.raise_for_status()

        raw_cbor = response.content
        data_dict = cast(Dict[str, Any], decode(raw_cbor))

        if not bypass:
            self.check_response_for_error(data_dict, operation)
        if get_result:
            return self.check_response_for_result(data_dict, operation)
        return data_dict

    def set_token(self, token: str) -> None:
        self.token = token

    def authenticate(self, token: str) -> dict:
        message = RequestMessage(self.id, RequestMethod.AUTHENTICATE, token=token)
        return self._send(message, "authenticating")

    def invalidate(self) -> None:
        message = RequestMessage(self.id, RequestMethod.INVALIDATE)
        self._send(message, "invalidating")
        self.token = None

    def signup(self, vars: Dict) -> str:
        message = RequestMessage(self.id, RequestMethod.SIGN_UP, data=vars)
        response = self._send(message, "signup", get_result=True)
        self.token = response
        return response

    def signin(self, vars: dict) -> str:
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
        return str(response)

    def info(self):
        message = RequestMessage(self.id, RequestMethod.INFO)
        return self._send(message, "getting database information", get_result=True)

    def use(self, namespace: str, database: str) -> None:
        message = RequestMessage(
            self.token,
            RequestMethod.USE,
            namespace=namespace,
            database=database,
        )
        self._send(message, "use")
        self.namespace = namespace
        self.database = database

    def query(self, query: str, params: Optional[dict] = None) -> dict:
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
        response = self._send(message, "query", get_result=True)
        return response[0]["result"]

    def query_raw(self, query: str, params: Optional[dict] = None) -> dict:
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
        return self._send(message, "query", bypass=True)

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

    def let(self, key: str, value: Any) -> None:
        self.vars[key] = value

    def unset(self, key: str) -> None:
        self.vars.pop(key)

    def merge(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.MERGE, record_id=thing, data=data
        )
        return self._send(message, "merge", get_result=True)

    def patch(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict[Any, Any]] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.PATCH, collection=thing, params=data
        )
        return self._send(message, "patch", get_result=True)

    def select(self, thing: Union[str, RecordID, Table]) -> QueryResult:
        message = RequestMessage(self.id, RequestMethod.SELECT, params=[thing])
        return self._send(message, "select", get_result=True)

    def update(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.UPDATE, record_id=thing, data=data
        )
        return self._send(message, "update", get_result=True)

    def version(self) -> str:
        message = RequestMessage(self.id, RequestMethod.VERSION)
        return self._send(message, "getting database version", get_result=True)

    def upsert(
        self, thing: Union[str, RecordID, Table], data: Optional[Dict] = None
    ) -> QueryResult:
        message = RequestMessage(
            self.id, RequestMethod.UPSERT, record_id=thing, data=data
        )
        return self._send(message, "upsert", get_result=True)

    def __enter__(self) -> "BlockingHttpSurrealConnection":
        """
        Synchronous context manager entry.
        Initializes a session for HTTP requests.
        """
        self.session = requests.Session()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Synchronous context manager exit.
        Closes the HTTP session upon exiting the context.
        """
        if hasattr(self, "session"):
            self.session.close()
