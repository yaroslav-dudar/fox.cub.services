# pylint: disable=E1101
"""Pymongo base wrapper"""

import atexit
import asyncio
from typing import Any, ClassVar, Type, Union

import pymongo
import asyncpg

from config import Config


class Connection:
    SUPPORT_CONNECTIONS = (pymongo.MongoClient, asyncpg.pool.Pool)

    def __set_name__(self, owner: Any, name: str) -> None:
        self.name = name

    def __set__(
        self, instance: Any, value: Union[pymongo.MongoClient, asyncpg.pool.Pool]
    ) -> None:
        if not isinstance(value, self.SUPPORT_CONNECTIONS):
            raise ValueError(f"Connection: {value.__class__} is not supported.")

        instance.__dict__[self.name] = value

    def __get__(
        self, instance: Any, owner: Any
    ) -> Union[pymongo.MongoClient, asyncpg.pool.Pool]:
        if not instance:
            return None

        return instance.__dict__.get(self.name)


class MongoClient:
    """Global MongoDB connector"""

    conn = Connection()
    db = None
    _obj = None

    def __new__(cls, *args: list[Any], **kwargs: dict[str, Any]) -> "MongoClient":
        if cls._obj:
            # prevent to create multiple db connections
            return cls._obj

        cls._obj = super(MongoClient, cls).__new__(cls)
        return cls._obj

    def __init__(self, db_config: dict):
        auth_config = {}
        if db_config.get("authMechanism") == "SCRAM-SHA-1":
            auth_config["authMechanism"] = db_config["authMechanism"]
            auth_config["authSource"] = db_config["authSource"]
            auth_config["username"] = db_config["username"]
            auth_config["password"] = db_config["password"]

        self.conn = pymongo.MongoClient(
            host=db_config["host"], port=db_config["port"], **auth_config
        )
        MongoClient.db = self.conn[db_config["db_name"]]
        # clean-up DB resources
        atexit.register(self.conn.close)


class BaseModel(type):

    db_session: ClassVar[pymongo.collection.Collection]

    def __new__(
        cls: Type["BaseModel"], name: str, bases: tuple[type, ...], attr: dict[str, Any]
    ) -> "BaseModel":
        attr["client"] = MongoClient(Config()["database"])

        if attr.get("capped_settings"):
            cls.setup_capped_collection(attr)

        session = attr["client"].db.get_collection(
            attr["collection"], codec_options=attr.get("codec_options")
        )

        attr["db_session"] = session

        return super().__new__(cls, name, bases, attr)

    @classmethod
    def setup_capped_collection(cls, attr: dict[str, Any]) -> None:
        settings = attr.get("capped_settings", {})
        try:
            attr["client"].db.create_collection(
                attr["collection"],
                capped=settings["capped"],
                size=settings["size"],
                max=settings["max"],
            )
        except pymongo.errors.CollectionInvalid as invalid_collection:
            stats = attr["client"].db.command("collStats", attr["collection"])
            # verifing that collection is capped
            if not stats.get("capped"):
                raise RuntimeError(
                    "{} should be capped collection".format(stats["ns"])
                ) from invalid_collection


class PgClient:
    """Global PostgreSQL connector"""

    conn_pool = Connection()
    db = None
    _obj = None

    def __new__(cls, *args: list[Any], **kwargs: dict[str, Any]) -> "PgClient":
        if cls._obj:
            # prevent to create multiple db connections
            return cls._obj

        cls._obj = super(PgClient, cls).__new__(cls)
        return cls._obj

    def __init__(self, db_config: dict, loop: asyncio.AbstractEventLoop):
        self.db_config = db_config
        self.loop = loop
        atexit.register(self.shutdown)

    async def init_connection(self) -> asyncpg.Pool:
        if self.conn_pool:
            return self.conn_pool

        self.conn_pool = await asyncpg.create_pool(
            user=self.db_config["user"],
            password=self.db_config["password"],
            database=self.db_config["database"],
            host=self.db_config["host"],
            port=self.db_config["port"],
            max_size=10,
            max_inactive_connection_lifetime=100,
            loop=self.loop,
        )

        return self.conn_pool

    def shutdown(self) -> None:
        """Cleanup DB resources before exit."""
        self.loop.run_until_complete(self.conn_pool.close())
