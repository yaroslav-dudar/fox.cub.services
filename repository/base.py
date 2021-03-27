# pylint: disable=E1101
"""Pymongo base wrapper"""

import atexit

import pymongo
#from bson.objectid import ObjectId
#from bson.codec_options import TypeRegistry, CodecOptions

from config import Config


class Connection:
    def __set_name__(self, owner, name):
        self.name = name

    def __set__(self, instance, value):
        if not isinstance(value, pymongo.MongoClient):
            raise ValueError("Should be MongoDB instance only")

        instance.__dict__[self.name] = value

    def __get__(self, instance, owner):
        if not instance:
            return None

        return instance.__dict__.get(self.name)


class MongoClient:
    """ Global MongoDB connector """
    conn = Connection()
    db = None
    _obj = None

    def __new__(cls, *args, **kwargs):
        if cls._obj:
            # prevent to create multiple db connections
            return cls._obj

        cls._obj = super(MongoClient, cls).__new__(cls)
        return cls._obj

    def __init__(self, db_config: dict):
        auth_config = {}
        if db_config.get("authMechanism") == "SCRAM-SHA-1":
            auth_config['authMechanism'] = db_config["authMechanism"]
            auth_config['authSource'] = db_config["authSource"]
            auth_config['username'] = db_config["username"]
            auth_config['password'] = db_config["password"]

        self.conn = pymongo.MongoClient(
            host=db_config['host'],
            port=db_config['port'],
            **auth_config
        )
        MongoClient.db = self.conn[db_config['db_name']]
        # clean-up DB resources
        atexit.register(self.conn.close)


class BaseModel(type):
    def __new__(cls, name, bases, attr):
        attr['client'] = MongoClient(Config()['database'])

        if attr.get("capped_settings"):
            cls.setup_capped_collection(attr)

        session = attr['client'].db.get_collection(
                attr["collection"],
                codec_options=attr.get('codec_options'))

        attr['db_session'] = session

        return super().__new__(cls, name, bases, attr)


    @classmethod
    def setup_capped_collection(cls, attr):
        settings = attr.get("capped_settings")
        try:
            attr['client'].db.create_collection(attr["collection"],
                                                capped=settings["capped"],
                                                size=settings["size"],
                                                max=settings["max"])
        except pymongo.errors.CollectionInvalid as invalid_collection:
            stats = attr['client'].db.command('collStats',
                                              attr["collection"])
            # verifing that collection is capped
            if not stats.get('capped'):
                raise RuntimeError('{} should be capped collection'.\
                                   format(stats['ns'])) from invalid_collection
