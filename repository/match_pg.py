# pylint: disable=E1101
"""Pymongo base wrapper"""

import atexit

import asyncio
import asyncpg

from domain import FootballMatch

class Connection:
    def __set_name__(self, owner, name):
        self.name = name

    def __set__(self, instance, value):
        if not isinstance(value, asyncpg.Pool):
            raise ValueError("Should be asyncpg instance only")

        instance.__dict__[self.name] = value

    def __get__(self, instance, owner):
        if not instance:
            return None

        return instance.__dict__.get(self.name)


class PgClient:
    """ Global PostgreSQL connector """
    conn_pool = Connection()
    db = None
    _obj = None

    def __new__(cls, *args, **kwargs):
        if cls._obj:
            # prevent to create multiple db connections
            return cls._obj

        cls._obj = super(PgClient, cls).__new__(cls)
        return cls._obj

    def __init__(self, db_config: dict, loop: asyncio.AbstractEventLoop):
        self.db_config = db_config
        self.loop = loop
        atexit.register(self.shutdown)

    async def init_connection(self):
        if self.conn_pool:
            return self.conn_pool

        self.conn_pool = await asyncpg.create_pool(
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            host=self.db_config['host'],
            port=self.db_config['port'],
            max_size=10, max_inactive_connection_lifetime=100,
            loop=self.loop)

        return self.conn_pool

    def shutdown(self):
        """Cleanup DB resources before exit."""
        self.loop.run_until_complete(self.conn_pool.close())


class MatchPgRepository:

    def __init__(self, pg_client: PgClient):
        self.client = pg_client

    async def insert_many(self, matches: list[FootballMatch]):
        if not matches:
            return

        input = ((m.team1_name, m.team2_name, m.date,
                  m.event_id, m.season_id, m.team1_ft_score,
                  m.team2_ft_score) for m in matches)

        async with self.client.conn_pool.acquire() as con:
            return await con.executemany('''
                INSERT INTO game (
                    home_team, away_team, date, event_id, season_id,
                    home_team_score, away_team_score
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', input)



    async def get_by_event(self, event_id: int):
        async with self.client.conn_pool.acquire() as con:
            return await con.fetch('''
                SELECT * FROM game WHERE event_id = $1
            ''', event_id)
