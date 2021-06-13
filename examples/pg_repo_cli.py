import asyncio
import time

from config import Config
import repository


async def init_task():
    loop = asyncio.get_running_loop()
    conf = Config()
    pg_client = repository.PgClient(conf['citus_database'],
                                    loop)
    await pg_client.init_connection()
    return pg_client


async def fetch(match_repo: repository.MatchPgRepository,
                min_value, max_value, event_ids, season_ids):
    start_at = time.time()
    await match_repo.create_season_table()
    league_table = await match_repo.get_by_league_pos(min_value, max_value, event_ids, season_ids)
    teams = await match_repo.get_by_score(event_ids, season_ids, 1.0, 3.0, 0.0, 1.1)
    stats = await match_repo.get_stats([2], [4, 2396, 8668, 8863])
    print(f"Execution time: {time.time() - start_at}")
    print(f"Fetched {len(league_table)} records")
    print(teams)
    print(dict(stats[0]))


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    pg_client = loop.run_until_complete(init_task())
    match_repo = repository.MatchPgRepository(pg_client)
    loop.run_until_complete(fetch(match_repo, 15, 20, [2], [4, 2396, 8668, 8863]))
