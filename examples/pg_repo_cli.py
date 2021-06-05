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
                min_points, max_points, event_ids, season_ids):
    start_at = time.time()
    #matches = await match_repo.get_by_points(min_points, max_points, event_ids, season_ids)
    stats = await match_repo.get_stats([2], [4, 2396, 8668, 8863])
    print(f"Execution time: {time.time() - start_at}")
    #print(f"Fetched {len(matches)} records")
    print(dict(stats[0]))


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    pg_client = loop.run_until_complete(init_task())
    match_repo = repository.MatchPgRepository(pg_client)
    loop.run_until_complete(fetch(match_repo, 0, 1, [2], [4, 2396, 8668, 8863]))
