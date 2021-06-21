import asyncio
import time
import pprint
from config import Config
import repository


async def init_task():
    loop = asyncio.get_running_loop()
    conf = Config()
    pg_client = repository.PgClient(conf["citus_database"], loop)
    await pg_client.init_connection()
    return pg_client


async def fetch_report(
    match_repo: repository.MatchPgRepository,
    min_value,
    max_value,
    event_ids,
    season_ids,
):
    start_at = time.time()
    await match_repo.create_multi_season_table()
    low_table_teams = await match_repo.get_by_score(
        event_ids, season_ids, 0.0, 1.0, 0.0, 3.1
    )
    middle_table_teams = await match_repo.get_by_points(event_ids, season_ids, 1.5, 1.8)
    [print(r) for r in low_table_teams]
    print("=" * 20)
    [print(r) for r in middle_table_teams]
    print("=" * 20)
    stats = await match_repo.get_stats(
        [2],
        None,
        home_teams=[r["team"] for r in middle_table_teams],
        away_teams=[r["team"] for r in low_table_teams],
    )

    print(f"Execution time: {time.time() - start_at}")
    print("=" * 20)
    pprint.pprint(dict(stats[0]))


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    pg_client = loop.run_until_complete(init_task())
    match_repo = repository.MatchPgRepository(pg_client)
    loop.run_until_complete(
        fetch_report(match_repo, 15, 20, [2], [4, 2396, 8668, 8863])
    )
