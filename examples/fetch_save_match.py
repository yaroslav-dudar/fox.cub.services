from pprint import pprint
import asyncio

from twisted.internet import asyncioreactor
from scrapy.crawler import CrawlerProcess

from config import Config
from parsers.betstady import BetStadySpider
import repository


async def init_task():
    loop = asyncio.get_running_loop()
    conf = Config()
    pg_client = repository.PgClient(conf['citus_database'],
                                    loop)
    await pg_client.init_connection()
    return pg_client


async def finalyze_task(records, match_repo):
    await match_repo.insert_many(records)
    #matches = await match_repo.get_by_event(1)
    pprint(records)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncioreactor.install(loop)
    pg_client = loop.run_until_complete(init_task())

    match_repo = repository.MatchPgRepository(pg_client)
    items_buffer = []
    process = CrawlerProcess()
    process.crawl(BetStadySpider,
                  pg_client=pg_client,
                  items_buffer=items_buffer)
    process.start()

    loop.run_until_complete(finalyze_task(items_buffer, match_repo))
