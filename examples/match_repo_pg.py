from pprint import pprint
import asyncio

from scrapy.crawler import CrawlerProcess

from config import Config
from parsers.betstady import BetStadySpider
import repository
import domain
import event_bus

async def main():
    loop = asyncio.get_running_loop()
    conf = Config()

    pg_client = repository.PgClient(conf['citus_database'],
                                    loop)
    await pg_client.init_connection()
    pg_repo = repository.MatchPgRepository(pg_client)
    bus = event_bus.MemoryEventBus()
    bus.register(domain.FootballMatch, pg_repo.insert_many)

    items_buffer = []
    process = CrawlerProcess()
    process.crawl(BetStadySpider, event_bus=bus, items_buffer=items_buffer)
    process.start()

    await pg_repo.insert_many(items_buffer)
    matches = await pg_repo.get_by_event(1)
    pprint(matches)

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
