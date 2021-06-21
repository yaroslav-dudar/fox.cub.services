import asyncio

from twisted.internet.asyncioreactor import install

install()
from twisted.internet import reactor, defer  # noqa: E402

from config import Config  # noqa: E402
import repository  # noqa: E402


async def main():
    loop = asyncio.get_running_loop()
    conf = Config()

    pg_client = repository.PgClient(conf["citus_database"], loop)
    await pg_client.init_connection()


if __name__ == "__main__":
    d = defer.Deferred.fromCoroutine(main())
    d.addErrback(lambda e: print(e))
    reactor.callLater(4, reactor.stop)
    reactor.run()
