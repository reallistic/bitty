import asyncio
import logging

from bitty import loggers
from bitty.consumers.gdax import GdaxConsumer
from bitty.consumers.poloniex import PoloniexConsumer

if __name__ == '__main__':
    logger = logging.getLogger('bitty')
else:
    logger = logging.getLogger(__name__)


PRODUCT_PAIRS = (
    ['BTC-USD'],
    ['ETH-USD'],
    ['ETH-BTC'],
    ['LTC-USD'],
    ['LTC-BTC']
)


def collect(loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    threads = []

    for product_ids in PRODUCT_PAIRS:
        logger.info('creating thread for pair: %s', product_ids)
        keeper = GdaxConsumer(product_ids, loop=loop)
        keeper.spawn_consumer()
        threads.append(keeper)

    keeper = PoloniexConsumer(['USDT_BTC'], loop=loop)
    keeper.spawn_consumer()
    threads.append(keeper)

    return threads


def main():
    loggers.setup()
    loop = asyncio.get_event_loop()
    threads = collect(loop=loop)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info('exiting')
        cos = []
        for thread in threads:
            cos.append(thread.kill())

        loop.run_until_complete(asyncio.gather(*cos, loop=loop))
        loop.stop()


if __name__ == '__main__':
    main()
