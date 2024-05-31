import asyncio
import json
import logging


logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

logger = logging.getLogger("websockets")
logger.addHandler(logging.StreamHandler())


from coinbase import coinbase_orderbook_download
from kraken import kraken_orderbook_download
from bitstamp import bitstamp_orderbook_download


# asyncio.run(coinbase_orderbook_download(["BTC-USD"]))
# asyncio.run(bitstamp_orderbook_download(["FLR-USD","AUDIO-USD"]))
asyncio.run(bitstamp_orderbook_download(["BTC-USD"]))
