import json
from enum import Enum
import time
from utils import to_external_pair, to_internal_pair, verbose_raise_for_status, decode_asset
import requests
import logging
from config import (
    WEBSOCKET_URLS,
    EXCEPTIONS_IN_RECONNECTION_IN_ROW_LIMIT,
    COMMIT_EVERY_X_SECONDS,
    COMMIT_EXCEPTIONS_IN_ROW_COUNT_LIMIT,
)
from websockets import connect
from datetime import datetime
from models import TickerInformation, OrderbookSnapshot,  Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# REMOVE
import ssl
import certifi

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(certifi.where())


sqlite_filepath = "market_data.sqlite"
engine = create_engine(f"sqlite:///{sqlite_filepath}")
# engine = create_engine("postgresql://zmgcrppa:jgyslewcjbobfiyhqrjp@alpha.europe.mkdb.sh:5432/jxpeozwd")
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Base.metadata.create_all(engine)
VERTEX_PROVIDER = 'vertex'

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

class VertexmarketType(Enum):
    SPOT = "spot"
    PERP = "perp"



GATEWAY_V2_ENDPOINT = 'https://gateway.prod.vertexprotocol.com/v2'
ARCHIVE_V2_ENDPOINT = 'https://archive.prod.vertexprotocol.com/v2'

def split_pair(ticker_id):
    base,counter = ticker_id.split('_')
    return base,counter

def check_perp_or_spot(ticker_id):
    base,_ = ticker_id.split('_')
    if base.endswith('-PERP'):
        return VertexmarketType.PERP
    return VertexmarketType.SPOT

def get_tickers(market_type: VertexmarketType | None):
    params = {}
    if market_type:
        market_type_str = market_type.value
        params['market'] = market_type_str
    resp = requests.get(f"{ARCHIVE_V2_ENDPOINT}/tickers",params=params)
    verbose_raise_for_status(resp)
    all_tickers = resp.json()
    
    return all_tickers

def persist_tickers(tickers_json,session):
    for _,val in tickers_json.items():
        ticker_info_instance = TickerInformation(ticker_id=val['ticker_id'],
                                                    provider='vertex',
                        base=decode_asset('vertex',val['base_currency']),
                        counter=decode_asset('vertex',val['quote_currency']),
                        last_price=val['last_price'],
                        base_volume=val['base_volume'],
                        counter_volume=val['quote_volume'],
                        market_type=check_perp_or_spot(val['ticker_id']).value
                        )
        session.add(ticker_info_instance)

    session.commit()
    
def get_and_persist_all_tickers(session):
    tickers = get_tickers(market_type=None)
    persist_tickers(tickers,session=session)

def get_orderbook(ticker_id,depth=100):
    params = {'ticker_id':ticker_id}
    if depth:
        params['depth'] = depth
    resp = requests.get(f"{GATEWAY_V2_ENDPOINT}/orderbook",params=params)
    verbose_raise_for_status(resp)
    book = resp.json()
    book['received_at'] = datetime.now()
    book['levels'] = depth if depth else -1
    return book

def persist_book(book_json,session):
    base,counter = split_pair(book_json['ticker_id'])
    external_time = datetime.utcfromtimestamp(
            float(book_json['timestamp']) / 1000
        )
    book_instance = OrderbookSnapshot(external_time=external_time,
                                      received_at=book_json['received_at'],
                                      base=decode_asset('vertex',base),
                                        counter=decode_asset('vertex',counter),
                                        provider = VERTEX_PROVIDER,
                                        levels=book_json['levels'],
                                        bids  = str([[str(i[0]),str(i[1])] for i in book_json['bids']]),
                                        asks  = str([[str(i[0]),str(i[1])] for i in book_json['asks']]),
                                      )
    session.add(book_instance)
    session.commit()



def main():
    store_max_one_book_every_seconds = 60
    time_last_fetched = None
    cycles_to_complete = 1000
    
    logging.info("getting tickers")
    tickers = get_tickers(market_type=None)
    logging.info("got tickers")
    
    
    
    for i in range(cycles_to_complete):
        if time_last_fetched:
            seconds_since_last_book = (datetime.now() - time_last_fetched).total_seconds()
            logging.info("seconds_since_last_book %s",seconds_since_last_book)
            if seconds_since_last_book < store_max_one_book_every_seconds:
                logging.info("only %s seconds since we fetched last book. sleeping and then retry",seconds_since_last_book)
                time.sleep(1)
                continue
        
        time_last_fetched = datetime.now()
        for ticker_num, ticker in enumerate(tickers):
            logging.info(f"fetching book for {ticker}. {ticker_num+1}/{len(tickers)}")
            book = get_orderbook(ticker_id = ticker) # around 400ms
            persist_book(book_json=book,session=session) # around 2ms
        logging.info(f"DONE WITH CYCLE {i}/{cycles_to_complete}")
                
        
        
        
main()