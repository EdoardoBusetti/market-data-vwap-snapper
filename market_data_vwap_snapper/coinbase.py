import json
import time
from utils import to_external_pair, to_internal_pair
import dateutil.parser
import logging
from config import (
    WEBSOCKET_URLS,
    EXCEPTIONS_IN_RECONNECTION_IN_ROW_LIMIT,
    COMMIT_EVERY_X_SECONDS,
    COMMIT_EXCEPTIONS_IN_ROW_COUNT_LIMIT,
)
from websockets import connect
from datetime import datetime
from models import OrderbookSnapshot, OrderbookLevelOverride, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# REMOVE
import ssl
import certifi

ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(certifi.where())


sqlite_filepath = "market_data.sqlite"
engine = create_engine(f"sqlite:///{sqlite_filepath}")
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Base.metadata.create_all(engine)


def classify_coinbase_ws_message(ws_message):
    if ws_message["type"] == "error":
        msg = f"{ws_message['error']} - {ws_message['message']}"
        logging.error(msg)
        return {"external_time": None, "type": ws_message["type"]}
    elif ws_message["type"] == "subscriptions":
        subscriptions_msg_str = f"{ws_message['channels']}"
        if not ws_message["channels"]:
            msg = f"empty channles in subscription {subscriptions_msg_str}"
            logging.error(msg)
        return {"external_time": None, "type": ws_message["type"]}
    elif ws_message["type"] == "snapshot":
        msg = f"received snapshot message for {ws_message['product_id']}"
        logging.info(msg)
        internal_pair = to_internal_pair(ws_message["product_id"], "coinbase")
        external_time = dateutil.parser.isoparse(ws_message["time"])
        processed_msg = {
            "external_time": external_time,
            "type": ws_message["type"],
            "internal_pair": internal_pair,
            "pair": ws_message["product_id"],
            "asks": ws_message["asks"],
            "bids": ws_message["bids"],
        }
        return processed_msg
    elif ws_message["type"] == "l2update":
        msg = f"received l2update message for {ws_message['product_id']}"
        external_time = dateutil.parser.isoparse(ws_message["time"])

        # logging.debug(msg)
        return {
            "external_time": external_time,
            "type": ws_message["type"],
            "internal_pair": to_internal_pair(ws_message["product_id"], "coinbase"),
            "pair": ws_message["product_id"],
            "changes": ws_message["changes"],
        }
    else:
        msg = f"received unhandled message type {ws_message['type']}"
        logging.warning(msg)
        return {"external_time": None, "type": "unknown",'message_payload':ws_message}


async def coinbase_orderbook_download(pairs_internal):
    subscribe_message = {
        "type": "subscribe",
        "product_ids": [
            to_external_pair(pair_internal, "coinbase") for pair_internal in pairs_internal
        ],
        "channels": ["level2_batch"],
    }

    url = WEBSOCKET_URLS["coinbase"]
    message_counter = 0
    ws = await connect(url, ssl=ssl_context)
    await ws.send(json.dumps(subscribe_message))
    last_commit_time = datetime.now()
    orderbook_snapshot_ids = {}
    commit_exceptions_in_row_count = 0
    websocket_closed_times = 0
    commit_exceptions_total_count = 0
    expections_in_receipt_count = 0
    exceptions_in_reconnection_in_row = 0
    while True:
        try:
            if not ws.open:
                websocket_closed_times += 1
                logging.warning("Websocket NOT connected. Trying to reconnect.")
                ws = await connect(url, ssl=ssl_context)
                await ws.send(json.dumps(subscribe_message))
            data = await ws.recv()
        except Exception as e:
            expections_in_receipt_count += 1
            try:
                logging.warning(f"got exception in receipt. \n{e}\n. trying to reconnect...")
                ws = await connect(url, ssl=ssl_context)
                await ws.send(json.dumps(subscribe_message))
                logging.info(f"Reconnected")
                exceptions_in_reconnection_in_row = 0
            except Exception as e2:
                exceptions_in_reconnection_in_row += 1
                if exceptions_in_reconnection_in_row < EXCEPTIONS_IN_RECONNECTION_IN_ROW_LIMIT:
                    logging.warning(f"got exception in reconnection. \n{e2}\n. continuing")
                    time.sleep(3)
                    continue
                else:
                    raise ValueError(
                        f"too many exceptioins in reconnection. {exceptions_in_reconnection_in_row}/{EXCEPTIONS_IN_RECONNECTION_IN_ROW_LIMIT}"
                    )
        received_at = datetime.now()
        if message_counter % 10_000 == 0:
            logging.info(
                f"now: {received_at.strftime('%Y-%m-%d %H:%M:%S')}. last commit: {last_commit_time.strftime('%Y-%m-%d %H:%M:%S')}. # msg: {message_counter}. #closed socket: {websocket_closed_times}. #commit exep: {commit_exceptions_total_count}. #receipt exep: {expections_in_receipt_count}"
            )
        message_counter += 1

        json_data = json.loads(data)
        json_data["time_utc"] = time.time() * 1000
        msg_classified = classify_coinbase_ws_message(json_data)
        logging.info(msg_classified)
        should_commit_to_db = (
            (datetime.now() - last_commit_time).seconds
        ) >= COMMIT_EVERY_X_SECONDS

        if msg_classified["type"] == "snapshot":
            base_ccy, counter_ccy = msg_classified["internal_pair"].split("-")
            external_time = msg_classified["external_time"]
            current_orderbook_snapshot = OrderbookSnapshot(
                external_time=external_time,
                received_at=received_at,
                base=base_ccy,
                counter=counter_ccy,
                provider="coinbase",
                levels=-1,
                bids=json.dumps(msg_classified["bids"]),
                asks=json.dumps(msg_classified["asks"]),
            )
            session.add(current_orderbook_snapshot)
            session.commit()
            orderbook_snapshot_ids[f"{base_ccy}-{counter_ccy}"] = current_orderbook_snapshot.id
        elif msg_classified["type"] == "l2update":
            base_ccy, counter_ccy = msg_classified["internal_pair"].split("-")
            external_time = msg_classified["external_time"]
            bids_overrides = json.dumps([i[1:] for i in msg_classified["changes"] if i[0] == "buy"])
            asks_overrides = json.dumps([i[1:] for i in msg_classified["changes"] if i[0] == "sell"])

            current_orderbook_update = OrderbookLevelOverride(
                orderbook_snapshot_id=orderbook_snapshot_ids.get(f"{base_ccy}-{counter_ccy}"),
                external_time=external_time,
                received_at=received_at,
                base=base_ccy,
                counter=counter_ccy,
                provider="coinbase",
                bids_overrides=bids_overrides,
                asks_overrides=asks_overrides,
            )
            session.add(current_orderbook_update)
            if should_commit_to_db:
                try:
                    session.commit()
                    last_commit_time = datetime.now()
                    commit_exceptions_in_row_count = 0
                except Exception as e:
                    commit_exceptions_total_count += 1
                    commit_exceptions_in_row_count += 1
                    logging.warning(
                        f"exception while committing to DB. {commit_exceptions_in_row_count} in a row: \n{e}\n"
                    )
                    if commit_exceptions_in_row_count > COMMIT_EXCEPTIONS_IN_ROW_COUNT_LIMIT:
                        raise ValueError(
                            f"Too many commit exceptions in a row. crashing. {commit_exceptions_in_row_count}/{COMMIT_EXCEPTIONS_IN_ROW_COUNT_LIMIT}"
                        )
        else:
            logging.warning(f"received unclassified message: {msg_classified}")
