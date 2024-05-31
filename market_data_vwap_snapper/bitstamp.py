import json
import time
from utils import to_external_pair, to_internal_pair
import logging
from config import (
    WEBSOCKET_URLS,
    EXCEPTIONS_IN_RECONNECTION_IN_ROW_LIMIT,
    COMMIT_EVERY_X_SECONDS,
    COMMIT_EXCEPTIONS_IN_ROW_COUNT_LIMIT,
)
from websockets import connect
from datetime import datetime
from models import OrderbookSnapshot, OrderbookLevelOverride, OrderbookLevelDiff, Base
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


def classify_bitstamp_ws_message(ws_message):
    if ws_message["event"] == "bts:subscription_succeeded":
        logging.info(ws_message)
        return {"external_time": None, "type": ws_message["event"]}
    elif (ws_message["event"] == "data") & (ws_message["channel"].startswith("order_book_")):
        msg = f"received snapshot message for {ws_message['channel']}"
        logging.info(msg)
        external_pair = ws_message["channel"].split("_")[2]
        internal_pair = to_internal_pair(external_pair, "bitstamp")
        external_time = datetime.utcfromtimestamp(
            float(ws_message["data"]["microtimestamp"]) / 1000000
        )
        processed_msg = {
            "external_time": external_time,
            "type": "snapshot",
            "internal_pair": internal_pair,
            "pair": external_pair,
            "asks": ws_message["data"]["asks"],
            "bids": ws_message["data"]["bids"],
        }
        return processed_msg
    elif (ws_message["event"] == "data") & (ws_message["channel"].startswith("diff_order_book_")):
        msg = f"received l2update [diff] message for {ws_message['channel']}"
        logging.info(msg)
        external_pair = ws_message["channel"].split("_")[3]
        internal_pair = to_internal_pair(external_pair, "bitstamp")
        external_time = datetime.utcfromtimestamp(
            float(ws_message["data"]["microtimestamp"]) / 1000000
        )
        processed_msg = {
            "external_time": external_time,
            "type": "l2update",
            "internal_pair": internal_pair,
            "pair": external_pair,
            "changes": {
                "bids_overrides": ws_message["data"]["bids"],
                "asks_overrides": ws_message["data"]["asks"],
            },
        }
        return processed_msg
    elif (ws_message["event"] in {"order_created","order_deleted","order_changed"}) & (ws_message["channel"].startswith("live_orders")):
        msg = f"received live_orders message for {ws_message['channel']}"
        if ws_message["event"] == 'order_deleted': logging.info(ws_message)# ONLY DEBUG
            
        logging.info(msg)
        external_pair = ws_message["channel"].split("_")[2]
        internal_pair = to_internal_pair(external_pair, "bitstamp")
        external_time = datetime.utcfromtimestamp(
            float(ws_message["data"]["microtimestamp"]) / 1000000
        )
        assert ws_message["data"]["order_type"] in {0,1}
        # TODO TEMPORARY to get timestamps
        processed_msg = {
            "external_time": external_time,
            "type": "live_book_change",
            "internal_pair": internal_pair,
            "pair": external_pair,
            # "order_id": ws_message["data"]["id_str"],
            # "changes": {
            #     "ADD": ws_message["data"]["bids"],
            #     "REMOVE": ws_message["data"]["asks"],
            # }
            "amount": ws_message["data"]["amount_str"],
            "price": ws_message["data"]["price_str"],
            "action_type": 'ADD' # TODO we should convert such message in such a way that it tranforms into an action  to perform on the book : ADD/REMOVE some quantity from some level
        }
        return processed_msg
    else:
        msg = f"received unhandled message type {ws_message['event']}"
        logging.warning(msg)
        return {"external_time": None, "type": "unknown",'message_payload':ws_message}


async def bitstamp_orderbook_download(pairs_internal):
    type_of_sub = "live_orders"  # diff_order_book # order_book #live_orders
    url = WEBSOCKET_URLS["bitstamp"]
    message_counter = 0
    ws = await connect(url, ssl=ssl_context)

    for pair in pairs_internal:
        bitstamp_pair = to_external_pair(pair, "bitstamp")
        subscribe_message = {
            "event": "bts:subscribe",
            "data": {"channel": f"{type_of_sub}_{bitstamp_pair}"},
        }
        await ws.send(json.dumps(subscribe_message))

    last_commit_time = datetime.utcnow()
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
                # await ws.send(json.dumps(subscribe_message2))
            data = await ws.recv()
        except Exception as e:
            expections_in_receipt_count += 1
            logging.info(f"exception caught")
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
                    logging.error(
                        f"too many exceptioins in reconnection. {exceptions_in_reconnection_in_row}/{EXCEPTIONS_IN_RECONNECTION_IN_ROW_LIMIT}"
                    )
                    raise ValueError(
                        f"too many exceptioins in reconnection. {exceptions_in_reconnection_in_row}/{EXCEPTIONS_IN_RECONNECTION_IN_ROW_LIMIT}"
                    )
        received_at = datetime.utcnow()
        if message_counter % 10_000 == 0:
            logging.info(
                f"now: {received_at.strftime('%Y-%m-%d %H:%M:%S')}. last commit: {last_commit_time.strftime('%Y-%m-%d %H:%M:%S')}. # msg: {message_counter}. #closed socket: {websocket_closed_times}. #commit exep: {commit_exceptions_total_count}. #receipt exep: {expections_in_receipt_count}"
            )
        message_counter += 1

        json_data = json.loads(data)

        # print(f"RAW MESSAGE FOR DEBUGGING {json_data}")
        json_data["time_utc"] = time.time() * 1000
        msg_classified = classify_bitstamp_ws_message(json_data)
        should_commit_to_db = (
            (datetime.utcnow() - last_commit_time).seconds
        ) > COMMIT_EVERY_X_SECONDS

        if msg_classified["type"] == "snapshot":
            base_ccy, counter_ccy = msg_classified["internal_pair"].split("-")
            external_time = msg_classified["external_time"]
            current_orderbook_snapshot = OrderbookSnapshot(
                external_time=external_time,
                received_at=received_at,
                base=base_ccy,
                counter=counter_ccy,
                provider="bitstamp",
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
            bids_overrides = json.dumps(msg_classified["changes"]["bids_overrides"])
            asks_overrides = json.dumps(msg_classified["changes"]["asks_overrides"])

            current_orderbook_update = OrderbookLevelOverride(
                orderbook_snapshot_id=orderbook_snapshot_ids.get(f"{base_ccy}-{counter_ccy}"),
                external_time=external_time,
                received_at=received_at,
                base=base_ccy,
                counter=counter_ccy,
                provider="bitstamp",
                bids_overrides=bids_overrides,
                asks_overrides=asks_overrides,
            )
            session.add(current_orderbook_update)
            

            if should_commit_to_db:
                try:
                    session.commit()
                    last_commit_time = datetime.utcnow()
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
                        
        elif msg_classified["type"] == "live_book_change":
            base_ccy, counter_ccy = msg_classified["internal_pair"].split("-")
            external_time = msg_classified["external_time"]


            current_orderbook_update = OrderbookLevelDiff(
                orderbook_snapshot_id=orderbook_snapshot_ids.get(f"{base_ccy}-{counter_ccy}"),
                external_time=external_time,
                received_at=received_at,
                base=base_ccy,
                counter=counter_ccy,
                provider="bitstamp",
                bids_changes="[]", # TODO fix after understanding how to process these messages in book updates
                asks_changes=str([{'amount':msg_classified['amount'], # TODO fix after understanding how to process these messages in book updates
                               'price':msg_classified['price'],
                               'action_type':msg_classified['action_type'],
                               
                               }]),
            )
            session.add(current_orderbook_update)
            

            if should_commit_to_db:
                try:
                    session.commit()
                    last_commit_time = datetime.utcnow()
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

        elif msg_classified["type"] == "bts:subscription_succeeded":
            print("subscription")
            logging.info("subscription")
            # we need to call the full book endpoint when we get this message subscription succeeded message. as Bitstamp does not send the full book when we first subscribe
        else:
            logging.warning(f"received unclassified message: {msg_classified}")
