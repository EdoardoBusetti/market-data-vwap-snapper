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


def classify_kraken_ws_message(ws_message):
    if type(ws_message) == dict:
        if ws_message["event"] == "systemStatus":
            logging.info(ws_message)
            return {"external_time": None, "type": ws_message["event"]}
        elif ws_message["event"] == "subscriptionStatus":
            logging.info(ws_message)
            return {"external_time": None, "type": ws_message["event"]}
        elif ws_message["event"] == "heartbeat":
            logging.info(ws_message)
            return {"external_time": None, "type": ws_message["event"]}
        else:
            msg = f"received unhandled message type {ws_message['event']}"
            logging.warning(msg)
            return {"external_time": None, "type": "unknown",'message_payload':ws_message}
    elif type(ws_message) == list:
        all_dicts = [i for i in ws_message if type(i) == dict]
        if len(all_dicts) > 2:
            raise ValueError(f"More than 2 dics in message: {ws_message}")
        check_sum_vals = [i.get('c') for i in all_dicts if i.get('c')]
        if len(check_sum_vals) >1:
            raise ValueError(f"more than 1 checksum: {ws_message}, {check_sum_vals}")
        checksum = check_sum_vals[0] if check_sum_vals else None
        has_checksum = True if checksum else False # kraken has checksum on orderbook updates.
        is_book_update = has_checksum
        # logging.info(ws_message) # DEBUG
        if ((ws_message[-2].startswith('book')) and (not is_book_update)):
            msg = f"received book snapshot message for {ws_message[-2]} {ws_message[-1]}"
            logging.info(msg)
            external_pair = ws_message[-1]
            internal_pair = to_internal_pair(external_pair, "kraken")
            asks_times = [i[2] for i in ws_message[1]['as']]
            asks = [i[:2] for i in ws_message[1]['as']]
            bids_times = [i[2] for i in ws_message[1]['bs']]
            bids = [i[:2] for i in ws_message[1]['bs']]
            book_max_timestamp_sec = max([float(i) for i in bids_times + asks_times])
            external_time = datetime.utcfromtimestamp(
                float(book_max_timestamp_sec)
            )
            processed_msg = {
                "external_time": external_time,
                "type": "snapshot",
                "internal_pair": internal_pair,
                "pair": external_pair,
                "asks": asks,
                "bids": bids,
            }
            return processed_msg            
        elif ((ws_message[-2].startswith('book')) and (is_book_update)):
            msg = f"received update [diff] message for {ws_message[-2]} {ws_message[-1]}"
            logging.debug(msg)
            merged_dicts = {k: v for d in all_dicts for k, v in d.items()}
            # logging.info(merged_dicts)
            if len(all_dicts) ==2:
                logging.info(f"double dict info message: {all_dicts}")
            external_pair = ws_message[-1]
            internal_pair = to_internal_pair(external_pair, "kraken")
            asks_times = [i[2] for i in merged_dicts.get('a',[])]
            asks = [i[:2] for i in merged_dicts.get('a',[])]
            bids_times = [i[2] for i in merged_dicts.get('b',[])]
            bids = [i[:2] for i in merged_dicts.get('b',[])]
            book_max_timestamp_sec = max([float(i) for i in bids_times + asks_times])
            external_time = datetime.utcfromtimestamp(
                float(book_max_timestamp_sec)
            )
            processed_msg = {
                'checksum': checksum,
                "external_time": external_time,
                "type": "update",
                "internal_pair": internal_pair,
                "pair": external_pair,
                "changes": {
                    "bids_overrides": bids,
                    "asks_overrides": asks,
                },
            }
            return processed_msg
        else:
            msg = f"received unhandled message type {ws_message}"
            raise ValueError(msg)
            # logging.warning(msg)
            # return {"external_time": None, "type": "unknown"}


async def kraken_orderbook_download(pairs_internal):
    type_of_sub = "book" 
    url = WEBSOCKET_URLS["kraken"]
    message_counter = 0
    ws = await connect(url, ssl=ssl_context)

    
    subscribe_message = {
        "event": "subscribe",
        "pair" :[to_external_pair(pair, "kraken") for pair in pairs_internal],
        "subscription": {"name": type_of_sub},
    }
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
        received_at = datetime.now()
        if message_counter % 10_000 == 0:
            logging.info(
                f"now: {received_at.strftime('%Y-%m-%d %H:%M:%S')}. last commit: {last_commit_time.strftime('%Y-%m-%d %H:%M:%S')}. # msg: {message_counter}. #closed socket: {websocket_closed_times}. #commit exep: {commit_exceptions_total_count}. #receipt exep: {expections_in_receipt_count}"
            )
        message_counter += 1

        json_data = json.loads(data)

        # print(f"RAW MESSAGE FOR DEBUGGING {json_data}")
        # json_data["time_utc"] = time.time() * 1000
        msg_classified = classify_kraken_ws_message(json_data)
        should_commit_to_db = (
            (datetime.now() - last_commit_time).seconds
        ) > COMMIT_EVERY_X_SECONDS

        if msg_classified["type"] == "snapshot":
            base_ccy, counter_ccy = msg_classified["internal_pair"].split("-")
            external_time = msg_classified["external_time"]
            current_orderbook_snapshot = OrderbookSnapshot(
                external_time=external_time,
                received_at=received_at,
                base=base_ccy,
                counter=counter_ccy,
                provider="kraken",
                levels=-1,
                bids=json.dumps(msg_classified["bids"]),
                asks=json.dumps(msg_classified["asks"]),
            )
            
            session.add(current_orderbook_snapshot)
            session.commit()
            orderbook_snapshot_ids[f"{base_ccy}-{counter_ccy}"] = current_orderbook_snapshot.id
            logging.info(f"current_orderbook_snapshot.id: {current_orderbook_snapshot.id}")
            logging.info(f"orderbook_snapshot_ids: {orderbook_snapshot_ids}")
        elif msg_classified["type"] == "update":
            logging.debug(f"update received: {msg_classified['internal_pair']}. 'kraken'")
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
                provider="kraken",
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

        elif msg_classified["type"] == "systemStatus":
            pass
            # logging.info("systemStatus")
        elif msg_classified["type"] == "subscriptionStatus":
            pass
            # logging.info("subscriptionStatus")
        elif msg_classified["type"] == "heartbeat":
            pass
            # logging.info("heartbeat")
        else:
            logging.warning(f"received unclassified message: {msg_classified}")
