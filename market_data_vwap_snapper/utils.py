import inspect
import json
import os
import sqlite3
import psycopg2
import requests
from cachetools import LRUCache

_LOCAL_PARAMS_CACHE = LRUCache(maxsize=64)

KRAKEN_DECODERS = {'XBT':'BTC'}
decoders = {'kraken':KRAKEN_DECODERS}
encoders = {}


def verbose_raise_for_status(response):
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        if response.text:
            print(response.text)
        response.raise_for_status()

def local_param(param_name: str):
    """
    This function returns a parameter from an adjacent './params/' directory of a calling function sight
    Parameters should be valid json files
    Parameter is cached for efficiency reasons, so files are read only once

    :param param_name: name of a parameter file to be loaded. Postfix '.json' can be ommited,
        so "my_param.json" and "my_param" are equivalent

    :raises ValueError: if the referenced file doesn't exist, or is outside of './params' directory

    :Example:
        given:
            /my_task
                /params
                    my_param.json
                my_task.py

        calling `local_param('my_param')` will return the contents of './params/my_param.json'
    """

    try:
        current_frame = inspect.currentframe()
        if current_frame is None:
            raise ValueError("Caller doesn't have frame info")
        calling_function_filepath = inspect.getframeinfo(current_frame.f_back).filename  # type: ignore[arg-type]
    except BaseException as e:
        raise ValueError("Caller as not defined in a file") from e

    base_dir = os.path.dirname(calling_function_filepath)

    if not param_name.endswith(".json"):
        param_name = param_name + ".json"

    param_dir = os.path.join(base_dir, "params")
    abs_param_path = os.path.abspath(os.path.join(param_dir, param_name))
    if abs_param_path not in _LOCAL_PARAMS_CACHE:
        # ensure that the destination doesn't look outside of our sql folder
        abs_param_dir = os.path.abspath(param_dir)
        common_path = os.path.commonpath([abs_param_path, abs_param_dir])
        if not common_path.startswith(abs_param_dir):
            raise ValueError(
                "Only parameter files in a sibling './params/' directory can be referenced"
            )

        if os.path.islink(abs_param_path):
            raise ValueError("Links can not be referenced as local parameter fle")

        try:
            with open(abs_param_path, "r", encoding="utf-8") as f:
                _LOCAL_PARAMS_CACHE[abs_param_path] = json.load(f)
        except Exception as e:
            raise ValueError(f"Couldn't read referenced file: '{abs_param_path}': {e}") from e

    return _LOCAL_PARAMS_CACHE[abs_param_path]




def decode_asset(provider:str,asset:str) -> str:
    asset = asset.upper()
    return decoders.get(provider,{}).get(asset,asset)

def encode_asset(provider:str,asset:str) -> str:
    asset = asset.upper()
    return encoders.get(provider,{}).get(asset,asset)


def to_external_pair(pair_internal: str, external_provider_name: str) -> str:
    """converts from internal to external pair string

    Args:
        pair_internal (str): internal representation of pairs: ASSET1-ASSET2
        external_provider_name (str): name of the external venue that we want the pair for

    Returns:
        str: representation of the pair for the specified 'external_provider_name'
    """
    internal_base,internal_counter = pair_internal.split('-')
    if external_provider_name == "coinbase":
        base_encoded, counter_encoded = encode_asset('coinbase',internal_base), encode_asset('coinbase',internal_counter)
        return f"{base_encoded}-{counter_encoded}"
    
    elif external_provider_name == "bitstamp":
        base_encoded, counter_encoded = encode_asset('bitstamp',internal_base), encode_asset('bitstamp',internal_counter)
        return f"{base_encoded}{counter_encoded}".lower()
    
    elif external_provider_name == "kraken":
        base_encoded, counter_encoded = encode_asset('kraken',internal_base), encode_asset('kraken',internal_counter)
        return f"{base_encoded}/{counter_encoded}"
    else:
        raise ValueError(
            f"Non implemented error. we do not have mapping logic for {external_provider_name}"
        )


def to_internal_pair(pair_external: str, external_provider_name: str) -> str:
    """converts from external to internal pair string

    Args:
        pair_external (str): representation of the pair for the specified 'external_provider_name'
        external_provider_name (str): name of the external venue that we want the pair for

    Returns:
        str: internal representation of pairs: ASSET1-ASSET2
    """
    if external_provider_name == "coinbase":
        external_base,external_counter = pair_external.split('-')
        internal_base, internal_counter = decode_asset('coinbase',external_base), decode_asset('coinbase',external_counter)
        return f"{internal_base}-{internal_counter}"
    elif external_provider_name == "bitstamp":
        """
        bitstamp pairs do not have any separator. last 3 chars is the counter
        """
        bitstamp_trading_pairs_symbol_to_name_map = {pair_info['url_symbol']:pair_info['name'] for pair_info in local_param('bitstamp_trading_pairs')}
        pair_name = bitstamp_trading_pairs_symbol_to_name_map.get(pair_external,pair_external).upper()
        external_base,external_counter = pair_name.split('/')
        internal_base, internal_counter = decode_asset('bitstamp',external_base), decode_asset('bitstamp',external_counter)
        return f"{internal_base}-{internal_counter}"
    elif external_provider_name == "kraken":
        """
        kraken pairs are in XXX/YYY format, and might need a asset conversion e/g/ XBT --> BTC
        """
        external_base,external_counter = pair_external.split('/')
        internal_base, internal_counter = decode_asset('kraken',external_base), decode_asset('kraken',external_counter)
        return f"{internal_base}-{internal_counter}"
    else:
        raise ValueError(
            f"Non implemented error. we do not have mapping logic for {external_provider_name}"
        )


def create_connection(connection_string, connection_type):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    assert connection_type in ('sqlite3','psycopg2')
    if connection_type == 'sqlite3':
        conn = None
        try:
            conn = sqlite3.connect(connection_string)
        except sqlite3.Error as e:
            print(e)

        return conn
    elif connection_type == 'psycopg2':
        try:
            conn = psycopg2.connect(connection_string)
            return conn
        except psycopg2.Error as e:
            print(f"Error: {e}")
            return None

        
