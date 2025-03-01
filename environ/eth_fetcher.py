"""
Class to filter the event from Ethereum
"""

import os
import multiprocessing
import datetime
import json
import logging
from typing import Any, Dict, Iterable

from eth_abi.codec import ABICodec
from web3 import Web3
from web3._utils.events import get_event_data
from web3._utils.filters import construct_event_filter_params
from web3.exceptions import BlockNotFound
from web3.providers import HTTPProvider
from environ.constants import (
    ABI_PATH,
    ETHEREUM_USDC_ETH_500_V3_POOL,
    ETHEREUM_INFURA_API_BASE,
)

logger = logging.getLogger(__name__)


def _fetch_current_block(w3: Web3) -> int:
    """Fetch the current block number"""

    return w3.eth.block_number


def _get_block_timestamp(w3: Web3, block_num) -> datetime.datetime:
    """Get Ethereum block timestamp"""
    try:
        block_info = w3.eth.get_block(block_num)
    except BlockNotFound:
        return
    last_time = block_info["timestamp"]
    return datetime.datetime.utcfromtimestamp(last_time)


def _get_transaction(w3: Web3, tx_hash: str) -> Iterable:
    """Get Ethereum transaction"""
    return w3.eth.get_transaction(tx_hash)


def _get_token_decimals(w3: Web3, token_address: str) -> int:
    """Get the number of decimals for a token"""
    return _call_function(
        w3,
        token_address,
        json.load(open(ABI_PATH / "erc20.json", encoding="utf-8")),
        "decimals",
    )


def _fetch_events_for_all_contracts(
    w3: Web3,
    event: Any,
    argument_filters: Dict[str, Any],
    from_block: int,
    to_block: int,
) -> Iterable:
    """Method to get events

    Args:
        w3 (Web3): The Web3 instance
        event (Any): The event to fetch
        argument_filters (Dict[str, Any]): The filters to apply to the event
        from_block (int): The block number to start fetching events from, inclusive
        to_block (int): The block number to stop fetching events from, inclusive
    """

    if from_block is None:
        raise ValueError("Missing mandatory keyword argument 'from_block'")

    # Construct the event filter parameters
    abi = event._get_event_abi()
    codec: ABICodec = w3.codec
    _, event_filter_params = construct_event_filter_params(
        abi,
        codec,
        address=argument_filters.get("address"),
        argument_filters=argument_filters,
        from_block=from_block,
        to_block=to_block,
    )

    # logging
    logs = w3.eth.get_logs(event_filter_params)

    all_events = []
    for log in logs:
        evt = get_event_data(codec, abi, log)
        all_events.append(evt)

    return all_events


def _call_function(
    w3: Web3,
    address: str,
    abi: Dict,
    func_name: str,
    block: int,
    *args,
) -> Any:
    """Method to call a function on a contract"""

    contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)

    if not hasattr(contract.functions, func_name):
        raise ValueError(f"Function {func_name} not found in contract")

    return getattr(contract.functions, func_name)(*args).call(block_identifier=block)


def fetch_swap_events(
    from_block: int, to_block: int, queue: multiprocessing.Queue
) -> None:
    """Fetch swap events using a specific API key and block range"""

    http = queue.get()
    print("Using API key:", http)

    w3 = Web3(HTTPProvider(http))

    # Fetch swap events
    swap_event = w3.eth.contract(
        abi=json.load(open(f"{ABI_PATH}/v3pool.json", encoding="utf-8"))
    ).events.Swap

    events = _fetch_events_for_all_contracts(
        w3,
        swap_event,
        {"address": ETHEREUM_USDC_ETH_500_V3_POOL},
        from_block,
        to_block,
    )

    print(f"Fetched {len(events)} events for block range {from_block} - {to_block}")
    queue.put(http)


def fetch_swap_events_concurrently():
    """Fetch swap events concurrently using multiple processes."""

    # Initialize Web3 instances for two API keys
    INFURA_API_KEYS = str(os.getenv("INFURA_API_KEYS")).split(",")

    # Create a manager to handle the shared queue
    with multiprocessing.Manager() as manager:
        # Create a shared Queue using the manager
        queue = manager.Queue()

        # Put all API keys into the queue
        for api_key in INFURA_API_KEYS:
            queue.put(ETHEREUM_INFURA_API_BASE + api_key)

        # Set block ranges
        block_range_1 = (15000000, 15005000)
        block_range_2 = (15005000, 15010000)

        # Prepare arguments for the processes
        args_1 = (*block_range_1, queue)
        args_2 = (*block_range_2, queue)

        # Start the multiprocessing pool and execute the tasks
        with multiprocessing.Pool(processes=2) as pool:
            pool.starmap(fetch_swap_events, [args_1, args_2, args_1, args_2])


if __name__ == "__main__":

    # Archive fetch
    w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8545"))
    current_time = datetime.datetime.now()
    res = _fetch_events_for_all_contracts(
        w3,
        w3.eth.contract(
            abi=json.load(open(f"{ABI_PATH}/v3pool.json", encoding="utf-8"))
        ).events.Swap,
        {"address": ETHEREUM_USDC_ETH_500_V3_POOL},
        15000000,
        15100000,
    )
    current_time = datetime.datetime.now() - current_time
    print(f"Time taken: {current_time}, events: {len(res)}")

    # # Linear fetch
    # INFURA_API_KEYS = str(os.getenv("INFURA_API_KEYS")).split(",")
    # w3 = Web3(HTTPProvider(ETHEREUM_INFURA_API_BASE + INFURA_API_KEYS[0]))
    # current_time = datetime.datetime.now()

    # _ = _fetch_events_for_all_contracts(
    #     w3,
    #     w3.eth.contract(
    #         abi=json.load(open(f"{ABI_PATH}/v3pool.json", encoding="utf-8"))
    #     ).events.Swap,
    #     {"address": ETHEREUM_USDC_ETH_500_V3_POOL},
    #     15000000,
    #     15005000,
    # )

    # _ = _fetch_events_for_all_contracts(
    #     w3,
    #     w3.eth.contract(
    #         abi=json.load(open(f"{ABI_PATH}/v3pool.json", encoding="utf-8"))
    #     ).events.Swap,
    #     {"address": ETHEREUM_USDC_ETH_500_V3_POOL},
    #     15005000,
    #     15010000,
    # )

    # current_time = datetime.datetime.now() - current_time
    # print(f"Time taken: {current_time}")

    # # Multiprocessing fetch
    # current_time = datetime.datetime.now()
    # fetch_swap_events_concurrently()
    # current_time = datetime.datetime.now() - current_time
    # print(f"Time taken for fetch: {current_time}")
