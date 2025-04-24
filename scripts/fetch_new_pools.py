"""
Script to fetch new pool data
"""

import argparse
import json
import logging
import multiprocessing
import os
import time
from typing import Any

from dotenv import load_dotenv
from web3 import Web3
from web3.providers import HTTPProvider

from environ.constants import ABI_PATH, API_BASE, DATA_PATH, FACTORY
from environ.utils import _fetch_events_for_all_contracts, to_dict

load_dotenv()

os.makedirs(f"{DATA_PATH}/log", exist_ok=True)
logging.basicConfig(
    filename=f"{DATA_PATH}/log/error.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.ERROR,
)


def fetch_new_pools(
    chain: str, from_block: int, to_block: int, queue: multiprocessing.Queue
) -> None:
    """Fetch new pools using a specific API key and block range"""

    time.sleep(1)
    http = queue.get()

    try:
        w3 = Web3(HTTPProvider(http))

        # Fetch pool creation events
        pool_created_event = w3.eth.contract(
            abi=json.load(open(f"{ABI_PATH}/v3factory.json", encoding="utf-8"))
        ).events.PoolCreated

        events = _fetch_events_for_all_contracts(
            w3,
            pool_created_event,
            {"address": FACTORY[chain]},
            from_block,
            to_block,
        )

        events = to_dict(events)

        with open(
            f"{DATA_PATH}/{chain}/pool/{from_block}_{to_block}.json",
            "a",
            encoding="utf-8",
        ) as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
    except Exception as e:
        print(
            f"Fetching Pools: Block not found for block range {from_block} - {to_block}, {e}"
        )

    queue.put(http)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="Pool Fetcher CLI")
    parser.add_argument(
        "--chain",
        default="polygon",
        help="The chain to fetch data from (e.g., polygon).",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=22757547,
        help="The block number to start fetching data from.",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=68400000,
        help="The block number to stop fetching data from.",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=500000,
        help="The block range to fetch data in.",
    )
    return parser.parse_args()


def split_blocks(
    start_block: int, end_block: int, step: int, chain: str
) -> list[dict[str, Any]]:
    """
    Split the blocks into step ranges
    """

    min_block = (start_block // step) * step
    max_block = (end_block // step) * step

    blocks = []

    for i in range(min_block, max_block, step):
        # check if the file already exists
        if not os.path.exists(f"{DATA_PATH}/{chain}/pool/{i}_{i + step - 1}.json"):
            blocks.append((i, i + step - 1))
        else:
            continue

    print(f"TODOS: {len(blocks)}")
    return blocks


def main() -> None:
    """
    CLI entrypoint
    """

    args = parse_args()
    if not os.path.exists(f"{DATA_PATH}/{args.chain}/pool"):
        os.makedirs(f"{DATA_PATH}/{args.chain}/pool")

    INFURA_API_KEYS = str(os.getenv("INFURA_API_KEYS")).split(",")

    with multiprocessing.Manager() as manager:
        api_queue = manager.Queue()

        for api_key in INFURA_API_KEYS:
            api_queue.put(API_BASE[args.chain] + api_key)

        blocks = split_blocks(
            args.start,
            args.end,
            args.step,
            args.chain,
        )

        num_processes = min(len(INFURA_API_KEYS), os.cpu_count())
        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.starmap(
                fetch_new_pools,
                [(args.chain, *block_range, api_queue) for block_range in blocks],
            )


if __name__ == "__main__":
    main()
