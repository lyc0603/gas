"""
Script to fetch swaps
"""

import argparse
import glob
import json
import multiprocessing
import os
from typing import List

from tqdm import tqdm
from web3 import HTTPProvider, Web3
from web3.exceptions import Web3RPCError

from environ.constants import (
    ABI_PATH,
    DATA_PATH,
    POLYGON_V3_FACTORY_END_BLOCK,
    POLYGON_V3_FACTORY_START_BLOCK,
)
from environ.utils import API_BASE, _fetch_events_for_all_contracts, to_dict

FACTORY_BLOCK = {
    "polygon": {
        "start": POLYGON_V3_FACTORY_START_BLOCK,
        "end": POLYGON_V3_FACTORY_END_BLOCK,
    }
}
STEP = {
    "polygon": 1000,
}


def extract_pool(chain: str = "polygon") -> List:
    """Fetch the set of pools from the file"""

    # get the list of all files in the folder
    glob_path = f"{DATA_PATH}/{chain}/pool/*.json"
    files = glob.glob(glob_path)

    pool_list = []
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                event = json.loads(line)
                pool_list.append((event["args"]["pool"], event["blockNumber"]))

    pool_list.sort(key=lambda x: x[1])

    return pool_list


def split_blocks(start_block: int, end_block: int, step: int, pools: List) -> list:
    """
    Split the blocks into step ranges
    """

    min_block = (start_block // step) * step
    max_block = (end_block // step) * step

    blocks = []

    for i in tqdm(range(min_block, max_block, step), desc="Splitting Blocks"):
        pool_list = []
        for pool in pools:
            if pool[1] <= i + step - 1:
                pool_list.append(pool[0])
            else:
                break

        blocks.append((i, i + step - 1, pool_list))

    return blocks


def fetch_swap_events(
    chain: str,
    from_block: int,
    to_block: int,
    pools: list,
    queue: multiprocessing.Queue,
    path: str,
) -> None:
    """Fetch swap events using a specific API key and block range"""

    http = queue.get()

    try:
        w3 = Web3(HTTPProvider(http))

        # Fetch swap events
        swap_event = w3.eth.contract(
            abi=json.load(open(f"{ABI_PATH}/v3pool.json", encoding="utf-8"))
        ).events.Swap

        events = _fetch_events_for_all_contracts(
            w3,
            swap_event,
            {"address": pools},
            from_block,
            to_block,
        )
        events = to_dict(events)

        with open(
            path,
            "a",
            encoding="utf-8",
        ) as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
    except Web3RPCError as e:
        error_msg = json.loads(e.args[0].replace("'", '"'))
        if error_msg["code"] == -32005:
            mid_block = (from_block + to_block) // 2
            fetch_swap_events(chain, from_block, mid_block, pools, http, path)
            fetch_swap_events(chain, mid_block + 1, to_block, pools, http, path)
    except Exception as e:
        print(
            f"Error fetching swap events for block range {from_block} - {to_block}, {e}"
        )
        with open(
            f"{DATA_PATH}/{chain}/error/error.log",
            "a",
            encoding="utf-8",
        ) as f:
            f.write(
                f"Error fetching {chain} swap events for block range {from_block} - {to_block}.\n"
            )
    finally:
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
        default=22757547,
        type=int,
        help="The starting block number for fetching data.",
    )
    parser.add_argument(
        "--end",
        default=68400000,
        type=int,
        help="The ending block number for fetching data.",
    )
    parser.add_argument(
        "--step",
        default=1000,
        type=int,
        help="The block step size for fetching data.",
    )
    return parser.parse_args()


def main() -> None:
    """
    CLI entrypoint
    """

    args = parse_args()
    if not os.path.exists(f"{DATA_PATH}/{args.chain}/swap"):
        os.makedirs(f"{DATA_PATH}/{args.chain}/swap")
    if not os.path.exists(f"{DATA_PATH}/{args.chain}/error"):
        os.makedirs(f"{DATA_PATH}/{args.chain}/error")

    INFURA_API_KEYS = str(os.getenv("INFURA_API_KEYS")).split(",")

    with multiprocessing.Manager() as manager:
        api_queue = manager.Queue()

        for api_key in INFURA_API_KEYS:
            api_queue.put(API_BASE[args.chain] + api_key)

        blocks = split_blocks(
            args.start,
            args.end,
            args.step,
            extract_pool(args.chain),
        )

        with multiprocessing.Pool(processes=len(INFURA_API_KEYS)) as pool:
            pool.starmap(
                fetch_swap_events,
                [
                    (
                        args.chain,
                        *block_range,
                        api_queue,
                        f"{DATA_PATH}/{args.chain}/swap/{block_range[0]}_{block_range[1]}.json",
                    )
                    for block_range in blocks
                ],
            )


if __name__ == "__main__":
    main()
