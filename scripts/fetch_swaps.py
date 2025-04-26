"""
Script to fetch swaps
"""

import argparse
import glob
import json
import logging
import multiprocessing
import os
import time
from typing import Optional

from tqdm import tqdm
from web3 import HTTPProvider, Web3
from web3.exceptions import Web3RPCError

from environ.constants import API_BASE, DATA_PATH, INFURA_API_KEYS, UNISWAP_V3_POOL_ABI
from environ.utils import _fetch_events_for_all_contracts, to_dict

os.makedirs(DATA_PATH / "log", exist_ok=True)
logging.basicConfig(
    filename=DATA_PATH / "log" / "error.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.ERROR,
)


def extract_pool(chain: str = "polygon") -> list[tuple[str, int]]:
    """Fetch the set of pools from the file"""

    # get the list of all files in the folder
    glob_path = DATA_PATH / chain / "pool" / "*.json"
    files = glob.glob(str(glob_path))

    pool_list = []
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                event = json.loads(line)
                pool_list.append((event["args"]["pool"], event["blockNumber"]))

    pool_list.sort(key=lambda x: x[1])

    return pool_list


def split_blocks(
    start_block: int, end_block: int, step: int, pools: list[str], chain: str
) -> list[tuple[int, int]]:
    """
    Split the blocks into step ranges
    """

    min_block = (start_block // step) * step
    max_block = (end_block // step) * step

    blocks = []

    for i in tqdm(range(min_block, max_block, step), desc="Splitting Blocks"):

        # check wether the file exits
        if os.path.exists(DATA_PATH / chain / "swap" / f"{i}_{i + step - 1}.json"):
            continue

        pool_list = []
        for pool in pools:
            if pool[1] <= i + step - 1:
                pool_list.append(pool[0])
            else:
                break

        blocks.append((i, i + step - 1, pool_list))

    print(f"TODOs: {len(blocks)}")
    return blocks


def fetch_swap_multiprocess(
    chain: str,
    from_block: int,
    to_block: int,
    pools: list[str],
    queue: multiprocessing.Queue,
    path: str,
    abi: dict[str, str],
) -> None:
    """Fetch swap events using a specific API key and block range"""

    http = queue.get()
    time.sleep(1)
    fetch_swap_events(chain, from_block, to_block, pools, http, path, abi)
    queue.put(http)


def fetch_swap_events(
    chain: str,
    from_block: int,
    to_block: int,
    pools: list[str],
    http: str,
    path: str,
    abi: dict[str, str],
    is_main: bool = True,
    temp_data: Optional[list] = None,
) -> None:
    """Fetch swap events using a specific API key and block range"""

    if temp_data is None:
        temp_data = []

    try:
        w3 = Web3(HTTPProvider(http))
        swap_event = w3.eth.contract(abi=abi).events.Swap
        events = _fetch_events_for_all_contracts(
            w3,
            swap_event,
            {"address": pools},
            from_block,
            to_block,
        )
        events = to_dict(events)
        temp_data.extend(events)

    except Web3RPCError as e:
        try:
            error_msg = json.loads(e.args[0].replace("'", '"'))
            if error_msg["code"] == -32005:
                mid_block = (from_block + to_block) // 2
                fetch_swap_events(
                    chain,
                    from_block,
                    mid_block,
                    pools,
                    http,
                    path,
                    abi,
                    False,
                    temp_data,
                )
                fetch_swap_events(
                    chain,
                    mid_block + 1,
                    to_block,
                    pools,
                    http,
                    path,
                    abi,
                    False,
                    temp_data,
                )
        except json.JSONDecodeError as _:
            logging.error(
                "Fetching Swaps: Error fetching %s swap events for block range %d - %d: %s",
                chain,
                from_block,
                to_block,
                e,
            )
        except Exception as _:
            logging.error(
                "Fetching Swaps: Error fetching %s swap events for block range %d - %d: %s",
                chain,
                from_block,
                to_block,
                e,
            )
    except Exception as e:
        logging.error(
            "Fetching Swaps: Error fetching %s swap events for block range %d - %d: %s",
            chain,
            from_block,
            to_block,
            e,
        )

    # If we are the main caller, finally write to the file
    if is_main:
        with open(
            path,
            "w",
            encoding="utf-8",
        ) as f:
            for event in events:
                f.write(json.dumps(event) + "\n")


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
    os.makedirs(DATA_PATH / args.chain / "swap", exist_ok=True)

    with multiprocessing.Manager() as manager:
        api_queue = manager.Queue()

        for api_key in INFURA_API_KEYS:
            api_queue.put(API_BASE[args.chain] + api_key)

        blocks = split_blocks(
            args.start,
            args.end,
            args.step,
            extract_pool(args.chain),
            args.chain,
        )

        abi = UNISWAP_V3_POOL_ABI
        num_processes = min(len(INFURA_API_KEYS), os.cpu_count())
        with multiprocessing.Pool(processes=num_processes) as pool:
            pool.starmap(
                fetch_swap_multiprocess,
                [
                    (
                        args.chain,
                        *block_range,
                        api_queue,
                        f"{DATA_PATH}/{args.chain}/swap/{block_range[0]}_{block_range[1]}.jsonl",
                        abi,
                    )
                    for block_range in blocks
                ],
            )


if __name__ == "__main__":
    main()
