"""
Script to process the swaps data
"""

import argparse
import json
import multiprocessing
import os
from glob import glob
from typing import List

from environ.constants import DATA_PATH


def process_txn(files: List, chain: str) -> None:
    """Method to process the transaction hash from the swap files"""

    txn_set = set()
    batch_num = 0

    for file in files:
        from_block = int(file.split("/")[-1].split(".")[0].split("_")[0])
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                event = json.loads(line)
                txn_set.add(event["transactionHash"])

                if len(txn_set) == 1000:
                    batch_num += 1
                    with open(
                        f"{DATA_PATH}/{chain}/txn_hash/{from_block}_{batch_num}.jsonl",
                        "w",
                        encoding="utf-8",
                    ) as f:
                        for txn in txn_set:
                            f.write(json.dumps({"transactionHash": txn}) + "\n")
                    txn_set.clear()

    if txn_set:
        batch_num += 1
        with open(
            f"{DATA_PATH}/{chain}/txn_hash/{from_block}_{batch_num}.jsonl",
            "w",
            encoding="utf-8",
        ) as f:
            for txn in txn_set:
                f.write(json.dumps({"transactionHash": txn}) + "\n")


def parse_args():
    """Method to parse the arguments"""
    parser = argparse.ArgumentParser(description="Process the swap data")
    parser.add_argument(
        "--chain",
        default="polygon",
        help="The chain to fetch data from (e.g., polygon).",
    )

    return parser.parse_args()


def main() -> None:
    """Main entrypoint"""

    args = parse_args()
    os.makedirs(f"{DATA_PATH}/{args.chain}/txn_hash", exist_ok=True)

    files = glob(f"{DATA_PATH}/{args.chain}/swap/*.jsonl")
    num_workers = int(os.cpu_count()) - 4

    # divide the files into chunks with number equals number of workers
    chunk_size = len(files) // num_workers
    remainder = len(files) % num_workers

    chunks = []
    start = 0
    for i in range(num_workers):
        end = start + chunk_size + (1 if i < remainder else 0)  # Distribute remainder
        chunks.append(files[start:end])
        start = end

    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.starmap(process_txn, [(chunk, args.chain) for chunk in chunks])


if __name__ == "__main__":
    main()
