"""
Subgraph fetcher
"""

import argparse
import datetime
import json
import os
import time
from typing import Any, Dict, Iterable

import requests
from tqdm import tqdm

from environ.constants import DATA_PATH
from environ.query import UNISWAP_V3_QUERY

INFO_DICT = {
    "polygon": {
        "http": "https://thegraph.com/explorer/api/playground/QmdAaDAUDCypVB85eFUkQMkS5DE1HV4s7WJb6iSiygNvAw",
        "start_date": "2024-01-01",
    }
}

# create the directories if they do not exist
if not os.path.exists(f"{DATA_PATH}/uniswap_v3"):
    os.makedirs(f"{DATA_PATH}/uniswap_v3")


def fetch_query(header: Dict, query: str, http: str) -> Any:
    """Send a query to the subgraph and return the response"""

    return requests.post(
        http,
        headers=header,
        json={"query": query},
        timeout=60,
    )


def to_do(chain: str) -> Iterable:
    """Check if the data is already fetched"""

    start_date = datetime.datetime.strptime(INFO_DICT[chain]["start_date"], "%Y-%m-%d")
    end_date = datetime.datetime.now()

    while start_date < end_date:

        if not os.path.exists(
            f"{DATA_PATH}/uniswap_v3/{chain}/swap/{start_date.date()}.jsonl"
        ):
            yield start_date

        start_date += datetime.timedelta(days=1)


def fetch_uniswap(date: datetime.datetime, chain: str = "polygon") -> None:
    """Fetches Uniswap data"""

    if not os.path.exists(f"{DATA_PATH}/uniswap_v3/{chain}/swap"):
        os.makedirs(f"{DATA_PATH}/uniswap_v3/{chain}/swap")

    # read the header
    with open(f"{DATA_PATH}/headers/uniswap_v3_{chain}.txt", encoding="utf-8") as f:
        header = {line.strip()[:-1]: next(f).strip() for line in f}

    # convert the dates to timestamps
    start_timestamp = int(date.timestamp())
    end_timestamp = int((date + datetime.timedelta(days=1)).timestamp())

    res_list = []
    progress_bar = tqdm(
        total=end_timestamp - start_timestamp,
        desc=f"Fetching {date.date()} data",
        unit="ts",
    )

    while True:
        time.sleep(5)

        # fetch the data
        response = requests.post(
            INFO_DICT[chain]["http"],
            headers=header,
            json={"query": UNISWAP_V3_QUERY.format(ts=start_timestamp)},
            timeout=60,
        )

        # check if the response is successful
        response.raise_for_status()

        # get the data
        data = response.json()

        # check if the data is empty
        if not data["data"]["swaps"]:
            raise ValueError("No data found")

        # yield the data
        last_timestamp = data["data"]["swaps"][-1]["timestamp"]
        res_list.append(
            [
                _
                for _ in data["data"]["swaps"]
                if (int(_["timestamp"]) != last_timestamp)
                & (int(_["timestamp"]) < end_timestamp)
            ]
        )

        progress_bar.update(int(last_timestamp) - int(start_timestamp))

        res_next = [
            _ for _ in data["data"]["swaps"] if int(_["timestamp"]) >= end_timestamp
        ]

        if res_next:
            break

        start_timestamp = last_timestamp

    date_str = date.strftime("%Y-%m-%d")
    with open(
        f"{DATA_PATH}/uniswap_v3/{chain}/swap/{date_str}.jsonl", "w", encoding="utf-8"
    ) as f:
        for res in res_list:
            for line in res:
                f.write(json.dumps(line) + "\n")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Subgraph Fetcher CLI")
    parser.add_argument(
        "--chain",
        default="polygon",
        help="The chain to fetch data from (e.g., polygon).",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    for date in to_do(args.chain):
        try:
            fetch_uniswap(date, chain=args.chain)
        except Exception as e:
            print(f"Error fetching {date.date()}: {e}")
            time.sleep(60)


if __name__ == "__main__":

    main()
