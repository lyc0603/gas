"""
Subgraph fetcher
"""

import datetime
import time

import requests

from environ.constants import DATA_PATH

UNISWAP_V2_HTTP = "https://thegraph.com/explorer/api/playground/QmZzsQGDmQFbzYkv2qx4pVnD6aVnuhKbD3t1ea7SAvV7zE"
UNISWAP_V3_HTTP = "https://thegraph.com/explorer/api/playground/QmTZ8ejXJxRo7vDBS4uwqBeGoxLSWbhaA7oXa1RvxunLy7"

with open(f"{DATA_PATH}/v2_header.txt", encoding="utf-8") as f:
    v2_header = {line.strip()[:-1]: next(f).strip() for line in f}

with open(f"{DATA_PATH}/v3_header.txt", encoding="utf-8") as f:
    v3_header = {line.strip()[:-1]: next(f).strip() for line in f}


def fetch_uniswap(date: datetime.datetime) -> None:
    """Fetches Uniswap data"""

    # convert the dates to timestamps
    start_timestamp = int(date.timestamp())
    end_timestamp = int((date + datetime.timedelta(days=1)).timestamp())

    res_list = []

    while True:
        time.sleep(3)

        # fetch the data
        response = requests.post(
            UNISWAP_V2_HTTP,
            headers=v2_header,
            json={"query": UNISWAP_V2_QUERY.format(ts=start_timestamp)},
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
        res_next = [
            _ for _ in data["data"]["swaps"] if int(_["timestamp"]) >= end_timestamp
        ]

        if res_next:
            break

        start_timestamp = last_timestamp
        print(f"Fetching data for {date}, from {start_timestamp} to {end_timestamp}")

    date_str = date.strftime("%Y-%m-%d")
    with open(
        f"{DATA_PATH}/uniswap_v2/swap/{date_str}.jsonl", "w", encoding="utf-8"
    ) as f:
        for res in res_list:
            for item in res:
                f.write(f"{item}\n")


UNISWAP_V2_QUERY = """{{
  swaps(
    first: 1000
    orderBy: timestamp
    orderDirection: asc
    where: {{timestamp_gte: "{ts}"}}
  ) {{
    transaction {{
      id
      blockNumber
    }}
    id
    timestamp
    amount0In
    amount0Out
    amount1In
    amount1Out
    amountUSD
    from
    sender
    to
    pair {{
      id
      token0 {{
        id
        symbol
      }}
      token1 {{
        id
        symbol
      }}
    }}
  }}
}}"""

UNISWAP_V3_QUERY = """{{
  swaps(
    first: 1000
    orderBy: timestamp
    orderDirection: asc
    where: {{timestamp_gte: "{ts}"}}
  ) {{
    amount0
    amount1
    amountUSD
    origin
    id
    recipient
    sender
    token0 {{
      id
      symbol
    }}
    token1 {{
      symbol
      id
    }}
    transaction {{
      blockNumber
      gasPrice
      gasUsed
      id
    }}
    timestamp
  }}"""

if __name__ == "__main__":
    fetch_uniswap(datetime.datetime(2024, 1, 1))
