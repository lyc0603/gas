"""
Script to process the swaps data
"""

import argparse
import json
from glob import glob
import os
import time
import datetime

from web3 import Web3
from web3.providers import HTTPProvider

from environ.constants import (
    ARBITRUM_INFURA_API_BASE,
    DATA_PATH,
    ETHEREUM_INFURA_API_BASE,
    POLYGON_INFURA_API_BASE,
)

API_BASE = {
    "ethereum": ETHEREUM_INFURA_API_BASE,
    "arbitrum": ARBITRUM_INFURA_API_BASE,
    "polygon": POLYGON_INFURA_API_BASE,
}


# os.makedirs(f"{DATA_PATH}/polygon/txn_hash", exist_ok=True)

# load the files list from the directory
chain = "polygon"
files = glob(f"{DATA_PATH}/{chain}/swap/*.jsonl")

txn_set = set()
iter_count = 0
batch_num = 0
txn_num = 0

for file in files:
    with open(file, "r", encoding="utf-8") as f:
        for line in f:
            event = json.loads(line)
            iter_count += 1
            txn_set.add(event["transactionHash"])

            if iter_count % 10000 == 0:
                iter_count = 0
                batch_num += 1
                with open(
                    f"{DATA_PATH}/{chain}/txn_hash/{batch_num}.jsonl",
                    "w",
                    encoding="utf-8",
                ) as f:
                    for txn in txn_set:
                        f.write(json.dumps({"transactionHash": txn}) + "\n")
                txn_num += len(txn_set)
                txn_set.clear()

# if txn_set:
#     batch_num += 1
#     with open(
#         f"{DATA_PATH}/{chain}/txn_hash/{batch_num}.jsonl",
#         "w",
#         encoding="utf-8",
#     ) as f:
#         for txn in txn_set:
#             f.write(json.dumps({"transactionHash": txn}) + "\n")

print(f"Total number of transactions: {txn_num}")

# INFURA_API_KEYS = str(os.getenv("INFURA_API_KEYS")).split(",")
# w3 = Web3(HTTPProvider(API_BASE["polygon"] + INFURA_API_KEYS[0]))

# # before_time = datetime.datetime.now()
# # _ = w3.eth.get_transaction(
# #     "0x591bfe05e2a0cd7e131fafc60c698c772715c1c8a1074d047af20673a2c7625f"
# # )
# # print(_)
# # after_time = datetime.datetime.now()
# # print(after_time - before_time)

# with open(f"{DATA_PATH}/polygon/txn_hash/1.jsonl", "r", encoding="utf-8") as f:
#     hash_list = []
#     counter = 0
#     for line in f:
#         hash_list.append(json.loads(line)["transactionHash"])
#         counter += 1
#         if counter == 1:
#             with w3.batch_requests() as batch:
#                 for txn in hash_list:
#                     batch.add(w3.eth.get_transaction(txn))
#                 responses = batch.execute()
#             hash_list.clear()
#             counter = 0
#             print(responses)
#             time.sleep(0.5)
