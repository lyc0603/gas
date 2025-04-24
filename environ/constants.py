"""This file contains the configuration settings for the market environment."""

import os
import json

from environ.settings import PROJECT_ROOT

DATA_PATH = PROJECT_ROOT / "data"
ABI_PATH = DATA_PATH / "abi"

INFURA_API_KEYS = str(os.getenv("INFURA_API_KEYS")).split(",")

# Infura API base URL
API_BASE = {
    "ethereum": "https://mainnet.infura.io/v3/",
    "arbitrum": "https://arbitrum-mainnet.infura.io/v3/",
    "polygon": "https://polygon-mainnet.infura.io/v3/",
}

# Factory Addresses
FACTORY = {
    "arbitrum": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "polygon": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
}

# ABI
UNISWAP_V3_FACTORY_ABI = json.load(open(ABI_PATH / "v3factory.json", encoding="utf-8"))
UNISWAP_V3_POOL_ABI = json.load(open(ABI_PATH / "v3pool.json", encoding="utf-8"))
