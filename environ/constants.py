"""This file contains the configuration settings for the market environment."""

from environ.settings import PROJECT_ROOT

DATA_PATH = PROJECT_ROOT / "data"
ABI_PATH = DATA_PATH / "abi"

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
