"""This file contains the configuration settings for the market environment."""

from environ.settings import PROJECT_ROOT

DATA_PATH = PROJECT_ROOT / "data"
ABI_PATH = DATA_PATH / "abi"

# User Header
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"

# Event Block Ethereum
START_BLOCK_ETHEREUM = 14688876  # 2022-05-01 12:59:51 AM UTC
END_BLOCK_ETHEREUM = 14689597  # 2022-05-01 03:38:26 AM UTC

# Event Block Arbitrum
START_BLOCK_ARBITRUM = 86024107  # 2022-05-01 12:59:51 AM UTC
END_BLOCK_ARBITRUM = 86061549  # 2022-05-01 03:38:26 AM UTC

# Infura API base URL
ETHEREUM_INFURA_API_BASE = "https://mainnet.infura.io/v3/"
ARBITRUM_INFURA_API_BASE = "https://arbitrum-mainnet.infura.io/v3/"
POLYGON_INFURA_API_BASE = "https://polygon-mainnet.infura.io/v3/"

# Pool Addresses
ETHEREUM_USDC_ETH_500_V3_POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
ARBITRUM_USDC_ETH_500_V3_POOL = "0xC6962004f452bE9203591991D15f6b388e09E8D0"
POLYGON_USDC_ETH_500_V3_POOL = "0xA4D8c89f0c20efbe54cBa9e7e7a7E509056228D9"

ETHEREUM_WETH_USDT_500_V3_POOL = "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36"
ARBITRUM_WETH_USDT_500_V3_POOL = "0x641C00A822e8b671738d32a431a4Fb6074E5c79d"

# Factory Addresses
POLYGON_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
POLYGON_V3_FACTORY_START_BLOCK = 22757547
POLYGON_V3_FACTORY_END_BLOCK = 68400000
ARBITRUM_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
