# Gas

## Setup

```
git clone https://github.com/lyc0603/gas.git
cd gas
```

### Give execute permission to your script and then run `setup_repo.sh`

```
chmod +x setup_repo.sh
./setup_repo.sh
. venv/bin/activate
```

or follow the step-by-step instructions below between the two horizontal rules:

---

#### Create a python virtual environment

- MacOS / Linux

```bash
python3 -m venv venv
```

- Windows

```bash
python -m venv venv
```

#### Activate the virtual environment

- MacOS / Linux

```bash
. venv/bin/activate
```

- Windows (in Command Prompt, NOT Powershell)

```bash
venv\Scripts\activate.bat
```

#### Install toml

```
pip install toml
```

#### Install the project in editable mode

```bash
pip install -e ".[dev]"
```

## Set up the environmental variables

sign up as many Infura APIs as possible in https://www.infura.io/
put your Infura APIs in `.env`:

```
INFURA_API_KEYS = "API_1,API_2, ..., API_N"
```

## Set up the global constants
put your global constants in `constants.py`:

### Infura API base URL
```python
API_BASE = {
    "ethereum": "https://mainnet.infura.io/v3/",
    "arbitrum": "https://arbitrum-mainnet.infura.io/v3/",
    "polygon": "https://polygon-mainnet.infura.io/v3/",
    ...
}
```

### Uniswap V3 Factory contract address
```python
FACTORY = {
    "ethereum": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "arbitrum": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "polygon": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    ...
}
```

## Run the script

### Fetch all liquidity pools of Uniswap V3, and then save them to data.

Arguments:
- `--chain`: the chain you want to fetch pools from. (e.g. `ethereum`, `polygon`, `optimism`, `arbitrum`)
- `--start`: the block number when the factory contract is created on that chain. (e.g. `22757547`)
- `--end`: the block number you want to end at. (e.g. `68400000`)
- `--step`: the step size of each iteration. (e.g. `500000`)

```
python scripts/fetch_new_pools.py --chain polygon --start 22757547 --end 68400000 --step 500000
```

### Fetch all swap transactions of Uniswap V3, and then save them to data.
Arguments:
- `--chain`: the chain you want to fetch pools from. (e.g. `ethereum`, `polygon`, `optimism`, `arbitrum`)
- `--start`: the block number when the factory contract is created on that chain. (e.g. `22757547`)
- `--end`: the block number you want to end at. (e.g. `68400000`)
- `--step`: the step size of each iteration. (e.g. `1000`)

```
python scripts/fetch_swaps.py --chain polygon --start 22757547 --end 68400000 --step 1000
```