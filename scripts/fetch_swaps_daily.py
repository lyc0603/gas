"""
UniswapV3 Swap fetcher that chunks the chain per calendar day with
boundaries at midnight GMT
"""

# ─── standard library ────────────────────────────────────────────────────────
import argparse, glob, json, logging, multiprocessing as mp, os, time
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ─── third-party ─────────────────────────────────────────────────────────────
from tqdm import tqdm
from web3 import Web3, HTTPProvider
from web3.exceptions import Web3RPCError

# ─── project-local ───────────────────────────────────────────────────────────
from environ.constants import (
    UNISWAP_V3_POOL_ABI,
    API_BASE,
    DATA_PATH,
    INFURA_API_KEYS,
)  # type: ignore
from environ.utils import _fetch_events_for_all_contracts, to_dict  # type: ignore

# ─── logging setup (unchanged) ───────────────────────────────────────────────
(DATA_PATH / "log").mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=DATA_PATH / "log" / "error.log",
    filemode="a",
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.ERROR,
)

# ─── precision / global caches (unchanged) ───────────────────────────────────
getcontext().prec = 60
WETH = Web3.to_checksum_address("0xC02aaA39b223FE8D0A0E5C4F27eAD9083C756Cc2")
USDC = Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
USDT = Web3.to_checksum_address("0xdAC17F958D2ee523a2206206994597C13D831ec7")
DAI = Web3.to_checksum_address("0x6B175474E89094C44Da98b954EedeAC495271d0F")
STABLES = {USDT}
WETH_USDC_POOL = Web3.to_checksum_address("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640")
_decimals_cache: Dict[str, int] = {}
_symbol_cache: Dict[str, str] = {}
_weth_usd_cache: Dict[int, float] = {}
_block_ts_cache: Dict[int, int] = {}
_token_weth_cache: Dict[Tuple[str, int], Optional[float]] = {}


# ──────────────────────────────────────────────────────────────────────────────
# ERC-20 helpers
# ──────────────────────────────────────────────────────────────────────────────
def erc20_decimals(w3: Web3, token: str) -> int:
    if token in _decimals_cache:
        return _decimals_cache[token]
    abi = [
        {
            "constant": True,
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "stateMutability": "view",
            "type": "function",
        }
    ]
    try:
        d = w3.eth.contract(address=token, abi=abi).functions.decimals().call()
    except Exception:
        d = 18
    _decimals_cache[token] = d
    return d


def erc20_symbol(w3: Web3, token: str) -> str:
    if token in _symbol_cache:
        return _symbol_cache[token]
    abi = [
        {
            "constant": True,
            "inputs": [],
            "name": "symbol",
            "outputs": [{"name": "", "type": "string"}],
            "stateMutability": "view",
            "type": "function",
        }
    ]
    try:
        raw = w3.eth.contract(address=token, abi=abi).functions.symbol().call()
        sym = raw.decode() if isinstance(raw, bytes) else raw
        if not sym or not sym.isprintable():
            raise ValueError
    except Exception:
        sym = "UNK"
    _symbol_cache[token] = sym
    return sym


# ──────────────────────────────────────────────────────────────────────────────
# price helpers
# ──────────────────────────────────────────────────────────────────────────────
def sqrtpx96_to_ratio(x: int) -> Decimal:
    """(token1/token0) ratio from sqrtPriceX96."""
    return (Decimal(x) ** 2) / Decimal(2**192)


def safe_slot0(pool, block: int | None = None):
    try:
        return pool.functions.slot0().call(block_identifier=block)
    except Exception:
        return pool.functions.slot0().call()


def weth_price_usd(w3: Web3, block: int) -> float:
    if block in _weth_usd_cache:
        return _weth_usd_cache[block]
    pool = w3.eth.contract(address=WETH_USDC_POOL, abi=UNISWAP_V3_POOL_ABI)
    sqrtpx96, *_ = safe_slot0(pool, block)
    price = float(sqrtpx96_to_ratio(sqrtpx96) * Decimal(10 ** (6 - 18)))
    _weth_usd_cache[block] = price
    return price


def token_price_in_weth(
    w3: Web3,
    token: str,
    block: int,
    weth_pools: Dict[str, List[str]],
) -> Optional[float]:
    """Best token/WETH price among indexed pools."""
    if token == WETH:
        return 1.0
    key = (token, block)
    if key in _token_weth_cache:
        return _token_weth_cache[key]

    for pool_addr in weth_pools.get(token, []):
        try:
            pool = w3.eth.contract(address=pool_addr, abi=UNISWAP_V3_POOL_ABI)
            t0, t1 = pool.functions.token0().call(), pool.functions.token1().call()
            sqrtpx96, *_ = safe_slot0(pool, block)
            d0, d1 = erc20_decimals(w3, t0), erc20_decimals(w3, t1)
            r = sqrtpx96_to_ratio(sqrtpx96)

            if t0 == token and t1 == WETH:  # price = WETH / token
                price = 1 / (r * Decimal(10 ** (d0 - d1)))
            elif t1 == token and t0 == WETH:  # price = token / WETH
                price = r * Decimal(10 ** (d0 - d1))
            else:
                continue

            _token_weth_cache[key] = float(price)
            return _token_weth_cache[key]

        except Exception:
            continue

    _token_weth_cache[key] = None
    return None


# ──────────────────────────────────────────────────────────────────────────────
# pool metadata utilities
# ──────────────────────────────────────────────────────────────────────────────
def load_pool_info(chain: str) -> Dict[str, Dict[str, Any]]:
    """Return {pool → {"token0", "token1"}} from stored PoolCreated logs."""
    info: Dict[str, Dict[str, Any]] = {}
    for p in (DATA_PATH / chain / "pool").glob("*.json*"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    if not isinstance(data, list):
                        data = [data]
                except json.JSONDecodeError:
                    f.seek(0)
                    data = (json.loads(l) for l in f if l.strip())
                for evt in data:
                    if evt.get("event") == "PoolCreated":
                        info[evt["args"]["pool"]] = {
                            "token0": evt["args"]["token0"],
                            "token1": evt["args"]["token1"],
                        }
        except Exception as e:
            logging.error(f"load_pool_info {p.name}: {e}")
    return info


def build_weth_index(pool_info: Dict[str, Dict[str, Any]]) -> Dict[str, List[str]]:
    """token → list[pools with token & WETH]"""
    idx: Dict[str, List[str]] = {}
    for pool, meta in pool_info.items():
        if WETH not in (meta["token0"], meta["token1"]):
            continue
        other = meta["token1"] if meta["token0"] == WETH else meta["token0"]
        idx.setdefault(other, []).append(pool)
    return idx


# ──────────────────────────────────────────────────────────────────────────────
# minor helpers used by main
# ──────────────────────────────────────────────────────────────────────────────
def extract_pool(chain: str = "ethereum") -> List[Tuple[str, int]]:
    pools: List[Tuple[str, int]] = []
    for fp in glob.glob(str(DATA_PATH / chain / "pool" / "*.json")):
        with open(fp, "r", encoding="utf-8") as f:
            for line in f:
                evt = json.loads(line)
                pools.append((evt["args"]["pool"], evt["blockNumber"]))
    pools.sort(key=lambda x: x[1])
    return pools


def split_blocks(
    start: int,
    end: int,
    step: int,
    pools: List[Tuple[str, int]],
    chain: str,
) -> List[Tuple[int, int, List[str]]]:
    out = []
    for i in tqdm(
        range(start // step * step, end // step * step, step), desc="Splitting Blocks"
    ):
        if (DATA_PATH / chain / "swap" / f"{i}_{i+step-1}.jsonl").exists():
            continue
        out.append((i, i + step - 1, [p[0] for p in pools if p[1] <= i + step - 1]))
    return out


# ──────────────────────────────────────────────────────────────────────────────
# core: fetch & enrich swaps
# ──────────────────────────────────────────────────────────────────────────────
def fetch_swap_events(
    chain: str,
    frm: int,
    to: int,
    pools: List[str],
    http: str,
    out: Path,
    abi: List[dict],
    pool_info: Dict[str, Dict[str, Any]],
    weth_index: Dict[str, List[str]],
) -> None:
    try:
        w3 = Web3(HTTPProvider(http))
        swap_evt = w3.eth.contract(abi=abi).events.Swap
        logs = _fetch_events_for_all_contracts(
            w3, swap_evt, {"address": pools}, frm, to
        )
        events = to_dict(logs)

        with out.open("a", encoding="utf-8") as fh:
            for ev in events:
                try:
                    pool = Web3.to_checksum_address(ev["address"])
                    t0, t1 = pool_info[pool]["token0"], pool_info[pool]["token1"]
                    sym0, sym1 = erc20_symbol(w3, t0), erc20_symbol(w3, t1)
                    dec0 = erc20_decimals(w3, t0)
                    dec1 = erc20_decimals(w3, t1)
                    amt0 = abs(int(ev["args"]["amount0"])) / 10**dec0
                    block = ev["blockNumber"]

                    # ──► timestamp (cached per block)  ═══════════════════════
                    if block not in _block_ts_cache:
                        _block_ts_cache[block] = w3.eth.get_block(block).timestamp
                    ts = _block_ts_cache[block]
                    # ════════════════════════════════════════════════════════

                    # price logic (unchanged)
                    if t0 in STABLES:
                        p0_usd = 1.0
                    elif t0 == WETH:
                        p0_usd = weth_price_usd(w3, block)
                    else:
                        p0_w = token_price_in_weth(w3, t0, block, weth_index)
                        p0_usd = p0_w * weth_price_usd(w3, block) if p0_w else None
                    if p0_usd is None and t1 == WETH:
                        sqrtpx96 = int(ev["args"]["sqrtPriceX96"])
                        p0_w = (
                            1 / sqrtpx96_to_ratio(sqrtpx96) * Decimal(10 ** (18 - dec0))
                        )
                        p0_usd = float(p0_w) * weth_price_usd(w3, block)

                    usd_vol = amt0 * p0_usd if p0_usd is not None else None

                    ev.update(
                        {
                            "timestamp": ts,
                            "token0_symbol": sym0,
                            "token1_symbol": sym1,
                            "token0_decimals": dec0,
                            "token1_decimals": dec1,
                            "amountUSD": usd_vol,
                        }
                    )
                except Exception as e:
                    ev["amountUSD"] = None
                    logging.error(f"enrich error: {e}")

                fh.write(json.dumps(ev) + "\n")

    except Web3RPCError as e:
        try:
            if -32005 == json.loads(e.args[0].replace("'", '"'))["code"]:
                mid = (frm + to) // 2
                fetch_swap_events(
                    chain, frm, mid, pools, http, out, abi, pool_info, weth_index
                )
                fetch_swap_events(
                    chain, mid + 1, to, pools, http, out, abi, pool_info, weth_index
                )
                return
        except Exception:
            pass
        logging.error(f"RPC error {frm}-{to}: {e}")
    except Exception as e:
        logging.error(f"Error {frm}-{to}: {e}")


# ─── GMT timezone helper ─────────────────────────────────────────────────────
GMT = timezone(timedelta(hours=0), name="GMT")  # fixed, no DST


# ─── timestamp → block (binary search) ───────────────────────────────────────
def timestamp_to_block(
    w3: Web3, target_ts: int, low: int = 0, high: Optional[int] = None
) -> int:
    """Return the first block whose timestamp ≥ `target_ts`."""
    if high is None:
        high = w3.eth.block_number
    while low < high:
        mid = (low + high) // 2
        if w3.eth.get_block(mid).timestamp < target_ts:
            low = mid + 1
        else:
            high = mid
    return low


# ─── split chain into day-sized block ranges (00:00 GMT cut-offs) ────────────
def split_days(start_date: str, end_date: str, w3: Web3) -> List[Tuple[int, int, str]]:
    """Return [(from_block, to_block, 'YYYYMMDD')] for each GMT day."""
    dt0 = datetime.fromisoformat(start_date).replace(tzinfo=GMT)
    dt1 = datetime.fromisoformat(end_date).replace(tzinfo=GMT)
    ts0, ts1 = int(dt0.timestamp()), int(dt1.timestamp())

    ranges: List[Tuple[int, int, str]] = []
    day_start = ts0
    while day_start < ts1:
        next_day = day_start + 86_400
        frm_blk = timestamp_to_block(w3, day_start)
        to_blk = timestamp_to_block(w3, next_day) - 1
        if to_blk >= frm_blk:
            date_str = datetime.fromtimestamp(day_start, GMT).strftime(
                "%Y%m%d"
            )  # ← no dashes
            ranges.append((frm_blk, to_blk, date_str))
        day_start = next_day
    return ranges


# ─── CLI ─────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch daily Uniswap-V3 swaps (midnight GMT)"
    )
    p.add_argument("--chain", default="ethereum")
    p.add_argument(
        "--start_date",
        required=True,
        help="inclusive start date YYYY-MM-DD (midnight GMT)",
    )
    p.add_argument(
        "--end_date", required=True, help="exclusive end date YYYY-MM-DD (midnight GMT)"
    )
    return p.parse_args()


# ─── multiprocessing worker ──────────────────────────────────────
def worker(
    chain: str,
    frm: int,
    to: int,
    pools: List[str],
    q: mp.Queue,
    out: Path,
    abi: List[dict],
    pool_info: Dict[str, Dict[str, Any]],
    weth_index: Dict[str, List[str]],
):
    http = q.get()
    time.sleep(0.1)
    try:
        fetch_swap_events(chain, frm, to, pools, http, out, abi, pool_info, weth_index)
    finally:
        q.put(http)


def _run_task(args_tuple):
    worker(*args_tuple)
    return 1


# ─── main ────────────────────────────────────────────────────────────────────
def main() -> None:
    args = parse_args()
    (DATA_PATH / args.chain / "swap").mkdir(parents=True, exist_ok=True)

    pool_info = load_pool_info(args.chain)
    weth_index = build_weth_index(pool_info)
    pool_bnums = extract_pool(args.chain)  # [(pool_addr, creation_block)]

    w3_lookup = Web3(HTTPProvider(API_BASE[args.chain] + INFURA_API_KEYS[0]))
    day_ranges = split_days(args.start_date, args.end_date, w3_lookup)

    # shared queue of RPC endpoints
    mgr: mp.Manager = mp.Manager()
    q: mp.Queue = mgr.Queue()
    for key in INFURA_API_KEYS:
        q.put(API_BASE[args.chain] + key)

    # one tuple per calendar-day chunk
    tasks = [
        (
            args.chain,
            frm,
            to,
            [p for p, bn in pool_bnums if bn <= to],  # pools live that day
            q,
            DATA_PATH
            / args.chain
            / "swap"
            / f"infura_uniswap_v3_swaps_{date}.jsonl",  # ← new filename
            UNISWAP_V3_POOL_ABI,
            pool_info,
            weth_index,
        )
        for frm, to, date in day_ranges
    ]

    procs = min(len(INFURA_API_KEYS), os.cpu_count() or 1)
    with (
        mp.Pool(procs) as pool,
        tqdm(total=len(tasks), desc="Fetching days", unit="day") as bar,
    ):
        # imap_unordered yields as soon as a day finishes → advance bar
        for _ in pool.imap_unordered(_run_task, tasks, chunksize=1):
            bar.update()


if __name__ == "__main__":
    main()
