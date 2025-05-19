from pathlib import Path
import json
import pandas as pd
from tqdm import tqdm


def convert_json_to_csv(json_path, save_path=None):
    """
    Convert a .jsonl file to a flattened DataFrame and optionally save it as CSV.
    """
    json_path = Path(json_path)  # ensures Path-like behaviour

    # 1. Read one JSON object per line
    records = []
    with json_path.open("r") as f:
        for line in f:
            line = line.strip()
            if line:  # skip blank lines
                records.append(json.loads(line))

    # 2. Flatten into a DataFrame
    df = pd.json_normalize(records)
    df.columns = [c.replace("args.", "") for c in df.columns]
    df.rename(
        columns={
            "transactionHash": "transaction",
            "token0": "token0_id",
            "token1": "token1_id",
            "amount0": "raw_amount0",
            "amount1": "raw_amount1",
            "address": "pool",
        },
        inplace=True,
    )
    df["amount0"] = df["raw_amount0"] / 10 ** df["token0_decimals"]
    df["amount1"] = df["raw_amount1"] / 10 ** df["token1_decimals"]

    # 3. Decide where (or whether) to save
    if save_path is None:
        save_path = json_path.with_suffix(".csv")  # e.g. xxx.jsonl → xxx.csv

    if save_path:  # "" means “don’t save”
        save_path = Path(save_path)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(save_path, index=False)

    return df


def main():
    # 1. Locate input / output folders
    base_dir = Path.cwd()
    swap_dir = base_dir / "data" / "ethereum" / "swap"
    out_dir = base_dir / "data" / "ethereum" / "swap_csv"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 2. Collect all *.jsonl files
    jsonl_files = sorted(swap_dir.glob("*.jsonl"))
    if not jsonl_files:
        print(f"No .jsonl files found in {swap_dir}")
        return

    # 3. Convert each file, with a one-tick-per-file progress bar
    for json_file in tqdm(jsonl_files, desc="Converting swap logs", unit="file"):
        csv_path = out_dir / f"{json_file.stem}.csv"
        convert_json_to_csv(json_file, save_path=csv_path)


if __name__ == "__main__":
    main()
