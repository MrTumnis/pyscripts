#!/{HOME}/{USER}/myenv/bin/python
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
#     "rich",
#     "simple-term-menu",
# ]
# ///

import sys
import os
import datetime as dt
import glob
import re
import pandas as pd
from rich import print as rprint
from rich.console import Console
from rich.theme import Theme
from simple_term_menu import TerminalMenu

HOME = os.getenv("HOME")
DOWNLOAD = f"{HOME}/"

custom_theme = Theme(
    {
        "info": "dim cyan",
        "success": "dodger_blue2",
        "warning": "magenta",
        "error": "bold red",
    }
)
console = Console(theme=custom_theme)

# ---------- IO ----------


def file_read(file_path: str) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()

        if "STATION" in df.columns:
            df = df.drop(columns=["STATION"])

        if "TIMESTAMP" not in df.columns:
            console.print("Missing TIMESTAMP column.", style="error")
            return None

        # Parse time, coerce bad rows, then clean numerics
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
        df = df.dropna(subset=["TIMESTAMP"])

        value_cols = [c for c in df.columns if c != "TIMESTAMP"]
        for c in value_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # Drop rows that are entirely NaN across value columns
        df = df.dropna(how="all", subset=value_cols)

        # Ensure strictly increasing index for resample
        df = df.sort_values("TIMESTAMP").reset_index(drop=True)
        return df
    except Exception as e:
        console.print(f"#1 Error occurred: {e}", style="error")
        return None


# ---------- Time utils ----------


VALID_MINUTES = [5, 15, 30, 60, 1440]
ALIAS = {5: "5min", 15: "15min", 30: "30min", 60: "H", 1440: "D"}


def detect_interval_minutes(df: pd.DataFrame) -> int:
    # Use mode of first few diffs in minutes
    diffs = df["TIMESTAMP"].diff().dropna()
    if diffs.empty:
        raise ValueError("Cannot infer interval from a single row.")
    mins = (diffs.dt.total_seconds() / 60).round().astype(int)
    interval = mins.mode().iat[0]
    if interval not in VALID_MINUTES:
        raise ValueError(f"Unsupported interval: {interval} minutes")
    return interval


def time_check(file_path: str) -> int:
    df = file_read(file_path)
    if df is None:
        sys.exit(1)
    interval = detect_interval_minutes(df)
    msg = {
        5: "This is a five-min file",
        15: "This is a 15-min file",
        30: "This is a 30-min file",
        60: "This is a 60-min file",
        1440: "This is a Daily file",
    }[interval]
    rprint(msg)
    return interval


# ---------- Resampling ----------


def build_agg_map(columns: list[str]) -> dict[str, str]:
    """
    Decide per-column aggregation based on name patterns.
    Defaults to mean.
    """
    agg = {}
    for col in columns:
        if re.search(r"Avg$", col):
            agg[col] = "mean"
        elif re.search(r"Max$", col):
            agg[col] = "max"
        elif re.search(r"Min$", col):
            agg[col] = "min"
        elif re.search(r"Tot$", col):
            agg[col] = "sum"
        # wind comps without trailing n/x
        elif re.search(r"^W.*(?<![nx])$", col):
            agg[col] = "mean"
        elif re.search(r"^S.*(?<![nxg])$", col):  # sigma without trailing n/x/g
            agg[col] = "mean"
        else:
            agg[col] = "mean"
    return agg


def time_change(file_path: str) -> tuple[pd.DataFrame, int]:
    df = file_read(file_path)
    if df is None:
        sys.exit(1)

    current = detect_interval_minutes(df)
    value_cols = [c for c in df.columns if c != "TIMESTAMP"]
    col_order = df.columns.tolist()

    # Menu
    options = ["5", "15", "30", "60", "1440", "exit"]
    menu = TerminalMenu(options, title="What aggregation would you like to convert to?")
    sel = menu.show()
    if sel is None:
        sys.exit(1)
    choice = options[sel]
    if choice == "exit":
        sys.exit(0)

    try:
        target = int(choice)
    except ValueError:
        console.print(
            f"Please specify a valid time as an integer {VALID_MINUTES}",
            style="warning",
        )
        sys.exit(1)

    if target not in VALID_MINUTES:
        console.print(f"Unsupported target interval: {target}", style="error")
        sys.exit(1)
    if target < current:
        console.print(
            "Cannot convert to a *shorter* interval than the source.", style="error"
        )
        sys.exit(1)

    freq = ALIAS[target]

    # Resample in one pass with a per-column agg map
    g = build_agg_map(value_cols)

    df_idx = df.set_index("TIMESTAMP")
    try:
        res = df_idx.resample(freq, closed="right", label="right").agg(g)
    except Exception as e:
        console.print(f"#2 Error occurred during resample: {e}", style="error")
        sys.exit(1)

    # Reorder to original order if still present
    existing = [c for c in col_order if c in res.columns or c == "TIMESTAMP"]
    res = res.reindex(columns=[c for c in existing if c != "TIMESTAMP"])
    res = res.round(3)

    # Restore TIMESTAMP as a column
    res = res.reset_index()

    return res, target


# ---------- Export ----------


def time_file(file_path: str) -> None:
    date = dt.datetime.now()
    # directory = os.path.dirname(file_path)
    directory = DOWNLOAD
    basename = os.path.basename(file_path)

    df_out, minutes = time_change(file_path)

    new_filename = f"{date.strftime('%Y%m%d')}-{minutes}-min_{basename}"
    output_path = os.path.join(directory, new_filename)

    df_out.to_csv(output_path, index=False)
    console.print(
        f"Success. File converted to a {minutes}-min datafile", style="success"
    )


# ---------- CLI ----------


if __name__ == "__main__":
    cwd = os.getcwd()
    while True:
        files = glob.glob(os.path.join(cwd, "*.csv"))
        if not files:
            console.print("No CSV files found in current directory.", style="error")
            sys.exit(1)
        files_menu = TerminalMenu(files, title="Select a csv file")
        idx = files_menu.show()
        if idx is None:
            sys.exit(1)
        file_path = files[idx]
        if file_path.endswith(".csv"):
            break
        console.print("This is not a CSV file. Try again.", style="error")

    # Show detected interval once, then run
    _ = time_check(file_path)
    time_file(file_path)
