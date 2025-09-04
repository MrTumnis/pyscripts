import os
import sys
import argparse
import glob
import zipfile
from datetime import datetime

import polars as pl
from polars.exceptions import ColumnNotFoundError, SchemaError
from simple_term_menu import TerminalMenu

HOME = os.getenv("HOME")
DOWNLOADS = f"{HOME}/Downloads"

# Schema for BAM data
schema = {
    "column_1": pl.String,
    "column_2": pl.Float64,
    "column_3": pl.Float64,
    "column_4": pl.Float64,
    "column_5": pl.Float64,
    "column_6": pl.Float64,
    "column_7": pl.Float64,
    "column_8": pl.Float64,
    "column_9": pl.Float64,
    "column_10": pl.Int8,
    "column_11": pl.Int8,
    "column_12": pl.Int8,
    "column_13": pl.Int8,
    "column_14": pl.Int8,
    "column_15": pl.Int8,
    "column_16": pl.Int8,
    "column_17": pl.Int8,
    "column_18": pl.Int8,
    "column_19": pl.Int8,
    "column_20": pl.Int8,
    "column_21": pl.Int8,
}


def read_file(args):
    file_list = glob.glob("*.*")
    if not file_list:
        print("❌ No files found in the current directory.")
        sys.exit(1)

    files_menu = TerminalMenu(
        file_list,
        title="Choose the file to convert. (.zip files will be extracted first)",
    )
    selection = files_menu.show()
    file_path = file_list[selection]

    # Unzip if needed
    if file_path.endswith(".zip"):
        target_dir = os.path.splitext(file_path)[0]
        with zipfile.ZipFile(file_path, "r") as zip_file:
            zip_file.extractall(target_dir)
        print(f"✅ Extracted to {target_dir}")
        return  # Avoid continuing on .zip directly

    try:
        df_file = pl.scan_csv(
            file_path,
            separator=",",
            has_header=False,
            raise_if_empty=True,
            infer_schema_length=10000,
            infer_schema=False,
        )
    except Exception as e:
        print(f"❌ Failed to read file: {e}")
        sys.exit(1)

    col = df_file.collect_schema().names()

    # Strip leading/trailing characters from all string columns
    df_csv = df_file.with_columns([pl.col(c).str.strip_chars().alias(c) for c in col])

    # check datetime format
    time = df_csv.select(pl.col("column_1")).collect().row(1)
    t = time[0]

    # fmt = ("%Y-%m-%d %H:%M:%S", "%m/%d/%y %H:%M")

    met_fmt = "%Y-%m-%d %H:%M:%S"
    bam_fmt = "%m/%d/%y %H:%M"

    def try_parse(t, fmt):
        try:
            return datetime.strptime(t, fmt)
        except ValueError:
            return None

    time = try_parse(time[0], met_fmt)
    if time:
        print("Matched met_fmt:", time)
        df_time = df_csv.with_columns(
            pl.col("column_1")
            .str.to_datetime("%Y-%m-%d %H:%M:%S", strict=True)
            .dt.strftime("%Y-%m-%d %H:%M:%S")
            .alias("column_1")
        )

    else:
        time = try_parse(time[0], bam_fmt)
        if time:
            print("Matched bam_fmt:", time)

            df_time = df_csv.with_columns(
                pl.col("column_1")
                .str.to_datetime("%m/%d/%y %H:%M", strict=True)
                .dt.strftime("%Y-%m-%d %H:%M:%S")
                .alias("column_1")
            )
        else:
            print(
                "Format of datetime does not match any known format. Add a new one or fix the file."
            )

    try:
        # Only cast schema for BAM/PM files
        if any(x in file_path.lower() for x in ["pm10", "pm2.5", "bam"]):
            try:
                df_time = df_time.cast(schema)
            except SchemaError as e:
                print("⚠️ Schema mismatch. Proceeding without strict casting.")
                pass

        df = df_time.collect()

        # Add RECORD column if needed
        if args.rec and "column_r" not in df.columns:
            df = df.insert_column(1, pl.Series("column_r", list(range(df.height))))

        # Output format selection
        name, ext = os.path.splitext(file_path)

        if args.add_col_name and args.add_col_index is not None:
            fill_val = args.add_col_val if args.add_col_val is not None else None
            new_col = pl.Series(args.add_col_name, [fill_val] * df.height)

            try:
                df = df.insert_column(args.add_col_index, new_col)
                print(
                    f"✅ Added column '{args.add_col_name}' at index {
                        args.add_col_index
                    }"
                )
            except Exception as e:
                print(f"❌ Failed to add column: {e}")
                sys.exit(1)

        if args.dat:
            df_fmt = df.select([pl.format('"{}"', pl.col("column_1")).alias("column")])
            df_fnl = pl.concat([df_fmt, df.drop("column_1")], how="horizontal")
            print(df_fnl)
            df_fnl.write_csv(
                file=f"{DOWNLOADS + '/' + name}_1.dat",
                include_header=False,
                quote_style="never",
            )
            print(f"✅ .dat file written to {DOWNLOADS + '/' + name}_1.dat")

        elif args.csv:
            if ext.lower() == ".csv":
                print("ℹ️ This is already a .csv file")
            else:
                df.write_csv(
                    file=f"{DOWNLOADS + '/' + name}.csv",
                    include_header=True,
                    float_scientific=False,
                )
                print(f"✅ .csv file written to {DOWNLOADS + '/' + name}.csv")

        else:
            print("⚠️ No output format specified. Use --csv or --dat")

    except ColumnNotFoundError:
        print("⚠️ Column not found. Make sure headers are removed before converting.")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert BAM/raw CSV files to .csv or .dat format with optional RECORD column."
    )
    parser.add_argument(
        "-c", "--csv", action="store_true", help="Convert the file to .csv"
    )
    parser.add_argument(
        "-d",
        "--dat",
        action="store_true",
        help="Convert and reformat to .dat for server uploading",
    )
    parser.add_argument(
        "-r", "--rec", action="store_true", help="Add RECORD column for server upload"
    )
    parser.add_argument(
        "--add-col-name", type=str, help="Name of the new column to add"
    )
    parser.add_argument(
        "--add-col-index", type=int, help="Index (position) to insert the new column"
    )
    parser.add_argument(
        "--add-col-val",
        type=float,
        nargs="?",
        const=float("nan"),
        help="Value to fill the new column (default is null if not specified)",
    )
    args = parser.parse_args()

    read_file(args)
