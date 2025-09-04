#!/home/thomas/dev/python/scripts/tools/.venv

import polars as pl
import os
import sys

"""Script for quick column creation for pasting into central servers when creating station apps"""

cp = os.getcwd()
file_name = sys.argv[1]
station, agg = file_name.rsplit('_', 1)

if len(sys.argv) > 1:
    file = pl.scan_csv(f'{cp}/{file_name}', has_header=True, raise_if_empty=True,
                       ignore_errors=True)  # , skip_lines=1, truncate_ragged_lines=True)

    # print(file.collect())

else:
    print("Need to add a dat or csv file as an argument to the script")
    sys.exit()

file = file.collect()
# print('df:', file)

first_row = file.row(0)
# print('first row:', first_row)

if any(isinstance(station, str) for station in first_row):
    df = file.slice(1)

columns = df.columns
test = file.columns
# print('columns:', test)

for col in columns:
    if col == 'TIMESTAMP':
        col.strip('TIMESTAMP')
    elif col == 'RECORD':
        col.strip('RECORD')
    else:
        print(col, end=" ")
