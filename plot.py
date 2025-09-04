import polars as pl
import sys
import os
import matplotlib.pyplot as plt
# import webbrowser
# import altair as alt
from simple_term_menu import TerminalMenu

cp = os.getcwd()

if len(sys.argv) > 1: 
    file_name = sys.argv[1]
    file = pl.scan_csv(f'{cp}/{file_name}', has_header=True, raise_if_empty=True, ignore_errors=True)

file = file.collect()

first_row = file.row(0)

if any(isinstance(value, str) for value in first_row):
    df = file.slice(1)

col = file.columns
chart_len = df.height

if col == []:
    print('No column names found.')
    sys.exit()

col_menu = TerminalMenu(col, title=('Choose the x axis for the plot'))
selectionx = col_menu.show()
x = col[selectionx]

col_menu = TerminalMenu(col, title=('Choose the y axis for the plot'))
selectiony = col_menu.show()
y = col[selectiony]

col_menu = TerminalMenu(col, title=('Choose the second y axis for the plot'))
selectiony2 = col_menu.show()
y2 = col[selectiony2]

x = df[x].to_list()
y1 = df[y].to_list()
y2 = df[y2].to_list()

plt.xlim(1,4)
plt.ylim(0,360)
# Plot using matplotlib
plt.figure(figsize=(10, 6))
plt.plot(x, y1, label=y1, marker="o")
plt.plot(x, y2, label=y2, marker="s")

plt.title("Data Comparison")
plt.xlabel(x)
plt.ylabel(y)
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
# df_piv = df.unpivot(on=x, index=[y,y2], variable_name='x1', value_name='y1' )
                    
# print(df_piv)

# chart = df_piv.plot.line(
#     x= alt.X('x1', title=f'{x1}'),
#     y= alt.Y('y1', title=f'{y}', scale=alt.Scale(domain=[-5,360]))).properties(width=chart_len, height=650)

# chart.save('linechart.html')

# file_path = os.path.abspath('linechart.html')
# webbrowser.open('file://' + file_path)
