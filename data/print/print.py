import sys
import os
import json
import pandas as pd
from colorama import Fore, Style
from tabulate import tabulate

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config import paths

# Load configuration from config file
config_path = paths.CONFIG_PATH

with open(config_path, 'r') as json_file:
    config = json.load(json_file)
    save_path = config["data"].get("save_path", "")
    download_dir = os.path.join(save_path, "loaded")
    file_path = os.path.join(download_dir, 'incidents.csv')

# Read the CSV file
df = pd.read_csv(file_path)

# Truncate each value to 15 characters
df = df.applymap(lambda x: str(x)[:15])

# Define colors for each column
column_colors = {
    'ID': Fore.RED,
    'Year': Fore.GREEN,
    'Source Database': Fore.YELLOW,
    'Attack Description': Fore.CYAN,
    'Country': Fore.MAGENTA,
    'Industry Type': Fore.BLUE,
    'TI Safe Score': Fore.WHITE,
    'Link at the Internet': Fore.LIGHTBLACK_EX
}

# Apply colors to each row
colored_df = []
for index, row in df.iterrows():
    colored_row = []
    for column, value in row.items():
        color = column_colors.get(column, Fore.WHITE)
        colored_row.append(f"{color}{value}{Style.RESET_ALL}")
    colored_df.append(colored_row)

# Print the table with colors
print(tabulate(colored_df, headers=df.columns, tablefmt="grid", showindex=False))
