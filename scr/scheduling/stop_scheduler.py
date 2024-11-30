import os
import sys
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config import paths

# Load the config.json to access values
config_path = paths.CONFIG_PATH

with open(config_path, 'r') as json_file:
    config = json.load(json_file)

# Access the 'stop_flag_file' value from the JSON config
stop_flag_file = config["settings"].get("scrap_config", {}).get("stop_flag_file", "stop_flag.txt")

# Construct the path to the stop flag file using the path from paths.py
stop_flag_path = paths.STOP_FLAG_PATH

# Create the stop flag file and write a message to it
with open(stop_flag_path, 'w') as file:
    file.write("stop scheduled scrapping")

print(f"Stop flag created at: {stop_flag_path}")
