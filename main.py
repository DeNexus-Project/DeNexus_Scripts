import subprocess
import json
from config import paths

if __name__ == "__main__":
    # Load the configuration from the JSON file
    config_path = paths.CONFIG_PATH
    with open(config_path, 'r') as json_file:
        config = json.load(json_file)

    # Check if the 'active' value under 'scrap_config' is True
    scrap_config_active = config["settings"].get("scrap_config", {}).get("active", False)

    # Define the script paths
    scrap_script_path = paths.SCRAP_SCRIPT_PATH
    print_script_path = paths.PRINT_SCRIPT_PATH
    scheduler_script_path = paths.SCHEDULER_PATH

    try:
        # Run scrap.py script
        subprocess.run(['python', scrap_script_path], check=True)
        print("scrap.py executed successfully.")
        
        # Run print.py script after scrap.py finishes
        subprocess.run(['python', print_script_path], check=True)
        print("print.py executed successfully.")
        
        # If 'active' is True, run scheduler.py
        # if scrap_config_active:
        #    subprocess.run(['python', scheduler_script_path], check=False)
        #    print("scheduler.py executed successfully.")

    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing script: {e}")
