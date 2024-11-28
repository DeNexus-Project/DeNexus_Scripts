from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import os
import time
from datetime import datetime
import json

# Load configuration from JSON file
config_path = os.path.join("config", "config.json")

with open(config_path, 'r') as json_file:
    config = json.load(json_file)

# Access settings and data from JSON
history_enabled = config["settings"].get("history", False)
update_enabled = config["settings"].get("update", False)
url = config["data"].get("url", "")
save_path = config["data"].get("save_path", "")
html_target = config["data"].get("html_target", "")

# Ensure the download directory exists
if not os.path.exists(save_path):
    os.makedirs(save_path)

# Set Chrome options for downloading
chrome_options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": save_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)

# Initialize WebDriver
driver = webdriver.Chrome(options=chrome_options)

# Open the URL from the config file
driver.get(url)

time.sleep(5)

# Locate and click the CSV button
csv_button = driver.find_element(By.CLASS_NAME, html_target)
ActionChains(driver).move_to_element(csv_button).perform()

csv_button.click()

time.sleep(10)

# Generate filename with current date and time
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M")
target_filename = f"incidents_{current_time}.csv"

default_filename = os.path.join(save_path, "incidents.csv")
final_filename = os.path.join(save_path, target_filename)

# Skip renaming if update is enabled
if update_enabled:
    print("Update is enabled, skipping renaming of the file.")
else:
    if os.path.exists(default_filename):
        os.rename(default_filename, final_filename)
        print(f"File renamed to: {final_filename}")

# Close the browser
driver.quit()
