# Description: Script that automatically presses the download button on the EuroRepo website.
#               It opens the website without altering any information on the table, but filters can also be
#               applied to the table automatically. The script then downloads the table as an Excel file and
#               converts it to a CSV file.
# Author: Leonardo Valdés Esparza
# Notes: Certain parts of the code must be modified depending on where it is run.
#         Namely: download_dir and service.
#        Additionally, chromedriver must be installed.

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    import time
    import os

    import pandas as pd

    # REPLACE
    download_dir = '/Users/scrambledmacbook/PycharmProjects/WebScraping/Result'
    url = 'https://database.eurepoc-dashboard.eu/'
    # REPLACE
    service = Service('/Users/scrambledmacbook/PycharmProjects/WebScraping/chromedriver-mac-x64/chromedriver')
    resultSize = len(os.listdir(download_dir))
    chrome_options = Options()

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }

    chrome_options.add_experimental_option('prefs', prefs)

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)
    print("got url")
    time.sleep(5)

    # Wait for the download button to be clickable and scroll to it
    downloadButton = WebDriverWait(driver, 20).until(
        expected_conditions.element_to_be_clickable((By.XPATH, '//*[@id="download-button"]'))
    )
    print("found download button")
    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", downloadButton)
    print("scrolled to download button")

    # Wait for the download button to be visible and click it.
    time.sleep(3)
    downloadButton.click()
    print("clicked download button")

    # Wait for the file to download
    time.sleep(20)
    print('File is downloaded')

    # find Excel file in Result
    for file in os.listdir(download_dir):
        if file.endswith('.xlsx'):
            excel_file = file
            break

    # Read Excel file
    df = pd.read_excel(os.path.join(download_dir, excel_file))

    # Convert to CSV
    csv_file = excel_file.replace('.xlsx', '.csv')
    df.to_csv(os.path.join(download_dir, csv_file), index=False)

    print(f'Converted {excel_file} to {csv_file}')
except Exception as e:
    print("Error: ", e)

finally:
    driver.quit()
