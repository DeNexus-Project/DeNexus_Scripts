KonBriefing Scraper:

This script scrapes data from three provided web pages and reads two CSV files to collect information about cyber attack incidents. It combines and organizes the data, then saves it in a new CSV file.

Requirements
Install the necessary libraries:

--- pip install requests beautifulsoup4 pandas ---

How It Works:

1. Scrapes Data: Extracts cyber attack details (date, title, description, references) from three web pages.
2. Loads CSV Files: Reads two local CSV files with cyber attack data.
3. Cleans and Combines Data: Merges the web data with CSV files, formats dates, and handles missing dates.
4. Generates CSV: Saves the combined data into a new CSV file with the following columns:
	
	date, title, description, references, dnx_id.



-- Running the Script --

- Install the required libraries.
- Update the CSV file paths in the script.
- Run the script, and the output will be saved as "Combined_Cleaned_Information.csv". 

A better version of this csv is showed in "../Script Analysis/KONBRIEFING/CSV/New/Styled_Combined_Information.csv".

NOTE: not every single link related to a cyberattack will work due to its longevity or the absence of a news report of it. 


