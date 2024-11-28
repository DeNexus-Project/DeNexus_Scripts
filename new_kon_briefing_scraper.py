import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# Configuration: current date for the upload date
CURRENT_DATE = '2024-04-29'  # Used for upload tracking
URLs = [
    "https://konbriefing.com/en-topics/cyber-attacks.html",
    "https://konbriefing.com/en-topics/cyber-attacks-2022.html",
    "https://konbriefing.com/en-topics/cyber-attacks-2021-2.html"
]

# Function to extract data from a web page
def extract_data_from_page(url):
    """
    Extracts cyber attack incidents from a given URL.
    """
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to access {url}, status code: {response.status_code}")
        return []

    soup = BeautifulSoup(response.content, "html.parser")
    incidents = []

    # Find articles containing incidents
    entries = soup.find_all('article', class_="portfolio-item col-12")
    for article in entries:
        div = article.div
        if div and div.has_attr('class') and 'portfolio-item' in div['class']:
            # Extract references
            references = [{'title': a.text.strip(), 'url': a['href']} for a in div.find_all('a')]

            # Extract date, title, description, and references
            try:
                date = div.contents[1].contents[1].text.strip()  # Extract date
                incident = {
                    'date': date,  # Use the actual date from the HTML
                    'title': div.contents[1].contents[3].text.strip(),  # Extract title
                    'description': div.contents[3].contents[0].text.strip(),  # Extract description
                    'references': "; ".join([f"{ref['title']} ({ref['url']})" for ref in references])  # Extract references
                }
                incidents.append(incident)
            except (IndexError, AttributeError):
                print("Error processing an incident. Data is incomplete.")

    return incidents


# Consolidate data from all pages
all_incidents = []
for url in URLs:
    all_incidents.extend(extract_data_from_page(url))

# Create a DataFrame from the scraped data
df_scraped = pd.DataFrame(all_incidents)

# Read the two provided CSV files
file1_path = r"C:/Users/Juans/Downloads/Uni/2AÑO/6. S.O/kon_briefing_incidents_2024-04-29.csv"
file2_path = r"C:/Users/Juans/Downloads/Uni/2AÑO/6. S.O/kon_briefing_incidents_2024-04-22.csv"

df1 = pd.read_csv(file1_path)
df2 = pd.read_csv(file2_path)

# Combine the three DataFrames (two CSV files and the scraped data)
combined_df = pd.concat([df1, df2, df_scraped], ignore_index=True)

# Ensure all columns are consistent (adding missing columns if necessary)
all_columns = set(df1.columns).union(set(df2.columns))
for col in all_columns:
    if col not in combined_df.columns:
        combined_df[col] = ""

# Process the 'date' column to ensure proper formatting for Excel
if 'date' in combined_df.columns:
    # First, convert the 'date' column to datetime, coercing invalid dates to NaT
    combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce')

    # If date contains a full date, preserve it; otherwise, keep only the month and year
    def clean_date_format(date):
        if pd.isna(date):
            return "No date specified"
        # Check if the date contains the day, if not, just set the month and year
        if isinstance(date, str) and len(date.split()) == 2:  # Only month and year
            return date + " 01"  # Add a default day to match the format
        return date.strftime('%Y-%m-%d')  # Full date (day, month, year)

    combined_df['date'] = combined_df['date'].apply(clean_date_format)

# If date is not available from CSV or scraping, leave it as "No date specified"
combined_df['date'] = combined_df['date'].fillna('No date specified')

# Generate a unique identifier for each row
combined_df['dnx_id'] = ['inc_knb_' + str(i + 1) for i in range(len(combined_df))]

# Sort the DataFrame by date (if the date is valid, otherwise it will stay blank)
combined_df = combined_df.sort_values(by='date', na_position='last').reset_index(drop=True)

# Save the cleaned and combined data to a new CSV in the current project folder
output_file = 'Combined_Cleaned_Information.csv'
combined_df.to_csv(output_file, index=False)

# Display a preview of the combined DataFrame
print(f"Data successfully saved to {output_file}\n")
print("Preview of the combined data:")
print(combined_df.head(10))
