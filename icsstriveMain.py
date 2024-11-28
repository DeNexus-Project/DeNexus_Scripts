import requests
from bs4 import BeautifulSoup
import csv
import time

class ICSStriveScraper:
    def __init__(self):
        self.base_url = "https://icsstrive.com/?wpv_aux_current_post_id=153&wpv_aux_parent_post_id=153&wpv_view_count=9385&wpv_paged={page_number}"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
            "Content-Type": "text/html",
            "Referer": "https://www.google.com/",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate"
        }
        self.pages_to_scrape = 67
        self.incidents = []

    def fetch_page(self, url, retries=3):
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {url}: {e}. Retrying ({attempt+1}/{retries})...")
                time.sleep(5)
        raise Exception(f"Failed to fetch {url} after {retries} attempts.")

    def scrape_pages(self):
        for page_number in range(1, self.pages_to_scrape + 1):
            url = self.base_url.format(page_number=page_number)
            print(f"Scraping page: {url}")
            response = self.fetch_page(url)
            self.parse_page(response.content)

    def parse_page(self, content):
        soup = BeautifulSoup(content, "html.parser")
        for incident_div in soup.find_all("div", {"class": "incident-outer"}):
            try:
                title_div = incident_div.find("div", {"class": "search-r-title"})
                title_link = title_div.find("a")["href"] if title_div else None
                print(f"Extracted link: {title_link}")  # Debug print
                incident = {
                    "title": self.extract_field(incident_div, "search-r-title"),
                    "date": self.extract_field(incident_div, "search-r-date"),
                    "victim": self.extract_field_list(incident_div, "search-r-victim"),
                    "malware_names": self.extract_field_list(incident_div, "search-r-malware"),
                    "threat": self.extract_field_list(incident_div, "search-r-threat"),
                    "impacts": self.extract_impacts(incident_div),
                    "location": self.extract_location(incident_div),
                    "data_source_link_url": title_link
                }
                self.incidents.append(incident)
            except Exception as e:
                print(f"Error parsing incident: {e}")

    def extract_field(self, element, class_name):
        field = element.find("div", {"class": class_name})
        return field.text.strip() if field else None

    def extract_field_list(self, element, class_name):
        container = element.find("div", {"class": class_name})
        if container:
            return [li.text.strip() for li in container.find_all("li")]
        return []

    def extract_impacts(self, element):
        container = element.find("div", {"class": "search-r-impacts"})
        if container:
            return [line.strip() for line in container.stripped_strings]
        return []

    def extract_location(self, element):
        ul = element.find("ul", {"class": "search-r-country"})
        if ul:
            return [li.text.strip() for li in ul.find_all("li")]
        return []

    def save_to_csv(self, filename="incidents.csv"):
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["title", "date", "victim", "malware_names", "threat", "impacts", "location", "data_source_link_url"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for incident in self.incidents:
                writer.writerow({
                    "title": incident["title"],
                    "date": incident["date"],
                    "victim": ", ".join(incident["victim"]),
                    "malware_names": ", ".join(incident["malware_names"]),
                    "threat": ", ".join(incident["threat"]),
                    "impacts": ", ".join(incident["impacts"]),
                    "location": ", ".join(incident["location"]),
                    "data_source_link_url": incident["data_source_link_url"]
                })
        print(f"Data saved to {filename}")

if __name__ == "__main__":
    scraper = ICSStriveScraper()
    scraper.scrape_pages()
    scraper.save_to_csv()