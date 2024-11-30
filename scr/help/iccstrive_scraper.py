# Databricks notebook source
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Load Index of Incidents
import requests
from time import sleep

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.41',
    'Content-Type': 'text/html',
    'Referer': 'https://www.google.com/',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Accept-Encoding': 'gzip, deflate'
}


URL_pattern = "https://icsstrive.com/?wpv_aux_current_post_id=153&wpv_aux_parent_post_id=153&wpv_view_count=9385&wpv_paged={page_number}"

pages = {}
#There are 54 pages
for page_number in range(1,54+1):
    print(page_number)
    sleep(20)
    # TODO: make sleep intervals random uniformly between [20, 100]
    pages[page_number] = requests.get(URL_pattern.format(page_number=page_number, headers=headers))

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: We need to review the page range to make it shorter!

# COMMAND ----------

# CURRENT_DATE = '2024-07-04'

# COMMAND ----------

# DBTITLE 1,Persist Index Pages as JSON
import json 

pages_json = {}
for page_number, page in pages.items():
    pages_json[str(page_number)] = page.content.decode("utf-8")

# with open(f'/dbfs/FileStore/user_als/incidents/scrapers/iccstrive/icsstrive_index_webpages_content_html_{CURRENT_DATE}.json', 'w') as f:
#     json.dump(pages_json, f, indent=4)

# COMMAND ----------

# DBTITLE 1,Scrap Index Pages
from bs4 import BeautifulSoup

def parse_optional_list(e, fname):
    l = e.find_all('div', {'class': fname})
    if len(l) != 1:
        raise ValueError(f"Field '{fname}' couldn't be parsed.")
    else:
        rl = []
        uls = l[0].find_all('ul', {'class': 'wpv-loop js-wpv-loop'})
        nf = l[0].find_all('div', {'class': 'not-found'})
        if len(uls) == 1:
            ul = uls[0]
            values = []
            for li in ul.children:
                values.append(li.a.text)
            return values
        elif len(nf) == 1:
            return nf[0].text
        else:
            raise ValueError(f"Field '{fname}' couldn't be parsed.")
    
def parse_simple_div_field(e, fname):
    l = e.find_all('div', {'class': fname})
    if len(l) == 1:
        if l[0].select_one('a'):
            return l[0].a.text
        else:
            return l[0].text
    else:
        raise ValueError(f"Field '{fname}' couldn't be parsed.")

def parse_countries(e):
    l = e.find_all('ul', {'class': 'search-r-country'})
    if len(l) == 1:
        ul = l[0]
        countries = []
        for li in ul.children:
            countries.append(li.a.text)
        return countries
    else:
        raise ValueError(f"Field 'search-r-country' couldn't be parsed.")

def parse_title_field(e):
    fname = 'search-r-title'
    l = e.find_all('div', {'class': fname})
    if len(l) == 1:
        return l[0].a.text, l[0].a['href']
    else:
        raise ValueError(f"Field '{fname}' couldn't be parsed.")

incidents = []
for page_number, page in pages.items():
    print(page_number)
    soup = BeautifulSoup(page.content, "html.parser")
    for e in soup.find_all('div', {'class': 'incident-outer'}):
        incident = {}
        title = parse_title_field(e)
        incident['title'] = title[0]
        incident['title_link'] = title[1]
        incident['date'] = parse_simple_div_field(e, 'search-r-date')
        incident['victim'] = parse_optional_list(e, 'search-r-victim')
        incident['malware_names'] = parse_optional_list(e, 'search-r-malware')
        incident['threat'] = parse_optional_list(e, 'search-r-threat')
        incident['impacts'] = parse_simple_div_field(e, 'search-r-impacts')
        incident['countries'] = parse_countries(e)
        incidents.append(incident)

import pandas as pd
df_index = pd.DataFrame(incidents)
df_index

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,To CSV
# df_index.to_csv(f'/dbfs/FileStore/user_als/incidents/scrapers/iccstrive/icsstrive_index_incidents_{CURRENT_DATE}.csv', index=False)

# COMMAND ----------

links = pd.DataFrame(incidents)['title_link'].tolist()
links

# COMMAND ----------

len(links)

# COMMAND ----------

# DBTITLE 1,Load Incident Web Pages
import requests
from time import sleep
import random

# This is necessary because we need to fake a real browser session
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.41',
    'Content-Type': 'text/html',
}

incident_pages = []
for i,url in enumerate(links):
    print((i,url))
    sleep(random.randint(1, 60))
    incident_pages.append({
        'url': url,
        'page': requests.get(url, headers=headers),
    })

# COMMAND ----------

len(incident_pages)

# COMMAND ----------

# DBTITLE 1,Persist Incident Pages as JSON
import json 

#to text
pages_json = []
for d in incident_pages:
    pages_json.append({
        'url': d['url'],
        'page': d['page'].content.decode("utf-8"),
        
     })

# with open(f'/dbfs/FileStore/user_als/incidents/scrapers/iccstrive/icsstrive_incident_webpages_content_html_{CURRENT_DATE}.json', 'w') as f:
#     json.dump(pages_json, f, indent=4)

# COMMAND ----------

# DBTITLE 1,Scrap Incident Web Pages
from bs4 import BeautifulSoup

description_div_class = "et_pb_module et_pb_text et_pb_text_1_tb_body et_pb_text_align_left et_pb_bg_layout_light"
date_div_class = "et_pb_module et_pb_text et_pb_text_2_tb_body et_pb_text_align_left et_pb_bg_layout_light"
location_div_class = "et_pb_module et_pb_text et_pb_text_3_tb_body et_pb_text_align_left et_pb_bg_layout_light"
estimated_cost_div_class = "et_pb_module et_pb_text et_pb_text_4_tb_body et_pb_text_align_left et_pb_bg_layout_light"
victims_div_class = "et_pb_module et_pb_text et_pb_text_5_tb_body et_pb_text_align_left et_pb_bg_layout_light"
type_of_malware_div_class = "et_pb_module et_pb_text et_pb_text_6_tb_body et_pb_text_align_left et_pb_bg_layout_light"
threat_source_div_class = "et_pb_module et_pb_text et_pb_text_7_tb_body et_pb_text_align_left et_pb_bg_layout_light"
industries_div_class = "et_pb_module et_pb_text et_pb_text_9_tb_body et_pb_text_align_left et_pb_bg_layout_light"
impacts_div_class = "et_pb_module et_pb_text et_pb_text_10_tb_body et_pb_text_align_left et_pb_bg_layout_light"
references_div_class = "et_pb_module et_pb_text et_pb_text_8_tb_body et_pb_text_align_left et_pb_bg_layout_light"

def parse_description(soup, class_=description_div_class):
    l = soup.find_all('div', class_=class_)
    if len(l) != 1:
        raise ValueError(f"Field description couldn't be parsed.")
    else:
        div = l[0]
        rl = []
        for c in div.div.children:
            if c.name == 'p':
                rl.append(c.text)
            elif c.name == 'h3':
                pass
            else:
                raise ValueError(f"Field description couldn't be parsed.")
        return "\n\n".join(rl)
    
def parse_locations(soup, class_=location_div_class):
    l = soup.find_all('div', class_=class_)
    if len(l) != 1:
        raise ValueError(f"Field location couldn't be parsed.")
    else:
        div = l[0]
        rl = []
        for a in div.div.p.children:
            if a.name == 'a':
                rl.append(a.text)
            else:
                ValueError(f"Field location couldn't be parsed.")
        return rl
    
def parse_victims(soup, class_=victims_div_class):
    l = soup.find_all('div', class_=class_)
    if len(l) != 1:
        raise ValueError(f"Field victims couldn't be parsed.")
    else:
        div = l[0]
        if div.div.ul:
            rl = []
            for li in div.div.ul.children:
                if li.name == 'li':
                    rl.append({'title': li.a.text, 'link_url':li.a['href']})
                else:
                    raise ValueError(f"Field victims couldn't be parsed.")
            return rl
        elif div.div.div.div and ('not-found' in div.div.div.div['class']):
            return div.div.div.div.text
        else:
            raise ValueError(f"Field victims couldn't be parsed.")

incidents_extended = []
for i, d in enumerate(incident_pages):
    link_url = d['url']
    page = d['page']
    print((i, link_url))
    soup = BeautifulSoup(page.content, "html.parser")
    incident = {
        'data_source_link_url': link_url,
        'description': parse_description(soup),
        'date': parse_description(soup, class_=date_div_class),
        'locations': parse_locations(soup),
        'estimated_cost': parse_description(soup, class_=estimated_cost_div_class),
        'victims': parse_victims(soup),
        'type_of_malware': parse_victims(soup, class_=type_of_malware_div_class),
        'threat_source': parse_victims(soup, class_=threat_source_div_class),
        'industries': parse_locations(soup, class_=industries_div_class),
        'impacts': parse_description(soup, class_=impacts_div_class),
        'references': parse_victims(soup, class_=references_div_class),
    }
    incidents_extended.append(incident)

incidents_extended

# COMMAND ----------

df_incidents_extended = pd.DataFrame(incidents_extended)
df_incidents_extended

# COMMAND ----------

# incidents_extended=[{'data_source_link_url': 'https://icsstrive.com/incident/operational-impact-at-electronics-company-alps-alpine-group/',
#   'description': 'ALPS\' North American production operations and delivery was impacted by a ransomware incident on their systems.   ALP promptly shut off the network connection of servers and other devices infected and reported they "are still working to restore equipment and production functions. At present, with the exception of our production bases in Mexico, we have resumed production and delivery with alternative methods for system failures."\n\nNorth American employee data was reportedly leaked.\n\nThis follows on the heels of a separate attack on July 6, 2023, where an attack exfiltrated data on 16,000 employees.',
#   'date': 'September 12, 2024',
#   'locations': ['Japan', 'United States'],
#   'estimated_cost': ' Partially impacted North American production and shipping for 2+ days,  employee data leaked',
#   'victims': [{'title': 'Alps Alpine Group',
#     'link_url': 'https://icsstrive.com/victim/alps-alpine-group/'}],
#   'type_of_malware': 'No Malware identified',
#   'threat_source': [{'title': 'BlackByte',
#     'link_url': 'https://icsstrive.com/threat-actor/blackbyte/'}],
#   'industries': ['Automotive', 'Manufacturing'],
#   'impacts': ' OT IT',
#   'references': [{'title': 'Cyber Incident Victim: Alps Alpine Group',
#     'link_url': 'https://www.csidb.net/csidb/incidents/e7098fd4-9a15-4754-8591-f7d635b30225/'}]}]

# COMMAND ----------

# DBTITLE 1,To CSV

# df_incidents_extended.to_csv(f'/dbfs/FileStore/user_als/incidents/scrapers/iccstrive/icsstrive_extended_incidents_{CURRENT_DATE}.csv', index=False)
# with open(f'/dbfs/FileStore/user_als/incidents/scrapers/iccstrive/icsstrive_extended_incidents_{CURRENT_DATE}.json', "w") as f:
#     json.dump(incidents_extended, f)
#     print("File saved correctly")
    


# COMMAND ----------

# with open(f'/dbfs/FileStore/user_als/incidents/scrapers/iccstrive/icsstrive_extended_incidents_{CURRENT_DATE}.json', "r") as f:
#     etda = json.load(f)

# print(etda)

# COMMAND ----------

# DBTITLE 1,Read CSV
# df_incidents_extended = pd.read_csv(f'/dbfs/FileStore/user_als/incidents/scrapers/iccstrive/icsstrive_extended_incidents_{CURRENT_DATE}.csv')
# df_incidents_extended

# COMMAND ----------

df_incidents_extended['date_uploaded'] = CURRENT_DATE
df_incidents_extended

# COMMAND ----------

# to DL
# Pandas to Spark
df_sp = spark.createDataFrame(df_incidents_extended)

userMetadata = f"ICSSTRIVE https://icsstrive.com/. Update:{CURRENT_DATE}. Incident details. Documentation: https://denexus.atlassian.net/wiki/spaces/MOD/pages/440107013/Cyber+Incidents+-+version+2023"
print(userMetadata)
df_sp.coalesce(1).write.format("delta").mode("append").option("userMetadata", userMetadata).saveAsTable("hive_metastore.temporal_tables.dkc_icsstrive_incident_details")

# COMMAND ----------

# DBTITLE 1,To Datalake
# # to DL
# # Pandas to Spark
# df_sp = spark.createDataFrame(df_incidents_extended)

# userMetadata = f"ICSSTRIVE https://icsstrive.com/. Update:2023-11-23. Incident details. Documentation: https://denexus.atlassian.net/wiki/spaces/MOD/pages/440107013/Cyber+Incidents+-+version+2023"
# print(userMetadata)
# df_sp.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema","true").option("userMetadata", userMetadata).saveAsTable("hive_metastore.temporal_tables.dkc_icsstrive_incident_details")

# COMMAND ----------

df= spark.read.table("hive_metastore.temporal_tables.dkc_icsstrive_incident_details").toPandas()

# COMMAND ----------

df.columns

# COMMAND ----------

df.groupby('date_uploaded').size()

# COMMAND ----------


