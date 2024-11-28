# Databricks notebook source
import pandas as pd

# COMMAND ----------

CURRENT_DATE = '2024-04-29'

# COMMAND ----------

import requests
from time import sleep

URLs = [
    "https://konbriefing.com/en-topics/cyber-attacks.html",
    "https://konbriefing.com/en-topics/cyber-attacks-2022.html",
    "https://konbriefing.com/en-topics/cyber-attacks-2021-2.html"
]
pages = [requests.get(URL) for URL in URLs]

# COMMAND ----------

print(pages[0].content.decode("utf-8"))

# COMMAND ----------

from bs4 import BeautifulSoup
import re

def parse_div(e):
    if e.name == 'div' and e.has_attr('class') and e['class'] == ['portfolio-item', 'kbresbox1']:
        references = []
        for a in e.find_all('a'):
            references.append({
                'title': a.text, 
                'url': a['href']
            })
        return {
            'date': e.contents[1].contents[1].text.strip(), 
            'title': e.contents[1].contents[3].text.strip(),
            'description': e.contents[3].contents[0].text.strip(),
            'references': references
        }
    else:
        raise ValueError(e)

incidents = []
for page in pages:
    soup = BeautifulSoup(page.content, "html.parser")
    entries = soup.find_all('article', class_="portfolio-item col-12")
    for article in entries:
        incidents.append(parse_div(article.div))
incidents

# COMMAND ----------

df = pd.DataFrame(incidents)
df #str.strip()

# COMMAND ----------

df.to_csv(f'/dbfs/FileStore/user_als/incidents/scrapers/kon_briefing/kon_brefing_incidents_{CURRENT_DATE}.csv', index=False)

# COMMAND ----------

# DBTITLE 1,Read CSV
df_incidents_extended = pd.read_csv(f'/dbfs/FileStore/user_als/incidents/scrapers/kon_briefing/kon_brefing_incidents_{CURRENT_DATE}.csv')
df_incidents_extended

# COMMAND ----------

df['date_uploaded'] = CURRENT_DATE
df

# COMMAND ----------

# to DL
# Pandas to Spark
df_sp = spark.createDataFrame(df)

userMetadata = f"KONBRIEFING https://konbriefing.com/en-topics/cyber-attacks.html. Updated: {CURRENT_DATE}. Documentation: https://denexus.atlassian.net/wiki/spaces/MOD/pages/440107013/Cyber+Incidents+-+version+2023 "
print(userMetadata)
df_sp.coalesce(1).write.format("delta").mode("append").option("userMetadata", userMetadata).saveAsTable("hive_metastore.temporal_tables.dkc_konbriefing_incidents")

# COMMAND ----------

# MAGIC %md
# MAGIC #ADD DNX_ID

# COMMAND ----------

df = pd.read_csv(f'/dbfs/FileStore/user_als/incidents/scrapers/kon_briefing/kon_brefing_incidents_{CURRENT_DATE}.csv')
df

# COMMAND ----------

import pandas as pd

# Obtener la longitud total del DataFrame
total_rows = len(df)

# Crear la nueva columna 'dnx_id' con los valores deseados
df['dnx_id'] = ['inc_knb_' + str(total_rows - i) for i in range(1, total_rows + 1)]

df

# COMMAND ----------

df['date_uploaded']  = CURRENT_DATE

# COMMAND ----------

# to DL
# Pandas to Spark
df_sp = spark.createDataFrame(df)

userMetadata = f"KONBRIEFING https://konbriefing.com/en-topics/cyber-attacks.html. Updated: {CURRENT_DATE}. Documentation: https://denexus.atlassian.net/wiki/spaces/MOD/pages/440107013/Cyber+Incidents+-+version+2023 "
print(userMetadata)
df_sp.coalesce(1).write.format("delta").mode("append").option("userMetadata", userMetadata).saveAsTable("hive_metastore.temporal_tables.dkc_konbriefing_incidents_id")

# COMMAND ----------



# COMMAND ----------

pd.to_datetime('September 26, 2023', format='%B %d, %Y')

# COMMAND ----------

pd.to_datetime('September 2023', format='%B %Y')

# COMMAND ----------

import time

def isTimeFormat(s, fmt):
    try:
        time.strptime(s, fmt)
        return True
    except ValueError:
        return False

def to_datetime(s):
    if isTimeFormat(s, '%B %d, %Y'):
        return pd.to_datetime(s, format='%B %d, %Y')
    elif isTimeFormat(s, '%B %Y'):
        return pd.to_datetime(s, format='%B %Y')
    elif isTimeFormat(s, '%d %B %Y'):
        return pd.to_datetime(s, format='%d %B %Y')
    elif isTimeFormat(s, '%Y'):
        return pd.to_datetime(s, format='%Y')
    else:
        None

# COMMAND ----------

df[df['date'].apply(lambda s: '?' in s)]['date'].unique()

# COMMAND ----------

df['date_processed_text'] = df['date']
df['date_processed_text'] = df['date_processed_text'].str.replace('\s?\?', '', regex=True)
df['date_processed'] = df['date_processed_text'].apply(to_datetime)
df['date_processed'].sort_values().unique()

# COMMAND ----------

pd.isna(df['date_processed']).sum()/len(df)

# COMMAND ----------

df[pd.isna(df['date_processed'])][['date', 'date_processed_text', 'date_processed']]

# COMMAND ----------

df[pd.isna(df['date_processed'])][['date', 'date_processed_text', 'date_processed']]['date_processed_text'].unique()

# COMMAND ----------

s = pd.Series([1]*len(df['date_processed']), index=df['date_processed'])
s = s.resample('Y').sum()
s.plot(kind='line', figsize=(15,6))

# COMMAND ----------

s.index.to_series().describe()

# COMMAND ----------

import re
import numpy as np

def get_country(s):
    m = re.fullmatch("(.*-)?(\.*,)+( \(.*\))?", s)
    if m:
        return m.group(1)
    else:
        return np.nan
df['country_processed'] = df['description'].str.replace('\(.*\)', '', regex=True).str.strip()
df['country_processed'].str.split(' - ')
# df['country_processed'] = df['description'].apply(get_country)

# COMMAND ----------

df.loc[df['country_processed'].isna(), ['description', 'country_processed']]

# COMMAND ----------

country = [
    ('US', 'United States'),
    ('AF', 'Afghanistan'),
    ('AL', 'Albania'),
    ('DZ', 'Algeria'),
    ('AS', 'American Samoa'),
    ('AD', 'Andorra'),
    ('AO', 'Angola'),
    ('AI', 'Anguilla'),
    ('AQ', 'Antarctica'),
    ('AG', 'Antigua And Barbuda'),
    ('AR', 'Argentina'),
    ('AM', 'Armenia'),
    ('AW', 'Aruba'),
    ('AU', 'Australia'),
    ('AT', 'Austria'),
    ('AZ', 'Azerbaijan'),
    ('BS', 'Bahamas'),
    ('BH', 'Bahrain'),
    ('BD', 'Bangladesh'),
    ('BB', 'Barbados'),
    ('BY', 'Belarus'),
    ('BE', 'Belgium'),
    ('BZ', 'Belize'),
    ('BJ', 'Benin'),
    ('BM', 'Bermuda'),
    ('BT', 'Bhutan'),
    ('BO', 'Bolivia'),
    ('BA', 'Bosnia And Herzegowina'),
    ('BW', 'Botswana'),
    ('BV', 'Bouvet Island'),
    ('BR', 'Brazil'),
    ('BN', 'Brunei Darussalam'),
    ('BG', 'Bulgaria'),
    ('BF', 'Burkina Faso'),
    ('BI', 'Burundi'),
    ('KH', 'Cambodia'),
    ('CM', 'Cameroon'),
    ('CA', 'Canada'),
    ('CV', 'Cape Verde'),
    ('KY', 'Cayman Islands'),
    ('CF', 'Central African Rep'),
    ('TD', 'Chad'),
    ('CL', 'Chile'),
    ('CN', 'China'),
    ('CX', 'Christmas Island'),
    ('CC', 'Cocos Islands'),
    ('CO', 'Colombia'),
    ('KM', 'Comoros'),
    ('CG', 'Congo'),
    ('CK', 'Cook Islands'),
    ('CR', 'Costa Rica'),
    ('CI', 'Cote D`ivoire'),
    ('HR', 'Croatia'),
    ('CU', 'Cuba'),
    ('CY', 'Cyprus'),
    ('CZ', 'Czech Republic'),
    ('DK', 'Denmark'),
    ('DJ', 'Djibouti'),
    ('DM', 'Dominica'),
    ('DO', 'Dominican Republic'),
    ('TP', 'East Timor'),
    ('EC', 'Ecuador'),
    ('EG', 'Egypt'),
    ('SV', 'El Salvador'),
    ('GQ', 'Equatorial Guinea'),
    ('ER', 'Eritrea'),
    ('EE', 'Estonia'),
    ('ET', 'Ethiopia'),
    ('FK', 'Falkland Islands (Malvinas)'),
    ('FO', 'Faroe Islands'),
    ('FJ', 'Fiji'),
    ('FI', 'Finland'),
    ('FR', 'France'),
    ('GF', 'French Guiana'),
    ('PF', 'French Polynesia'),
    ('TF', 'French S. Territories'),
    ('GA', 'Gabon'),
    ('GM', 'Gambia'),
    ('GE', 'Georgia'),
    ('DE', 'Germany'),
    ('GH', 'Ghana'),
    ('GI', 'Gibraltar'),
    ('GR', 'Greece'),
    ('GL', 'Greenland'),
    ('GD', 'Grenada'),
    ('GP', 'Guadeloupe'),
    ('GU', 'Guam'),
    ('GT', 'Guatemala'),
    ('GN', 'Guinea'),
    ('GW', 'Guinea-bissau'),
    ('GY', 'Guyana'),
    ('HT', 'Haiti'),
    ('HN', 'Honduras'),
    ('HK', 'Hong Kong'),
    ('HU', 'Hungary'),
    ('IS', 'Iceland'),
    ('IN', 'India'),
    ('ID', 'Indonesia'),
    ('IR', 'Iran'),
    ('IQ', 'Iraq'),
    ('IE', 'Ireland'),
    ('IL', 'Israel'),
    ('IT', 'Italy'),
    ('JM', 'Jamaica'),
    ('JP', 'Japan'),
    ('JO', 'Jordan'),
    ('KZ', 'Kazakhstan'),
    ('KE', 'Kenya'),
    ('KI', 'Kiribati'),
    ('KP', 'Korea (North)'),
    ('KR', 'Korea (South)'),
    ('KW', 'Kuwait'),
    ('KG', 'Kyrgyzstan'),
    ('LA', 'Laos'),
    ('LV', 'Latvia'),
    ('LB', 'Lebanon'),
    ('LS', 'Lesotho'),
    ('LR', 'Liberia'),
    ('LY', 'Libya'),
    ('LI', 'Liechtenstein'),
    ('LT', 'Lithuania'),
    ('LU', 'Luxembourg'),
    ('MO', 'Macau'),
    ('MK', 'Macedonia'),
    ('MG', 'Madagascar'),
    ('MW', 'Malawi'),
    ('MY', 'Malaysia'),
    ('MV', 'Maldives'),
    ('ML', 'Mali'),
    ('MT', 'Malta'),
    ('MH', 'Marshall Islands'),
    ('MQ', 'Martinique'),
    ('MR', 'Mauritania'),
    ('MU', 'Mauritius'),
    ('YT', 'Mayotte'),
    ('MX', 'Mexico'),
    ('FM', 'Micronesia'),
    ('MD', 'Moldova'),
    ('MC', 'Monaco'),
    ('MN', 'Mongolia'),
    ('MS', 'Montserrat'),
    ('MA', 'Morocco'),
    ('MZ', 'Mozambique'),
    ('MM', 'Myanmar'),
    ('NA', 'Namibia'),
    ('NR', 'Nauru'),
    ('NP', 'Nepal'),
    ('NL', 'Netherlands'),
    ('AN', 'Netherlands Antilles'),
    ('NC', 'New Caledonia'),
    ('NZ', 'New Zealand'),
    ('NI', 'Nicaragua'),
    ('NE', 'Niger'),
    ('NG', 'Nigeria'),
    ('NU', 'Niue'),
    ('NF', 'Norfolk Island'),
    ('MP', 'Northern Mariana Islands'),
    ('NO', 'Norway'),
    ('OM', 'Oman'),
    ('PK', 'Pakistan'),
    ('PW', 'Palau'),
    ('PA', 'Panama'),
    ('PG', 'Papua New Guinea'),
    ('PY', 'Paraguay'),
    ('PE', 'Peru'),
    ('PH', 'Philippines'),
    ('PN', 'Pitcairn'),
    ('PL', 'Poland'),
    ('PT', 'Portugal'),
    ('PR', 'Puerto Rico'),
    ('QA', 'Qatar'),
    ('RE', 'Reunion'),
    ('RO', 'Romania'),
    ('RU', 'Russian Federation'),
    ('RW', 'Rwanda'),
    ('KN', 'Saint Kitts And Nevis'),
    ('LC', 'Saint Lucia'),
    ('VC', 'St Vincent/Grenadines'),
    ('WS', 'Samoa'),
    ('SM', 'San Marino'),
    ('ST', 'Sao Tome'),
    ('SA', 'Saudi Arabia'),
    ('SN', 'Senegal'),
    ('SC', 'Seychelles'),
    ('SL', 'Sierra Leone'),
    ('SG', 'Singapore'),
    ('SK', 'Slovakia'),
    ('SI', 'Slovenia'),
    ('SB', 'Solomon Islands'),
    ('SO', 'Somalia'),
    ('ZA', 'South Africa'),
    ('ES', 'Spain'),
    ('LK', 'Sri Lanka'),
    ('SH', 'St. Helena'),
    ('PM', 'St.Pierre'),
    ('SD', 'Sudan'),
    ('SR', 'Suriname'),
    ('SZ', 'Swaziland'),
    ('SE', 'Sweden'),
    ('CH', 'Switzerland'),
    ('SY', 'Syrian Arab Republic'),
    ('TW', 'Taiwan'),
    ('TJ', 'Tajikistan'),
    ('TZ', 'Tanzania'),
    ('TH', 'Thailand'),
    ('TG', 'Togo'),
    ('TK', 'Tokelau'),
    ('TO', 'Tonga'),
    ('TT', 'Trinidad And Tobago'),
    ('TN', 'Tunisia'),
    ('TR', 'Turkey'),
    ('TM', 'Turkmenistan'),
    ('TV', 'Tuvalu'),
    ('UG', 'Uganda'),
    ('UA', 'Ukraine'),
    ('AE', 'United Arab Emirates'),
    ('UK', 'United Kingdom'),
    ('UY', 'Uruguay'),
    ('UZ', 'Uzbekistan'),
    ('VU', 'Vanuatu'),
    ('VA', 'Vatican City State'),
    ('VE', 'Venezuela'),
    ('VN', 'Viet Nam'),
    ('VG', 'Virgin Islands (British)'),
    ('VI', 'Virgin Islands (U.S.)'),
    ('EH', 'Western Sahara'),
    ('YE', 'Yemen'),
    ('YU', 'Yugoslavia'),
    ('ZR', 'Zaire'),
    ('ZM', 'Zambia'),
    ('ZW', 'Zimbabwe')
]
countries = pd.DataFrame(country)[1]

# COMMAND ----------

df['country_processed'] = 'Unknown'
df['description_preprocessed'] = df['description']
df['description_preprocessed'] = df['description_preprocessed'].str.replace('USA', 'United States')
for c in countries:
    df.loc[df['description_preprocessed'].str.contains(c, regex=False, na=False), 'country_processed'] = c

# COMMAND ----------

df[['description_preprocessed', 'country_processed']].tail()

# COMMAND ----------

s = df[['description_preprocessed', 'country_processed']].iloc[3699]['description_preprocessed']
pd.Series(s).str.contains('United States')

# COMMAND ----------

df['description_preprocessed'].str.contains('United States', regex=False, na=False).sum()

# COMMAND ----------

s = df['country_processed'].value_counts().rename('count').rename_axis(index='country')
dfaux = s.to_frame().reset_index()
dfaux = dfaux.groupby(by=pd.cut(dfaux['count'], np.arange(0, s.max()+s.max()//10, s.max()//10)))['country'].apply(list).to_frame()
dfaux['count'] = dfaux['country'].apply(len)
dfaux
