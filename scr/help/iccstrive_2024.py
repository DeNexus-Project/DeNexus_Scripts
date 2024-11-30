# Databricks notebook source
import pandas as pd
import json
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import ast

# COMMAND ----------

root_path = "."

# COMMAND ----------

denexus_colors = ['navy', 'royalblue', '#FAC205', 'darkorange', 'lightskyblue', 'mediumturquoise', 'lightgrey','dimgray', 'black']

# COMMAND ----------

CURRENT_DATE = '2024-08-19'

# COMMAND ----------

# MAGIC %md
# MAGIC #READ CSV

# COMMAND ----------

# DBTITLE 1,Read CSV
df_incidents_extended = pd.read_csv(f'/dbfs/FileStore/user_ol/incidents/csvs_extended/icsstrive/icsstrive_extended_incidents_{CURRENT_DATE}.csv')
df_incidents_extended

# COMMAND ----------

df_incidents_extended['date_clean'] = pd.to_datetime(df_incidents_extended['date'])
df_incidents_extended[['date', 'date_clean']]

# COMMAND ----------

df_incidents_2024 = df_incidents_extended[df_incidents_extended['date_clean'] >= '2024']
df_incidents_2024 = df_incidents_2024.reset_index(drop = True)
df_incidents_2024.head(50)

# COMMAND ----------

# MAGIC %md
# MAGIC #ANALYSIS

# COMMAND ----------

# MAGIC %md
# MAGIC ##MONTHS

# COMMAND ----------

# import calendar
# df_incidents_2024.loc[:,'month'] = df_incidents_2024['date_clean'].dt.month.apply(lambda x: calendar.month_name[x]) asi se guardan los nombres, pero no se mantiene el orden
df_incidents_2024.loc[:,'month'] = df_incidents_2024['date_clean'].dt.month
df_incidents_2024

# COMMAND ----------

df_incidents_2024 = df_incidents_2024[df_incidents_2024['month'] != 8]
df_incidents_2024['month'].value_counts()

# COMMAND ----------

incidents_per_month = df_incidents_2024.groupby('month').size().reset_index(name = 'No. of incidents')
incidents_per_month.rename(columns={'month': 'Month'}, inplace = True)
incidents_per_month

# COMMAND ----------

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July']
incidents_per_month_names = incidents_per_month.copy()
incidents_per_month_names['Month'] = months
incidents_per_month_names = incidents_per_month_names[:-1]
incidents_per_month_names

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT MONTHS

# COMMAND ----------

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July']

plt.figure(figsize = (6, 3))
plt.bar(incidents_per_month_names['Month'], incidents_per_month_names['No. of incidents'], edgecolor = 'royalblue' , color = 'lightskyblue', width = 0.4)
plt.plot(incidents_per_month_names['Month'], incidents_per_month_names['No. of incidents'], marker='o', color='darkblue', linewidth=2, markersize=6, linestyle='-')
plt.xlabel('Month')
plt.ylabel('Number of incidents')
plt.title('Number of incidents per month')
# plt.xticks(np.arange(1,8), months)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##COUNTRIES

# COMMAND ----------

# is_list = df_incidents_2024['locations'].apply(lambda x: isinstance(x, list))
df_incidents_2024['locations'] = df_incidents_2024['locations'].apply(ast.literal_eval)

df_exploded_locations = df_incidents_2024.explode('locations')
df_exploded_locations['locations'] = df_exploded_locations['locations'].str.replace('Hong Kong', 'China')
df_exploded_locations

# COMMAND ----------

incidents_per_country = df_exploded_locations['locations'].value_counts().reset_index(name = 'No. of incidents')
incidents_per_country.rename(columns={'index': 'Location'}, inplace = True)
incidents_per_country

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT COUNTRIES

# COMMAND ----------

plt.figure(figsize = (10, 7))
plt.bar(incidents_per_country['Location'], incidents_per_country['No. of incidents'], edgecolor = 'navy', color = 'royalblue', width = 0.6)
plt.xlabel('Location')
plt.ylabel('Number of incidents')
plt.title('Number of incidents per location')
plt.xticks(rotation = 90)
plt.show()


# COMMAND ----------

# Countries in Europe
europe = [
    "Albania", "Andorra", "Armenia", "Austria", "Azerbaijan", "Belarus", "Belgium",
    "Bosnia and Herzegovina", "Bulgaria", "Cyprus", "Croatia", "Denmark", "Slovakia",
    "Slovenia", "Spain", "Estonia", "Finland", "France", "Georgia", "Germany", "Greece",
    "Hungary", "Ireland", "Iceland", "Italy", "Kazakhstan", "Kosovo", "Latvia",
    "Liechtenstein", "Lithuania", "Luxembourg", "Macedonia (Rep. of N.)", "Malta", "Moldova", "Monaco", "Montenegro",
    "Norway", "Netherlands", "Poland", "Portugal", "Romania", "Russia", "San Marino",
    "Serbia", "Sweden", "Switzerland", "Turkey", "Ukraine", "United Kingdom", "Vatican City"
]
# Countries in North America
north_america = [
    "Canada", "United States", "Mexico"
]
# Countries in South America
south_america = [
    "Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Ecuador", "Guyana",
    "Paraguay", "Peru", "Suriname", "Uruguay", "Venezuela", "Belize", "Costa Rica",
    "El Salvador", "Guatemala", "Honduras", "Nicaragua", "Panama"
]
# Countries in Asia
asia = [
    "Afghanistan", "Saudi Arabia", "Armenia", "Azerbaijan", "Bahrain", "Bangladesh", "Myanmar",
    "Brunei", "Bhutan", "Cambodia", "China", "Cyprus", "North Korea", "South Korea",
    "United Arab Emirates", "Georgia", "India", "Indonesia", "Iraq", "Iran", "Israel",
    "Japan", "Jordan", "Kazakhstan", "Kuwait", "Laos", "Lebanon", "Malaysia", "Maldives",
    "Mongolia", "Nepal", "Oman", "Pakistan", "Palestine", "Philippines", "Qatar", "Russia", "Saudi Arabia",
    "Singapore", "Syria", "Sri Lanka", "Taiwan", "Tajikistan", "Thailand", "Timor-Leste", "Turkmenistan",
    "Uzbekistan", "Vietnam", "Yemen"
]
# Countries in Africa
africa = [
    "South Africa", "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi",
    "Cape Verde", "Cameroon", "Central African Republic", "Chad", "Comoros", "Congo (Brazzaville)",
    "Congo (Kinshasa)", "Ivory Coast", "Djibouti", "Egypt", "Eritrea",
    "Eswatini", "Ethiopia", "Gabon", "Gambia", "Ghana", "Guinea", "Guinea-Bissau",
    "Liberia", "Libya", "Madagascar", "Malawi", "Mali", "Mauritius",
    "Mauritania", "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria", "Rwanda",
    "São Tomé and Príncipe", "Senegal", "Seychelles", "Sierra Leone", "Somalia", "South Africa",
    "Sudan", "Togo", "Tanzania", "Tunisia", "Uganda", "Zambia", "Zimbabwe"
]
# Countries in Oceania
oceania = [
    "Australia", "Fiji", "Kiribati", "Marshall Islands", "Micronesia", "Nauru", "New Zealand",
    "Palau", "Papua New Guinea", "Samoa", "Solomon Islands", "Tonga", "Tuvalu", "Vanuatu"
]

# COMMAND ----------

def group_locations(location):
    if location in europe:
        return 'Europe'
    elif location in asia:
        return 'Asia'
    elif location in north_america:
        return 'North America'
    elif location in south_america:
        return 'South America'
    elif location in oceania:
        return 'Oceania'
    elif location in africa:
        return 'Africa'
    else:
        return location
    

df_exploded_locations['region'] = df_exploded_locations['locations'].apply(group_locations)

# COMMAND ----------

incidents_per_region = df_exploded_locations['region'].value_counts().reset_index(name = 'No. of incidents')
incidents_per_region['%'] = ((incidents_per_region['No. of incidents'] / incidents_per_region['No. of incidents'].sum()) * 100).round(2)
incidents_per_region.rename(columns={'index': 'Region'}, inplace = True)
incidents_per_region

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT REGIONS

# COMMAND ----------

plt.figure(figsize = (8, 5))
plt.bar(incidents_per_region['Region'], incidents_per_region['No. of incidents'], edgecolor = 'navy', color = 'royalblue', width = 0.6)
plt.xlabel('Region')
plt.ylabel('Number of incidents')
plt.title('Number of incidents per region')
plt.xticks(rotation = 90)
plt.show()

# COMMAND ----------

labels = incidents_per_region['Region']
sizes = incidents_per_region['%']
colors = ['navy', 'lightskyblue','royalblue', 'silver', 'mediumturquoise', 'cornflowerblue', 'dimgray']
text_colors = ['white', 'black', 'black', 'black', 'black', 'black', 'black']

fig1, ax1 = plt.subplots(figsize = (6, 6))
wedges, texts, autotexts = ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%')
for i, autotext in enumerate(autotexts):
    autotext.set_color(text_colors[i])
ax1.axis('equal')
plt.title('Percentage of incidents per region')

# COMMAND ----------

# MAGIC %md
# MAGIC ##INDUSTRIES

# COMMAND ----------

# is_list = df_incidents_2024['industries'].apply(lambda x: isinstance(x, list))
df_incidents_2024['industries'] = df_incidents_2024['industries'].apply(ast.literal_eval)

df_exploded_industries = df_incidents_2024.explode('industries')
df_exploded_industries

# COMMAND ----------

#incluir en la página de confluence lo que incluye Transportation, Energy and Engineering

def change_names(industry):
    if industry == 'Transportation (Includes Logistics, Shipping, Maritime, Rail, Trucking)':
        return 'Transportation'
    elif industry == 'Energy (Includes Power and Utilities)':
        return 'Energy'
    elif industry == 'Engineering (Includes Industrial Construction)':
        return 'Engineering'
    elif industry == 'Water and Waste Water':
        return 'Water & Waste Water'
    else:
        return industry

df_exploded_industries['industries'] = df_exploded_industries['industries'].apply(change_names)

# COMMAND ----------

incidents_per_industry = df_exploded_industries['industries'].value_counts().reset_index(name = 'No. of Incidents')
incidents_per_industry['%'] = ((incidents_per_industry['No. of Incidents'] / incidents_per_industry['No. of Incidents'].sum()) * 100).round(2)
incidents_per_industry.rename(columns={'index': 'Industry'}, inplace = True)
incidents_per_industry

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT INDUSTRIES

# COMMAND ----------

plt.figure(figsize = (9, 5))
plt.bar(incidents_per_industry['Industry'], incidents_per_industry['No. of Incidents'], edgecolor = 'royalblue', color = 'lightskyblue', width = 0.6)
plt.xlabel('Industry')
plt.ylabel('Number of incidents')
plt.title('Number of incidents per industry')
plt.xticks(rotation = 90)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##VERTICALS

# COMMAND ----------

# def group_industries(industry):
#     if industry in ['Manufacturing', 'Energy', 'Oil & Gas', 'Aerospace']:
#         return industry
#     else:
#         return 'Other verticals'
def group_industries(industry):
    if industry in ['Manufacturing', 'Energy', 'Chemical industry', 'Transportation']:
        return industry 
    else:
        return 'Other verticals'
    

# COMMAND ----------

df_exploded_industries['industries'] = df_exploded_industries['industries'].str.replace('Oil & Gas', 'Chemical industry')
df_exploded_industries['industries'] = df_exploded_industries['industries'].str.replace('Aerospace', 'Transportation')

df_exploded_industries['verticals'] = df_exploded_industries['industries'].apply(group_industries)

incidents_per_vertical = df_exploded_industries['verticals'].value_counts().reset_index(name = 'No. of Incidents')
incidents_per_vertical['%'] = ((incidents_per_vertical['No. of Incidents'] / incidents_per_vertical['No. of Incidents'].sum()) * 100).round(2)
incidents_per_vertical.rename(columns={'index': 'Vertical'}, inplace = True)
incidents_per_vertical

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT VERTICALS

# COMMAND ----------

plt.figure(figsize = (8, 5))
plt.bar(incidents_per_vertical['Vertical'], incidents_per_vertical['No. of Incidents'], edgecolor = 'navy', color = 'royalblue', width = 0.3)
plt.xlabel('Vertical')
plt.ylabel('Number of incidents')
plt.title('Number of incidents per vertical')
plt.xticks(rotation = 45)
plt.show()

# COMMAND ----------

labels = incidents_per_vertical['Vertical']
sizes = incidents_per_vertical['%']
colors = ['navy', 'lightskyblue','royalblue', 'mediumturquoise', 'silver']
explode = (0.05, 0, 0, 0, 0)
text_colors = ['white', 'black', 'black', 'black', 'black']

fig1, ax1 = plt.subplots(figsize = (6, 6))
wedges, texts, autotexts = ax1.pie(sizes, labels=labels, explode = explode, colors=colors, autopct='%1.1f%%')
for i, autotext in enumerate(autotexts):
    autotext.set_color(text_colors[i])
ax1.axis('equal')
plt.title('Percentage of incidents per vertical')


# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT VERTICALS x MONTH

# COMMAND ----------

colors = ['navy', 'lightskyblue','royalblue', 'mediumturquoise', 'silver']

plt.figure(figsize = (20, 10))
ax = sns.countplot(x=df_exploded_industries['month'], hue=df_exploded_industries['verticals'], palette=colors)
ax.set_xticklabels(months)
ax.set_title('Number incidents per month and vertical')
ax.set_xlabel('Month')
ax.set_ylabel('Number of incidents')
plt.legend(title= 'Vertical', fontsize = '13', title_fontsize= '14', loc = 'upper right')

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT VERTICALS x COUNTRIES

# COMMAND ----------

df_exploded_industries_locations = df_exploded_industries.explode('locations')
colors = ['navy', 'lightskyblue','royalblue', 'gainsboro', 'mediumturquoise']

plt.figure(figsize = (20, 10))
ax = sns.countplot(x=df_exploded_industries_locations['locations'], hue=df_exploded_industries_locations['verticals'], palette=colors)
ax.set_title('Number incidents per location and vertical')
ax.set_xlabel('Location')
ax.set_ylabel('Number of incidents')
plt.xticks(rotation = 90)

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT VERTICALS x REGIONS

# COMMAND ----------

df_exploded_industries_locations['locations'] = df_exploded_industries_locations['locations'].str.replace('Hong Kong', 'China')
df_exploded_industries_locations['region'] = df_exploded_industries_locations['locations'].apply(group_locations)

# COMMAND ----------

colors = ['navy', 'lightskyblue','royalblue', 'mediumturquoise', 'silver']

plt.figure(figsize = (20, 10))
ax = sns.countplot(x=df_exploded_industries_locations['region'], hue=df_exploded_industries_locations['verticals'], palette=colors)
ax.set_title('Number incidents per region and vertical')
ax.set_xlabel('Region')
ax.set_ylabel('Number of incidents')
plt.legend(title= 'Vertical', fontsize = '13', title_fontsize= '14', loc = 'upper right')
# plt.xticks(rotation = 90)

# COMMAND ----------

# MAGIC %md
# MAGIC ##IMPACTS

# COMMAND ----------

def unify_impacts(impact):
    if impact == ' IT' or impact == ' IT Privacy' or impact == ' IT Safety':
        return 'IT'
    elif impact == ' OT IT':
        return 'OT IT'
    else:
        return impact

# COMMAND ----------

#no lo hago directamente con value_counts() porque esta columna tiene valores nulos que no cogería con value_counts()
df_incidents_2024['impacts_unified'] = df_incidents_2024['impacts'].apply(unify_impacts)


it_ot_count = 0
it_count = 0
nan_count = 0

for impact in list(df_incidents_2024['impacts_unified']):
    if pd.isna(impact):
        nan_count += 1
    else:
        if impact == 'IT':
            it_count += 1
        elif impact == 'OT IT':
            it_ot_count += 1

total = it_count + it_ot_count + nan_count
it_percentage = (it_count / total) * 100
it_ot_percentage = (it_ot_count / total) * 100
nan_percentage = (nan_count / total) * 100

incidents_per_impact = pd.DataFrame({'Impact': ['IT', 'OT IT', 'Unknown'],
                                     'No. of Incidents': [it_count, it_ot_count, nan_count],
                                     '%': [it_percentage, it_ot_percentage, nan_percentage]
                                     })

incidents_per_impact['%'] = incidents_per_impact['%'].round(2)
incidents_per_impact

# COMMAND ----------

it_count = 0
ot_count = 0
nan_count = 0

for impact in list(df_incidents_2024['impacts_unified']):
    if pd.isna(impact):
        nan_count += 1
    else:
        if 'IT' in impact:
            it_count += 1
        if 'OT' in impact:
            ot_count += 1

total = it_count + ot_count + nan_count
it_percentage = (it_count / total) * 100
ot_percentage = (ot_count / total) * 100
nan_percentage = (nan_count / total) * 100


incidents_per_impact_2 = pd.DataFrame({'Impact': ['IT', 'OT', 'Unknown'],
                                     'No. of Incidents': [it_count, ot_count, nan_count],
                                     '%': [it_percentage, ot_percentage, nan_percentage]
                                     })
incidents_per_impact_2['%'] = incidents_per_impact_2['%'].round(2)
incidents_per_impact_2

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT IMPACTS

# COMMAND ----------

plt.figure(figsize = (8, 5.5))
plt.bar(incidents_per_impact['Impact'], incidents_per_impact['No. of Incidents'], edgecolor = 'royalblue', color = 'lightskyblue', width = 0.3)
plt.xlabel('Impact')
plt.ylabel('Number of incidents')
plt.title('Number of incidents per impact')
plt.show()

# COMMAND ----------

plt.figure(figsize = (8, 4))
plt.bar(incidents_per_impact_2['Impact'], incidents_per_impact_2['No. of Incidents'], edgecolor = 'royalblue', color = 'lightskyblue', width = 0.3)
plt.xlabel('Impact')
plt.ylabel('Number of incidents')
plt.title('Number of incidents per impact')
plt.show()

# COMMAND ----------

labels = incidents_per_impact['Impact']
sizes = incidents_per_impact['%']
colors = ['navy', 'royalblue', 'lightskyblue']
explode = (0, 0.1, 0)

plt.figure(figsize = (6, 6))
plt.pie(sizes, explode = explode, autopct='%1.1f%%', labels = labels, colors = colors)
plt.title('Percentage of incidents per impact')
plt.axis('equal')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##TYPES OF MALWARE

# COMMAND ----------

def extract_malware_types(malware):
    if malware == 'No Malware identified':
        return ['No Malware identified']
    else:
        malware_list = ast.literal_eval(malware)
        attack_types = [malware['title'] for malware in malware_list]
        return attack_types

# COMMAND ----------

df_incidents_2024['attack_types'] = df_incidents_2024['type_of_malware'].apply(extract_malware_types)
df_exploded_attack_types = df_incidents_2024.explode('attack_types')
df_exploded_attack_types

# COMMAND ----------

#Unficamos los 5 tipos de ataques ransomware que aparecen en uno único: Ransomware

def unify_ransomwares(attack):
    if attack == 'Unknown ransomware variant' or attack == 'Ransomware – Unknown group or variant' or attack == 'Ransomware – unknown' or attack == 'Backmydata ransomware' or attack == 'LockBit':
        return 'Ransomware attack'
    else:
        return attack
        

df_exploded_attack_types['attack_types'] = df_exploded_attack_types['attack_types'].apply(unify_ransomwares)
df_exploded_attack_types['attack_types'] = df_exploded_attack_types['attack_types'].str.replace('Attack', 'attack')

# COMMAND ----------

incidents_per_attack_type = df_exploded_attack_types['attack_types'].value_counts().reset_index(name = 'No. of Incidents')
incidents_per_attack_type['%'] = ((incidents_per_attack_type['No. of Incidents'] / incidents_per_attack_type['No. of Incidents'].sum()) * 100).round(2)
incidents_per_attack_type.rename(columns={'index': 'Attack type'}, inplace = True)
incidents_per_attack_type

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT ATTACK TYPES 

# COMMAND ----------

plt.figure(figsize = (9, 5.5))
plt.bar(incidents_per_attack_type['Attack type'], incidents_per_attack_type['No. of Incidents'], edgecolor = 'navy', color = 'royalblue', width = 0.3)
plt.xlabel('Attack type')
plt.ylabel('Number of incidents')
plt.title('Number of incidents per Attack type')
plt.xticks(rotation = 45)
plt.show()

# COMMAND ----------

labels = incidents_per_attack_type['Attack type']
sizes = incidents_per_attack_type['%']
colors = ['navy', 'lightskyblue', 'royalblue', 'silver']
# explode = (0.1, 0, 0, 0)
text_colors = ['white', 'black', 'black', 'black']

fig1, ax1 = plt.subplots(figsize = (6, 6))
wedges, texts, autotexts = ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%')
for i, autotext in enumerate(autotexts):
    autotext.set_color(text_colors[i])
ax1.axis('equal')
plt.title('Percentage of incidents per attack type')


# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT ATTACK TYPES x IMPACTS

# COMMAND ----------

colors = ['navy', 'lightskyblue']

plt.figure(figsize = (20, 10))
ax = sns.countplot(x=df_exploded_attack_types['attack_types'], hue=df_exploded_attack_types['impacts_unified'], palette=colors)
ax.set_title('Number incidents per vertical and attack type')
ax.set_xlabel('Attack Types')
ax.set_ylabel('Number of incidents')

# COMMAND ----------

# MAGIC %md
# MAGIC ###PLOT ATTACK TYPES x VERTICALS

# COMMAND ----------

df_exploded_attack_types_industries = df_exploded_attack_types.explode('industries')
df_exploded_attack_types_industries['industries'] = df_exploded_attack_types_industries['industries'].apply(change_names)
df_exploded_attack_types_industries['verticals'] = df_exploded_attack_types_industries['industries'].apply(group_industries)

# COMMAND ----------

# colors = ['royalblue', 'lightskyblue', 'mediumorchid', 'pink', 'lightseagreen', 'hotpink']
colors = ['navy', 'lightskyblue', 'royalblue', 'silver']

plt.figure(figsize = (15, 8))
ax = sns.countplot(x=df_exploded_attack_types_industries['verticals'], hue=df_exploded_attack_types_industries['attack_types'], palette=colors)
ax.set_title('Number incidents per vertical and attack type')
ax.set_xlabel('Vertical')
ax.set_ylabel('Number of incidents')
plt.yticks(range(0, 61, 5))
plt.legend(title= 'Attack Types', fontsize = '9', title_fontsize= '10', loc = 'upper right')

# COMMAND ----------

# MAGIC %md
# MAGIC #TO CSV

# COMMAND ----------

# df_incidents_2024 = df_incidents_2024.drop(columns = ['date_clean'])
# df_incidents_2024

# COMMAND ----------

# df_incidents_2024 = df_incidents_2024.drop(columns = ['date_clean', 'month', 'impacts_unified', 'attack_types'])

# COMMAND ----------

# DBTITLE 1,To CSV
# df_incidents_2024.to_csv(f'/dbfs/FileStore/user_ol/incidents/2024/csvs/icsstrive/icsstrive_2024_extended_incidents_{CURRENT_DATE}.csv', index=False)
