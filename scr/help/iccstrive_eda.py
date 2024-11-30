# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis

# COMMAND ----------

df_incidents = spark.read.table("hive_metastore.temporal_tables.dkc_icsstrive_incident_details").toPandas()
df_incidents

# COMMAND ----------

df_incidents.groupby('date_uploaded').size()

# COMMAND ----------



# COMMAND ----------

displayHTML(df_incidents.loc[
    (df_incidents['industries'].str.contains('Energy', regex=True))
    & (df_incidents['date'].str.contains('2023', regex=True)),
    ['data_source_link_url', 'victims']
].to_html())

# COMMAND ----------

df_incidents.loc[
    (df_incidents['industries'].str.contains('Energy', regex=True))
    & (df_incidents['date'].str.contains('2023', regex=True)),
    'data_source_link_url'
].to_list()

# COMMAND ----------

df_index = pd.read_csv('/dbfs/FileStore/user_ol/temporal/icsstrive_index_incidents_2023_09_26.csv', index_col=False)
df_extended = pd.read_csv('/dbfs/FileStore/user_ol/temporal/icsstrive_extended_incidents_2023_09_26.csv', index_col=False)

df_index

# COMMAND ----------

df_extended

# COMMAND ----------

df_index['date'] = pd.to_datetime(df_index['date'])

# COMMAND ----------

df_index.sort_values(by=['date'], ascending=False).head(10)

# COMMAND ----------

df_index[('2017-01' <= df_index['date']) & (df_index['date'] < '2018-01')]

# COMMAND ----------

s = pd.Series([1]*len(df_extended), index=pd.to_datetime(df_extended['date']))
s = s.resample('Y').sum()
s_selected = s[s.index >= '2000']
s_selected.plot(kind='line', figsize=(15,6))

# COMMAND ----------

# Unknown, Other, [], 
df_extended['industries'].value_counts()[["['Unknown']", "['Other']", "[]"]]

# COMMAND ----------

70/844

# COMMAND ----------

import ast
df_extended['industries'].apply(lambda s: ast.literal_eval(s)).explode().value_counts().plot(kind='bar')

# COMMAND ----------

df_extended['type_of_malware'].explode().value_counts()

# COMMAND ----------

import ast

def parse_column(s):
    if s == 'No Malware identified':
        return s
    l = ast.literal_eval(s)
    if isinstance(l, list):
        return [e['title'] for e in l]
    else:
        print(s)
        raise ValueError()

df_extended['type_of_malware'].apply(parse_column).explode().value_counts()

# COMMAND ----------

import ast

def parse_column(s):
    if s == 'No victims identified':
        return s
    l = ast.literal_eval(s)
    if isinstance(l, list):
        return [e['title'] for e in l]
    else:
        print(s)
        raise ValueError()
    
df_extended['victims'].apply(parse_column).explode().value_counts()

# COMMAND ----------

df_extended['impacts'].explode().value_counts()

# COMMAND ----------

df_extended['impacts'].str.strip().str.split('\s+', regex=True).explode().value_counts()

# COMMAND ----------

df_index[df_index['victim'].str.contains('Kojima')].loc[309, 'title']

# COMMAND ----------

df_extended[df_extended['description'].str.contains('Kojima')].loc[309, 'description']
