#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine


# In[2]:


green_trips = pd.read_parquet("./green_tripdata_2025-11.parquet")
zones = pd.read_csv("./taxi_zone_lookup.csv")


# In[6]:


pg_user = 'root'
pg_password = 'root'
pg_host = 'pg-database'
pg_port = '5432'
pg_db = 'ny_taxi'
target_table = 'green_taxi_data'


# In[7]:


engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')


# In[8]:


green_trips.head(0).to_sql(name=target_table, con=engine, if_exists = 'replace')


# In[9]:


green_trips.to_sql(name=target_table, con=engine, if_exists = 'append')


# In[10]:


target_table = 'taxi_zones'


# In[11]:


zones.head(0).to_sql(name=target_table, con=engine, if_exists = 'replace')


# In[12]:


zones.to_sql(name=target_table, con=engine, if_exists = 'append')


# In[ ]:




