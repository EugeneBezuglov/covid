#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install psycopg2


# In[ ]:


conda install -c anaconda sqlalchemy


# In[ ]:


conda update -n base -c defaults conda


# In[ ]:


import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:11112222@localhost:5432/covid')
df=pd.read_csv('D:\PostgreSQL\Data\owid-covid-data.csv')
df.to_sql('covid', engine)

