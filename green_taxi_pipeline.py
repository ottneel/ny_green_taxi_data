#!/usr/bin/env python
# coding: utf-8

# In[21]:


#import necessary libraries
import pandas as pd
from sqlalchemy import create_engine
from time import time
import subprocess


# In[ ]:





# In[ ]:


if url.endswith('.csv.gz'):
    csv_name = 'output.csv.gz'
else:
    csv_name = 'output.csv'

subprocess.run(["wget", url, "-O", csv_name])


# In[22]:


#create the connection engine to the database
engine = create_engine('postgresql://roots:roots@localhost:5431/ny_taxi')
#check the database connection
engine.connect()


# In[25]:


# create a dataframe iterator to load the data in chunk sizes
data_iter = pd.read_csv('green_tripdata_2019-09.csv', iterator = True, chunksize = 100000)


# In[24]:


#input the table names in the database using the to_sql method
data.head(0).to_sql(name = 'green_taxi_data', con = engine, if_exists='replace')


# In[26]:


#try block to iterate through the dataset and load the data chunk by chunk. and stop when there are no more chunks to add
try:
    while True:
        t_start = time() #track start time of each chunk
        data = next(data_iter)
        #change the datatype of the pickup and dropoff time from object to datetime
        data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
        data['lpep_dropoff_datetime'] = pd.to_datetime(data['lpep_dropoff_datetime'])
        data.to_sql(name = 'green_taxi_data', con = engine, if_exists='append')
    
        t_end = time() #track end time of each chunk
    
        print('inserted another chunk...., took %.3f second' % (t_end - t_start))
except StopIteration:
    print('No more chunks to add')


# In[30]:


#input the second table in the database using the to_sql method
data2.to_sql(name = 'taxi+_zone_lookup', con = engine, if_exists='replace')


# In[ ]:




