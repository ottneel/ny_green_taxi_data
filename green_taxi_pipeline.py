#!/usr/bin/env python
# coding: utf-8

#import necessary libraries
import pandas as pd
from sqlalchemy import create_engine
from time import time
import subprocess
import argparse

#define the main function which takes in the parameters to be used.
def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    table_name2=params.table_name2
    url2 = params.url2
    url = params.url


    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    subprocess.run([r"C:\Program Files\Git\mingw64\bin\wget.exe", url, "-O", csv_name])

    if url2.endswith('.csv.gz'):
        csv2_name = 'output2.csv.gz'
    else:
        csv2_name = 'output2.csv'

    subprocess.run([r"C:\Program Files\Git\mingw64\bin\wget.exe", url2, "-O", csv2_name])


    #create the connection engine to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    #check the database connection
    engine.connect()


    # create a dataframe iterator to load the data in chunk sizes
    data_iter = pd.read_csv(csv_name, iterator = True, chunksize = 100000)

    #input the table names in the database using the to_sql method
    data.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    #try block to iterate through the dataset and load the data chunk by chunk. and stop when there are no more chunks to add
    while True:
        try:
            t_start = time() #track start time of each chunk
            data = next(data_iter)
            #change the datatype of the pickup and dropoff time from object to datetime
            data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
            data['lpep_dropoff_datetime'] = pd.to_datetime(data['lpep_dropoff_datetime'])
            data.to_sql(name = table_name, con = engine, if_exists='append')
    
            t_end = time() #track end time of each chunk
    
            print('inserted another chunk...., took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('No more chunks to add')
        break

    #read and load the second table in the database using the to_sql method
    data2=pd.read_csv(csv2_name)
    data2.to_sql(name = table_name2, con = engine, if_exists='replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--table_name2', required=True, help='name of the second table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    parser.add_argument('--url2', required=True, help='url of the second csv file')

    args = parser.parse_args()

    main(args)


