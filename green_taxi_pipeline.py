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

    # Check the file extension of the URL to determine the output file name
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz' # If the URL ends with '.csv.gz', set the output file name to 'output.csv.gz'
    else:
        csv_name = 'output.csv' # Otherwise, set the output file name to 'output.csv'
        
   # Download the contents from the URL and save it to the specified output file.
    subprocess.run(["wget", url, "-O", csv_name])   # Use wget command to download the contents from the URL and save it to the output file

    if url2.endswith('.csv.gz'):
        csv2_name = 'output2.csv.gz'
    else:
        csv2_name = 'output2.csv'
    # download the contents on the second url and save the output to csv2_name variable
    subprocess.run(["wget", url2, "-O", csv2_name])


    #create the connection engine to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    #check the database connection
    engine.connect()


    # create a dataframe iterator to load the data in chunk sizes
    data_iter = pd.read_csv(csv_name, iterator = True, chunksize = 100000)

    df = next(data_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    #input the table names in the database using the to_sql method
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    #try block to iterate through the dataset and load the data chunk by chunk. and stop when there are no more chunks to add
    while True:
        try:
            t_start = time() #track start time of each chunk
            df = next(data_iter)
            #change the datatype of the pickup and dropoff time from object to datetime

            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

            # add each chunk to the database
            df.to_sql(name = table_name, con = engine, if_exists='append')
    
            t_end = time() #track end time of each chunk
    
            print('inserted another chunk...., took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('No more chunks to add')
            break # leave the loop

    #read and load the second table in the database using the to_sql method
    data2=pd.read_csv(csv2_name)
    data2.to_sql(name = table_name2, con = engine, if_exists='replace')
    print('added second csv file')


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


