#!/usr/bin/env python
# coding: utf-8

from time import time
import configparser
import matplotlib.pyplot as plt
import pandas as pd
import configparser
import psycopg2
import boto3
from sql_queries import *

config = configparser.ConfigParser()
config.read_file(open('/home/workspace/dwh.cfg'))
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DB_CLUSTER_IDENTIFIER  = config.get("CLUSTER","DB_CLUSTER_IDENTIFIER")
DB_NAME                = config.get("CLUSTER","DB_NAME")
DB_USER                = config.get("CLUSTER","DB_USER")
DB_PASSWORD         = config.get("CLUSTER","DB_PASSWORD")
DB_PORT                = config.get("CLUSTER","DB_PORT")

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]

DB_ENDPOINT = myClusterProps['Endpoint']['Address']
DB_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

def copy_tables(cur, conn):
    """
    - Copies data into the staging tables
    """
    for query in copy_table_queries:
        cur.execute(query.format(DB_ROLE_ARN))
        conn.commit()

def insert_tables(cur, conn):
    """
    - Inserts data from the staging tales into the analytics tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def check_tables(cur, conn):
    """
    - runs a quick check on the warehouse
    """
    for query in check_table_queries:
        cur.execute(query)
        conn.commit()
        print(list(cur))

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                        .format(DB_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))

cur = conn.cursor()

print('Starting data import to staging tables (this may take several minutes)')
copy_tables(cur, conn)
print ('Data staged')
print('Starting insert')
insert_tables(cur, conn)
print ('Data inserted into warehouse')
print('Checking songplay table contents')
print('Rows, Songplays, Artists, Songs, Users')
check_tables(cur, conn)

conn.close()