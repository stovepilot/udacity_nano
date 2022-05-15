#!/usr/bin/env python
# coding: utf-8

from time import time
import configparser
import matplotlib.pyplot as plt
import pandas as pd
import configparser
import psycopg2
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

#DWH_ROLE_ARN                = config.get("IAM_ROLE","ARN")

import boto3
redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]

DB_ENDPOINT = myClusterProps['Endpoint']['Address']
DB_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

def drop_tables(cur, conn):
    """
    - Drops the existing tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    - Recreates the tables
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                        .format(DB_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))

cur = conn.cursor()

drop_tables(cur, conn)
print ('Tables dropped')
create_tables(cur, conn)
print ('Tables created')

conn.close()