import time
import configparser
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import boto3
import json
import os
import re

from botocore.exceptions import ClientError

from datetime import datetime, timedelta
from pyspark.sql.functions import trim, unix_timestamp, udf, lit, explode, split, regexp_extract, col, isnan, isnull, desc, when, sum, to_date, desc, regexp_replace, count, to_timestamp, current_timestamp
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql import types as T
from pyspark.sql import SparkSession

import importlib

from sql_queries import *
from clean_data import *
from upload_data import *
from redshift import *
from etl import *

##########################################################################################

print('Reading configuration')

# Read configuration
config = configparser.ConfigParser()
config.read_file(open('aws.cfg'))

DB_CLUSTER_TYPE        = config.get("CLUSTER","DB_CLUSTER_TYPE")
DB_NUM_NODES           = config.get("CLUSTER","DB_NUM_NODES")
DB_NODE_TYPE           = config.get("CLUSTER","DB_NODE_TYPE")

DB_NAME                = config.get("CLUSTER","DB_NAME")
DB_USER                = config.get("CLUSTER","DB_USER")
DB_PASSWORD            = config.get("CLUSTER","DB_PASSWORD")
DB_PORT                = config.get("CLUSTER","DB_PORT")

DB_CLUSTER_IDENTIFIER  = config.get("CLUSTER","DB_CLUSTER_IDENTIFIER")
DB_SNAPSHOT_IDENTIFIER = config.get("CLUSTER","DB_SNAPSHOT_IDENTIFIER")
DB_SNAPSHOT_RETENTION  = config.get("CLUSTER","DB_SNAPSHOT_RETENTION")

IAM_ROLE_NAME          = config.get("IAM_ROLE", "IAM_ROLE_NAME")
ARN                    = config.get("IAM_ROLE", "ARN")

S3_BUCKET              = config.get("S3", "S3_BUCKET")
S3_FOLDER              = config.get("S3", "S3_FOLDER")

I94_DATASET_PATH       = config.get("LOCAL_DATA", "I94_DATASET_PATH")
CLEAN_DATA_DIR         = config.get("LOCAL_DATA", "CLEAN_DATA_DIR")
LOCAL_FILEPATH         = config.get("LOCAL_DATA", "LOCAL_FILEPATH")
I94_LABELS             = config.get("LOCAL_DATA", "I94_LABELS")

(DB_USER, DB_PASSWORD, DB_NAME)

pd.DataFrame({"Param":
                  ["DB_CLUSTER_TYPE", "DB_NUM_NODES", "DB_NODE_TYPE", "DB_CLUSTER_IDENTIFIER", 
                   "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT", "IAM_ROLE_NAME","ARN"],
              "Value":
                  [DB_CLUSTER_TYPE, DB_NUM_NODES, DB_NODE_TYPE, DB_CLUSTER_IDENTIFIER, 
                   DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, IAM_ROLE_NAME, ARN],
             })

config.read_file(open('aws.credentials'))
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

##########################################################################################
print('Cleaning data')

# Create a spark session
spark = create_spark_session()
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

# Clean the immigration data
### !!!
# clean_immigration_fact_data(spark, I94_DATASET_PATH,'{}imm'.format(CLEAN_DATA_DIR))

# Clean the dimension data
print("Reading dimension data from {I94_LABELS} for cleaning")

# df_labels = read_i94_dimension_data(spark, I94_LABELS)

# df_I94PORT = extract_airport_codes(df_labels)
# write_dimension_data(df_I94PORT,'{}dim'.format(CLEAN_DATA_DIR),'I94PORT.csv')

# df_I94RES = extract_country_codes(df_labels)
# write_dimension_data(df_I94RES,'{}dim'.format(CLEAN_DATA_DIR),'I94RES.csv')

# df_I94ADDR = extract_state_codes(df_labels)
# write_dimension_data(df_I94ADDR,'{}dim'.format(CLEAN_DATA_DIR),'I94ADDR.csv')

# df_I94VISA = build_visa_data(spark)
# write_dimension_data(df_I94VISA,'{}dim'.format(CLEAN_DATA_DIR),'I94VISA.csv')

# df_I94MODE = build_mode_data(spark)
# write_dimension_data(df_I94MODE,'{}dim'.format(CLEAN_DATA_DIR),'I94MODE.csv')

##########################################################################################
print('Setting up S3 bucket {S3_BUCKET}')

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

s3_client = boto3.client('s3',
                   region_name="us-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

create_bucket(s3)

print("Uploading cleaned data to {S3_BUCKET}\{S3_FOLDER}\n")

###!!!!
# upload_to_s3(s3, LOCAL_FILEPATH, S3_BUCKET, S3_FOLDER)

##########################################################################################
print("Creating clients for IAM, EC2 and Redshift")

# Create clients for IAM, EC2 and Redshift
iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
roleArn=create_iam_role(iam)
print(roleArn)

start_cluster(redshift, roleArn)

# pause_cluster(redshift)
# resume_cluster(redshift)
# delete_cluster(redshift)

