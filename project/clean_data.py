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

#######################################################################

def create_spark_session():
    """
    This function:
        - Creates a Spark Sesson and 
        - includes necessary Jar and adoop packages in the configuration. 
    """
    spark=SparkSession.builder.config("spark.jars.repositories", "https://repos.spark-packages.org/") \
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()
    return spark


def convert_SAS_datetime(x):
    """
        Converts SAS dates to datetime
        
        Cribbed from https://knowledge.udacity.com/questions/66798
    """
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None

    
def clean_immigration_fact_data(spark, path_to_files, output_path):
    """
        Function to clean sas files contaiing immigration data 
        and output the results in a set of parquet files
        
        Takes a spark session, a filepath for the raw files 
        and an output path for the ccleaned files  

        Usage:     
        cleanImmigrationFactData(<spark_session>, <path_to_files>, <output_path>)
        
    """
    print("Cleaning immigration data")
    
    filelist = os.listdir(path_to_files)
    
    i = 0
    
    print(f"The dataset contains {len(filelist)} files")
    
    for file in filelist:
        
        filepath = '{}{}'.format(path_to_files, file)
        
        size = os.path.getsize('{}/{}'.format(path_to_files, file))        
        print(f'Processing {filepath} - dim(bytes): {size} ')
        
        df_I94 = spark.read.format('com.github.saurfang.sas.spark').load(filepath).persist()

        # Snippet taken from https://www.1week4.com/it/machine-learning/udacity-data-engineering-capstone-project/
        toInt = udf(lambda x: int(x) if x!=None else x, IntegerType())

        for colname, coltype in df_I94.dtypes:
            if coltype == 'double':
                df_I94 = df_I94.withColumn(colname, toInt(colname))
        
        # Convert strings to dates
        df_I94 = df_I94.withColumn('dtaddto',to_date(col("dtaddto"),"MMddyyyy")) \
        .withColumn('dtadfile',to_date(col("dtadfile"),"yyyyMMdd"))
        
        # Convert SAS date to dates
        udf_datetime_from_sas = udf(lambda x: convert_SAS_datetime(x), T.DateType())
        
        df_I94 = df_I94.withColumn("arrdate", udf_datetime_from_sas("arrdate")) \
        .withColumn("depdate", udf_datetime_from_sas("depdate"))        

        # Drop a set of columns which only appear in the jun file and always contain 0
        df_I94 = df_I94.drop('delete_days','delete_dup', 'delete_mexl','delete_visa','delete_recdup','validres')
        
        # A couple of simple DQ checks
        try:
            assert df_I94.count() > 0, 'No rows found'
            assert len(df_I94.columns) == 28, f'26 columns expected but {len(df_I94.columns)} columns found'
        
        except AssertionError as msg:
            sys.exit(msg)
            

        output_imm_data(df_I94, output_path)
        

def output_imm_data(df_imm_data, output_path):
    """
        Outputs the cleaned immigration data
        
        Usage:
        output_imm_data(<df_imm_data>, <output_path>):
    """
    # write data out
    os.makedirs(output_path, exist_ok=True)  
    print(f'Exporting cleaned file to {output_path}')
    df_imm_data.write.format('parquet').mode('overwrite').partitionBy('i94yr','i94mon').save(output_path)


def read_i94_dimension_data(spark, label_file):
    """
        Reads the i94 diemension data into a dataframe; returns the dataframe
        
        Usage: 
        read_i94_dimension_data(<spark_session>, <label_file>)
    
    """
    print(f'Reading {label_file}')
    df_label_full = spark.read.text(label_file, wholetext=True)

    return df_label_full

def extract_airport_codes(df_label_full):
    """
        Takes a dataframe containing I94 labels and extracts the airport codes
        
        Usage:
        extract_airport_codes(<df_label_full>)
    """
    
    print(f'Extracting airport codes from dataframe')
    # airport codes
    pattern='(\$i94prtl)([^;]+)'

    df_extract = df_label_full.withColumn('I94PORT', regexp_extract(col('value'),pattern,2))
    df_extract = df_extract.withColumn('port',explode(split('I94PORT','[\r\n]+'))).drop('value').drop('I94PORT')
    df_I94PORT = df_extract.withColumn('port_code',trim(regexp_extract(col('port'),"(?<=')[0-9A-Z. ]+(?=')",0)))         .withColumn('city_state',trim(regexp_extract(col('port'),"(=\t')([0-9A-Za-z ,\-()\/\.#&]+)(')",2)))         .withColumn('city', trim(split(col('city_state'),',').getItem(0)))         .withColumn('state', trim(split(col('city_state'),',').getItem(1)))         .withColumn('state', trim(regexp_replace(col('state'), ' *$', '')))         .where(col('port')!='')         .drop('port')     
    return df_I94PORT

def extract_country_codes(df_label_full):    
    """
        Takes a dataframe containing I94 labels and extracts the country codes
        
        Usage:
        extract_country_codes(<df_label_full>)   
    """
    
    print(f'Extracting country codes from dataframe')
    pattern='(i94cntyl)([^;]+)'

    df_extract = df_label_full.withColumn('I94RES', regexp_extract(col('value'),pattern,2))
    df_extract = df_extract.withColumn('raw',explode(split('I94RES','[\r\n]+'))).drop('value').drop('I94RES')
    df_I94RES = df_extract.withColumn('country_code',trim(regexp_extract(col('raw'),"[0-9]+",0)))     .withColumn('country',trim(regexp_extract(col('raw'),"\'([A-Za-z ,\-()0-9]+)\'",1)))     .where(col('raw')!='')     .drop('raw')
    
    return df_I94RES

def extract_state_codes(df_label_full):    
    """
        Takes a dataframe containing I94 labels and extracts the state codes
        
        Usage:
        extract_state_codes(<df_label_full>)   
    """
    
    print(f'Extracting state codes from dataframe')
    pattern='(i94addrl)([^;]+)'

    df_extract = df_label_full.withColumn('i94addrl', regexp_extract(col('value'),pattern,2))
    df_extract = df_extract.withColumn('raw',explode(split('i94addrl','[\r\n]+'))).drop('value').drop('i94addrl')
    df_I94ADDR = df_extract.withColumn('state_code',trim(regexp_extract(col('raw'),"(?<=')[0-9A-Z. ]+(?=')",0)))     .withColumn('state',trim(regexp_extract(col('raw'),"(=\s*\')([A-Z]+)(\')",2)))     .where(col('raw')!='')     .drop('raw')
    
    return df_I94ADDR

def build_visa_data(spark):    
    """
        Builds a dataframe for the visa dimension data
        
        Usage:
        build_visa_data(<spark_session>)
    """
    
    print('Building visa code df')
    columns = ['I94VISA', 'category']
    vals = [(1,'Business'),(2,'Pleasure'),(3,'Student')]

    df_I94VISA = spark.createDataFrame(vals, columns)
    
    return df_I94VISA

def build_mode_data(spark):
    """
        Builds a dataframe for the arrival mode dimension data
        
        Usage:
        build_mode_data(<spark_session>)    
    """
    
    print('Building entry mode code df')    
    columns = ['I94MODE', 'category']
    vals = [(1,'Air'),(2,'Sea'),(3,'Land'),(4,'Not reported')]

    df_I94MODE = spark.createDataFrame(vals, columns)
    
    return df_I94MODE

def write_dimension_data(df_output, output_path, output_filename):
    """
        Takes a dataframe containing dimension data and writes it out as a csv file
        
        Usage:
        write_dimension_data(<df_output>, <output_path>, <output_filename>)
    """
    print('Writing data to {}/{}'.format(output_path,output_filename)) 
    os.makedirs(output_path, exist_ok=True)  
    df_output.toPandas().to_csv('{}/{}'.format(output_path,output_filename),index=False)


