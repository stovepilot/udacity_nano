#!/usr/bin/env python
# coding: utf-8

# ## Pre-requisite steps - Create an IAM user and save the credentials
# 
# ### Add aws.credentials to .gitignore
# 1. echo "aws.credentials" >> .gitignore
# 
# ### Set up an AWS user whose credentials you are going to use
# 1. Launch AWS (I did it from the Udacity console
# 1. Navigate to Services --> IAM --> Users
# 1. Choose a name of your choice.
# 1. Select "Programmatic access" as the access type. Click Next.
# 1. Choose the Attach existing policies directly tab, and select the "AdministratorAccess". Click Next.
# 1. Skip adding any tags. Click Next.
# 1. Review and create the user. It will show you a pair of access key ID and secret.
# 1. Take note of the pair of access key ID and secret. This pair is collectively known as Access key.
# 1. Add the access key id and the secret key id to the aws.credentials file as follows 
# 
# ```
# [AWS]
# KEY=#####################
# SECRET=#################################

import os
import configparser

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

def upload_to_s3(s3, local_filepath, s3_bucket_name, destination_folder, destination_filename=None):
    """
    Function to upload data from the local filesystem to S3
    
    Parameters:
    s3_resource - a boto3 s3 resource object
    local_filepath - the local file or folder.  If a folder is provided then all the files are uploaded (excluding chcksum files)
    s3_bucket_name - the s3 bucket name
    destination_folder - the folder in the s3 bucket
    destination_filename - the destination filename.  Only used when the local_filepath is a file.  If None then th original filename is used
    
    Usage:
    upload_to_s3(<s3_resource>, <local_filepath>, <s3_bucket_name>, <s3_destination_folder>, [destination_filename=None])
    
    """

    #if a directory is passed as origin_filepath parameter all files are copied into the destination_folder
    i=0
    if os.path.isfile(local_filepath):
        if destination_filename == None:
            destination_filename = re.findall('([^\/]*$)', local_filepath)[0]
        
        destination_filepath=destination_folder+'/'+destination_filename
        
        print(f'Transfering file: {local_filepath} ==> {s3_bucket_name}/{destination_filepath}\n')
        
        s3.meta.client.upload_file(local_filepath, s3_bucket_name, destination_filepath)
                
    elif os.path.isdir(local_filepath):
        filelist = os.walk(local_filepath)

        for root, subFolders, files in filelist:
            
            for file in files:
                
                if file[-4:] != '.crc':
                    
                    local_filename = os.path.join(root, file)
                    destination_filepath = os.path.join(destination_folder, local_filename.replace(local_filepath, ''))

                    print(f'Transfering file: {local_filepath} ==> {s3_bucket_name}/{destination_filepath}\n')
                    s3.meta.client.upload_file(local_filename, s3_bucket_name, destination_filepath)
                
    else: 
        
        print('Error - specified local file or directory not found.')   
        
def list_files_in_bucket():
    """
    Lists the files in the s3 Bucket    
    """
    my_bucket = s3.Bucket(S3_BUCKET)

    for my_bucket_object in my_bucket.objects.all():
        print(f'{my_bucket_object.key}\t\t{my_bucket_object.size}')

# Create S3 buckets
def create_bucket(s3):
    """
    Creates an s3 Bucket
    """
    print(f"Creating s3 bucket {S3_BUCKET} (unless it already exists)") 
    try:
        s3.create_bucket(Bucket=S3_BUCKET, CreateBucketConfiguration={
                        'LocationConstraint': 'us-west-2'})
    except Exception as e:
        print(e)

# Check that the new bucket exists
def list_buckets(s3_client):
    """
    List the s3 accessible s3 buckets
    """
    response = s3_client.list_buckets()

    # Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'\t{bucket["Name"]}')
       