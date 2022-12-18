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

from etl import * 

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

def create_iam_role(iam):
    """
    Takes an IAM object
    Creates an S3 role to call Redshift services
    Attaches a policy to give the role access to S3
    
    Returns the IAM role
    
    create_iam_role(<iam>)
    """
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'rexdshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        if type(e).__name__ != 'EntityAlreadyExistsException':
            print(f'Error {type(e).__name__}')

    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    return iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']


def create_new_cluster(redshift, roleArn):
    """
    Takes a boto3 redshift client object and a role ARN and creates a new Redshift cluster
    
    Usage:
    create_new_cluster(<redshift_client>, <roleArn>)
    """
    print("Creating a new Redshift cluster")

    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DB_CLUSTER_TYPE,
            NodeType=DB_NODE_TYPE,
            NumberOfNodes=int(DB_NUM_NODES),
            
            #Snapshots
#            AutomatedSnapshotRetentionPeriod=DB_SNAPSHOT_RETENTION,

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]
        )
        
        wait_for_cluster(redshift, 'available')
        
        return redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    except Exception as e:
        print(e)


# In[50]:


def pause_cluster(redshift):
    """
    Takes a boto3 redshift client object, pauses the cluster and retains a snapshot
    
    Usage:
    pause_cluster(<redshift_client>)
    """
    print(f'Pausing cluster {DB_CLUSTER_IDENTIFIER}')
    print(f'Deleting old snapshot ({DB_SNAPSHOT_IDENTIFIER})')
    
    try:
        delete_snapshots(redshift, DB_SNAPSHOT_IDENTIFIER)
        print(f'Deleting cluster {DB_CLUSTER_IDENTIFIER} while retaining snapshot ({DB_SNAPSHOT_IDENTIFIER})')
        redshift.delete_cluster( ClusterIdentifier=DB_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=False, FinalClusterSnapshotIdentifier=DB_SNAPSHOT_IDENTIFIER)
        wait_for_cluster(redshift, 'deleted')
        print(f'Cluster deleted with snapshot {DB_SNAPSHOT_IDENTIFIER} retained.')
    
    except Exception as e:
        print(e)

def resume_cluster(redshift, roleArn):
    """
    Takes a boto3 redshift client object and a role ARN and  resumes the cluster from a snapshot
    
    Usage:
    resume_cluster(<redshift_client>, <roleArn>)
    """
    print(f'Resuming cluster from snapshot {DB_SNAPSHOT_IDENTIFIER}')
    redshift.restore_from_cluster_snapshot( ClusterIdentifier=DB_CLUSTER_IDENTIFIER, SnapshotIdentifier=DB_SNAPSHOT_IDENTIFIER, IamRoles=[roleArn]  )
    
    wait_for_cluster(redshift, 'available')  
    
    return redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]

# In[206]:

def delete_snapshot(redshift, snapshot_identifier):
    try:
        redshift.delete_cluster_snapshot(SnapshotIdentifier=snapshot_identifier)
        
        return 'DELETED'
        
    except Exception as e:
        if type(e).__name__ == 'InvalidClusterSnapshotStateFault':
        
            return 'WAIT'
        
        else:
            
            return e
        
        
def delete_snapshots(redshift, snapshot_identifier=None):
    """
    Takes a boto3 redshift client object and, optionally, a snapshot_id 
    
    If a snapshot_id is provided then that snapshot is deleted
    In no snapshot _id is proided then all manually created snapshots of this cluster are deleted
    
    Usage:
    delete_snapshots(<redshift_client>, [snapshot_identifier=None])
    """
    delete_status =''
    
    count_snapshots=len(redshift.describe_cluster_snapshots(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Snapshots'])
    if count_snapshots > 0:
        print(f"Found {count_snapshots} snapshot(s):")
        try:
            if snapshot_identifier != None:
                if check_for_snapshot(redshift):
                    print(f'Deleting snapshot {snapshot_identifier}')
                    while delete_status != 'DELETED':
                        
                        delete_status = delete_snapshot(redshift, snapshot_identifier)
                        if delete_status == 'DELETED':
                            print(f'Snapshot {snapshot_identifier} deleted')
                        elif delete_status == 'WAIT':
                            print(f'Snapshot {snapshot_identifier} in use - waiting')
                            time.sleep(10)
                        else:
                            # Error
                            print(delete_status)
                        
                else:
                    print(f'Snapshot {snapshot_identifier} not present - continuing')
            else:
                
                for snapshot in redshift.describe_cluster_snapshots(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Snapshots']:
                    if snapshot['SnapshotType']=='manual':
                        print(f"Deleting snapshot {snapshot['SnapshotIdentifier']}")
                        while delete_status != 'DELETED':                      
                            delete_status = delete_snapshot(redshift, snapshot['SnapshotIdentifier'])
                            if delete_status == 'DELETED':
                                print(f"Snapshot {snapshot['SnapshotIdentifier']} deleted")
                            elif delete_status == 'WAIT':
                                print(f"Snapshot {snapshot['SnapshotIdentifier']} in use - waiting")
                                time.sleep(10)
                            else:
                                # Error
                                print(delete_status)
                        
                    elif snapshot['SnapshotType']=='automated':
                        print(f"Automated snapshot {snapshot['SnapshotIdentifier']} cannot be manually deleted but will be droped after {DB_SNAPSHOT_RETENTION} day retention period")
                    else:
                        print(f"Found unknown snapshot {snapshot['SnapshotIdentifier']} - taking no action")
        except Exception as e:
            print(e)
#            print(f'Error {type(e).__name__}')

            
def delete_cluster(redshift):
    """
    Deletes the cluster and any manually created snapshots
    
    Usage:
    delete_cluster(<redshift_client>)
    """
    try:
        print(f'Deleting {DB_CLUSTER_IDENTIFIER}')
        redshift.delete_cluster( ClusterIdentifier=DB_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        wait_for_cluster(redshift, 'deleted')
        delete_snapshots(redshift)
    
    except Exception as e:
        
        if type(e).__name__ == 'ClusterNotFoundFault':
            
            print('Cluster {DB_CLUSTER_IDENTIFIER} does not exist')
            
        else:
            
            print(e)

def wait_for_cluster(redshift, desired_status):
    """
    A function to loop and report back tothe user while wiating for the cluster to reach a specified state
    
    Usage:
    wait_for_cluster(<redshift_client>, <desired_status>)
    """

    i=0
    try:
        while redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']!=desired_status:
            print (f"Waiting for cluster to be {desired_status} (status currently \'{redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']}\')")
            time.sleep(10)
            i += 1

#             if i > 30:
#         #       Error
#                 print(f'Error - cluster not {desired_status} after 5 minutes')
#                 exit(1)

        # TODO - handle error better    
        print(f'Cluster {desired_status}')
        
    except Exception as e:
        if type(e).__name__ == 'ClusterNotFoundFault':
            if desired_status == 'deleted':
                print(f'Cluster {DB_CLUSTER_IDENTIFIER} deleted')      
        else:
            print(f'ErrorName= {type(e).__name__}')
            
def prettyRedshiftProps(props):
    """
    Formats redshift properties
    
    Usage:
    prettyRedshiftProps(<redshift_properties>)
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def check_for_snapshot(redshift):
    """
    Function to check whether a snapshot of the cluster exists
    
    Usage:
    check_for_snapshot(redshift)
    """
    print(f'Checking for snapshot {DB_SNAPSHOT_IDENTIFIER}')
    try:
        snapshot_exists = DB_SNAPSHOT_IDENTIFIER in [snapshot['SnapshotIdentifier'] for snapshot in redshift.describe_cluster_snapshots(SnapshotIdentifier=DB_SNAPSHOT_IDENTIFIER)['Snapshots']]
        return snapshot_exists
    except Exception as e:
        print('No snapshots found')

def create_connection(db_endpoint):
    """
    Function to create a connection to a redshift database
    
    Returns a dictionary contianing the connection info
    
    Usage:
    create_connection(<db_endpoint>)
    """
    print("Connecting to Readshift and running queries")
    # Connect to redshift 
    connection = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(db_endpoint, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    cursor = connection.cursor()
    
    return dict(conn = connection, cur = cursor)

def start_cluster(redshift, roleArn):
    """
    Takes a boto3 redshift client object and a role ARN and starts (or restarts) the cluster
    
    If a snapshot exists the cluster is restarted, otherwise a new cluster is created
    and a new DB created and populated
    
    Usage:
    start_cluster(<redshift>, <roleArn>)
    """
    
    if check_for_snapshot(redshift)==True:
        print(f'Resuming cluster operation based on snapshot {DB_SNAPSHOT_IDENTIFIER}')
        myClusterProps = resume_cluster(redshift, roleArn)
    else:
        print(f'No saved instances of {DB_CLUSTER_IDENTIFIER} found - creating a new cluster')
        myClusterProps = create_new_cluster(redshift, roleArn)
        db_endpoint = myClusterProps['Endpoint']['Address']
        db_role_arn = myClusterProps['IamRoles'][0]['IamRoleArn']
        session = create_connection(db_endpoint)
        create_database(session['cur'], session['conn'], db_role_arn)