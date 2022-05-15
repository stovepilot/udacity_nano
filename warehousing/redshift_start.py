#!/usr/bin/env python
# coding: utf-8

# In[67]:


import pandas as pd
import boto3
import json
import time


# # Load DWH Params from a file

# In[68]:


import configparser
config = configparser.ConfigParser()
config.read_file(open('/home/workspace/dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DB_CLUSTER_TYPE        = config.get("CLUSTER","DB_CLUSTER_TYPE")
DB_NUM_NODES           = config.get("CLUSTER","DB_NUM_NODES")
DB_NODE_TYPE           = config.get("CLUSTER","DB_NODE_TYPE")

DB_CLUSTER_IDENTIFIER  = config.get("CLUSTER","DB_CLUSTER_IDENTIFIER")
DB_NAME                = config.get("CLUSTER","DB_NAME")
DB_USER                = config.get("CLUSTER","DB_USER")
DB_PASSWORD         = config.get("CLUSTER","DB_PASSWORD")
DB_PORT                = config.get("CLUSTER","DB_PORT")

IAM_ROLE_NAME          = config.get("IAM_ROLE", "IAM_ROLE_NAME")
ARN                    = config.get("IAM_ROLE", "ARN")

(DB_USER, DB_PASSWORD, DB_NAME)

pd.DataFrame({"Param":
                  ["DB_CLUSTER_TYPE", "DB_NUM_NODES", "DB_NODE_TYPE", "DB_CLUSTER_IDENTIFIER", 
                   "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT", "IAM_ROLE_NAME","ARN"],
              "Value":
                  [DB_CLUSTER_TYPE, DB_NUM_NODES, DB_NODE_TYPE, DB_CLUSTER_IDENTIFIER, 
                   DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, IAM_ROLE_NAME, ARN],
             })


# # Create clients for IAM, EC2, S3 and Redshift
# **Note**: We are creating these resources in the the **us-west-2** region. Choose the same region in the your AWS web console to the see these resources.

# In[69]:


import boto3

ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )


# # STEP 1: IAM ROLE
# - Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

# In[70]:


from botocore.exceptions import ClientError

#1.1 Create the role, 
try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
    
print("1.2 Attaching Policy")

iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)


# # STEP 2:  Redshift Cluster
# 
# - Create a [RedShift Cluster](https://console.aws.amazon.com/redshiftv2/home)
# - For complete arguments to `create_cluster`, see [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.create_cluster)

# In[71]:
print("1.4 Creating a Redshift cluster")

try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DB_CLUSTER_TYPE,
        NodeType=DB_NODE_TYPE,
        NumberOfNodes=int(DB_NUM_NODES),

        #Identifiers & Credentials
        DBName=DB_NAME,
        ClusterIdentifier=DB_CLUSTER_IDENTIFIER,
        MasterUsername=DB_USER,
        MasterUserPassword=DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)


# ## 2.1 *Describe* the cluster to see its status
# - run this block several times until the cluster status becomes `Available`

# In[ ]:


### Loop while waiting for the cluster to beome available


# In[ ]:





# In[77]:


i=0
while redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']!='available':
    print ('Waiting for cluster to become available')
    time.sleep(10)
    i += 1
    
    if i > 30:
#       Error
        print('Error - cluster not available after 5 minutes')
        exit(1)
        
# TODO - handle error better    
print('Cluster Available')    


# In[ ]:


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


# In[78]:


# <h2> 2.2 Take note of the cluster endpoint and role ARN </h2>
# <font color='red'>DO NaOT RUN THIS unless the cluster status becomes "Available". Make sure you are checking your Amazon Redshift cluster in the **us-west-2** region. </font>
# 

# In[79]:

# Wait a few seconds to avoid errors reading the endpoint details
time.sleep(10)

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)


DB_ENDPOINT = myClusterProps['Endpoint']['Address']
DB_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DB_ENDPOINT :: ", DB_ENDPOINT)
print("DB_ROLE_ARN :: ", DB_ROLE_ARN)

# ## STEP 3: Open an incoming  TCP port to access the cluster ednpoint

# In[80]:

print ('Opening an incoming  TCP port to access the cluster endpoint')

try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[1]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DB_PORT),
        ToPort=int(DB_PORT)
    )
except Exception as e:
    print(e)




