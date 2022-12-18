import configparser
import boto3
from redshift import *

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


pause_cluster(redshift)