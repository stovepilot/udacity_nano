import configparser
import sql_queries


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


    
def create_database(cur, conn, db_role_arn):
    """
    Runs the functions to create and populate the database
    
    Usage:
    create_database(<cursor>, <connection>, <db_role_arn>)
    """

    # Run queries to create and populate db
    create_tables(cur, conn, db_role_arn)
    copy_to_dim(cur, conn, db_role_arn)
    copy_to_fact(cur, conn, db_role_arn)

def create_tables(cur, conn, dbRoleArn):
    """
    Runs the SQL to create the database
    
    Usage:
    create_tables(<cursor>, <connection>, <db_role_arn>)
    """
    print('Creating tables')
    for query in sql_queries.create_table_queries:
        cur.execute(query.format(dbRoleArn))
        conn.commit()

def copy_to_dim(cur, conn, dbRoleArn):
    """
    Runs the SQL to ingest the dimension data
    
    Usage:
    copy_to_dim(<cursor>, <connection>, <db_role_arn>)
    """
    print('Populating dimension tables')
    for query in sql_queries.copy_to_dim_queries:
        print(query.format(dbRoleArn))
        cur.execute(query.format(dbRoleArn))
        conn.commit()     

def copy_to_fact(cur, conn, dbRoleArn):
    """
    Runs the SQL to ingest the fact data
    
    Usage:
    copy_to_fact(<cursor>, <connection>, <db_role_arn>)
    """
    print('Populating fact tables')
    for query in sql_queries.copy_to_fact_queries:
        print(query.format(dbRoleArn))
        cur.execute(query.format(dbRoleArn))
        conn.commit()    

def drop_tables(cur, conn, dbRoleArn):
    """
    Runs the SQL to drop the tables
    
    Usage:
    drop_tables(<cursor>, <connection>, <db_role_arn>)
    """
    print('Dropping tables')
    for query in sql_queries.drop_dim_table_queries:
        print(query.format(dbRoleArn))
        cur.execute(query.format(dbRoleArn))
        conn.commit()  
