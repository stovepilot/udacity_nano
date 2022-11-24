

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# Stage Operator
# The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
# The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 file_format = "",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format
    def execute(self, context):
        self.log.info('StageToRedshiftOperator starteggd')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        self.log.info('StageToRedshiftOperator: aws_hook instantiated')       
        
        credentials = aws_hook.get_credentials()
        self.log.info(f'StageToRedshiftOperator: credentials found: {credentials.access_key}')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Clearing data from destination Redshift table `{self.table}` using `DELETE FROM {self.table}`")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.file_format
        )
        self.log.info(f"Running:\n{formatted_sql}")
        redshift.run(formatted_sql)





