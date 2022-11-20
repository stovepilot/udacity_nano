

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):

        self.log.info(f"Connecting to Redshift")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            
        for key, value in self.tables.items():
            self.log.info(f"Running DQ checks against '{key}'")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {key}")
            self.log.info(f"Running 'SELECT COUNT(*) FROM {key}'")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check on table {key} failed. Test returned no results")
            num_records = records[0][0]
            if num_records < value:
                raise ValueError(f"Data quality check on {key} failed. Test returned {num_records} rows when {value} were expected")
            self.log.info(f"DQ check passed on '{key}' - {num_records} records found.")
        
        
        
        