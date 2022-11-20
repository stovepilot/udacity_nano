

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_select="",
                 append="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.table_name= table_name
        self.sql_select= sql_select
        self.append = append

    def execute(self, context):
        
        self.log.info('Load Operator: connecting to Redshift')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        sql_insert = f"""
            INSERT INTO {self.table_name}
            {self.sql_select}
        """        
        
        if self.append == True:
            
            self.log.info(f'Appending data to dimension table {self.table_name}')

            redshift.run(sql_insert)

        else:

            self.log.info(f'Truncating and re-inserting data into dimension table {self.table_name}.')
            
            sql_delete = "DELETE FROM {}".format(self.table_name)

            redshift.run(sql_delete)

            redshift.run(sql_insert)
