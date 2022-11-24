

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from helpers import SqlQueries
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),    
}
dag = DAG('udac_example_dag',
          default_args=default_args,
          catchup=False,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )
def create_tables():
    redshift = PostgresHook("redshift")
    sql_create_tables = open("/home/workspace/airflow/create_tables.sql","r").read()
    redshift.run(sql_create_tables)
#     self.logging.info("Tables created in Redshift")
    
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
create_tables = PythonOperator(task_id = "create_redshift_tables",
                               dag = dag,
                               python_callable = create_tables
                              )
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_events',
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
#    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}",
    s3_key="log_data",
    file_format="json 's3://udacity-dend/log_json_path.json'",
    region="us-west-2",
    provide_context=True,
    execution_date='start_date'
)
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table='public.staging_songs',
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    file_format="json 'auto'",
    region="us-west-2",
)
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songplays',
    sql_select=SqlQueries.songplay_table_insert,
    append='True'
    
)
load_user_dimension_table = LoadFactOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='users',
    sql_select=SqlQueries.user_table_insert,
    append='False'
)
load_song_dimension_table = LoadFactOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songs',
    sql_select=SqlQueries.song_table_insert,
    append='False'
)
load_artist_dimension_table = LoadFactOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='artists',
    sql_select=SqlQueries.artist_table_insert,
    append='False'
)
load_time_dimension_table = LoadFactOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='time',
    sql_select=SqlQueries.time_table_insert,
    append='False'
)
# run_staging_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag,
#     redshift_conn_id='redshift',
#     tables={"staging_events":8026, 
#             "staging_songs": 1}, 
# )
# run_transformation_quality_checks = DataQualityOperator(
#     task_id='Run_transformation_quality_checks',
#     dag=dag,
#     redshift_conn_id='redshift',
#     tables={"songplays":1, 
#             "users": 1,
#             "songs": 1,
#             "artists": 1,
#             "time": 1}, 
# )
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
start_operator >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
# stage_events_to_redshift >> run_staging_quality_checks 
# stage_songs_to_redshift >> run_staging_quality_checks
# run_staging_quality_checks >> load_songplays_table 
stage_events_to_redshift >> load_songplays_table 
stage_songs_to_redshift >> load_songplays_table
# run_staging_quality_checks >> load_songplays_table 
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> end_operator
load_artist_dimension_table >> end_operator
load_song_dimension_table >> end_operator
load_time_dimension_table >> end_operator
#run_transformation_quality_checks >> end_operator
