from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'matthijs.kok',
    'start_date': datetime(2021, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    table_name='staging_events',
    s3_bucket='s3://udacity-dend/log_data',
    s3_key='log_data',
    aws_redshift_id='aws_redshift_dwh',
    aws_creds='aws_assume_role_creds'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    table_name='staging_songs',
    s3_bucket='s3://udacity-dend/song_data/A/A/A',
    s3_key='song_data',
    aws_redshift_id='aws_redshift_dwh',
    aws_creds='aws_assume_role_creds'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    aws_redshift_id='aws_redshift_dwh',
    table_name = 'songplays',
    sql = SqlQueries.songplay_table_insert,
    operation = 'insert'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    aws_redshift_id='aws_redshift_dwh',
    table_name = 'users',
    sql=SqlQueries.user_table_insert,
    operation = 'insert'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    aws_redshift_id='aws_redshift_dwh',
    table_name = 'songs',
    sql=SqlQueries.song_table_insert,
    operation = 'insert'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    aws_redshift_id='aws_redshift_dwh',
    table_name = 'artists',
    sql=SqlQueries.artist_table_insert,
    operation = 'insert'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    aws_redshift_id='aws_redshift_dwh',
    table_name = 'time',
    sql=SqlQueries.time_table_insert,
    operation = 'truncate'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    aws_redshift_id='aws_redshift_dwh',
    table_names=['songplays', 'users', 'songs', 'artists', 'time'],
    dq_checks=[{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
               {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result':0},
    	       {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result':0},
    	       {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result':0},
    	       {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result':0}]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
