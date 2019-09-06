import os
import configparser
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator) #CreateTablesOperator
from helpers import SqlQueries


config = configparser.ConfigParser()
config.read('./dwh.cfg')

os.environ['AWS_CREDENTIALS_ID'] = config['AWS']['AWS_CREDENTIALS_ID']
os.environ['S3_BUCKET'] = config['S3']['S3_BUCKET']
os.environ['EVENTS_S3_KEY'] = config['S3']['EVENTS_S3_KEY']
os.environ['SONGS_S3_KEY'] = config['S3']['SONGS_S3_KEY']
os.environ['EVENTS_JSON_PATH'] = config['S3']['EVENTS_JSON_PATH']
os.environ['SONGS_JSON_PATH'] = config['S3']['SONGS_JSON_PATH']
os.environ['FILE_FORMAT'] = config['S3']['FILE_FORMAT']
os.environ['CONN_ID'] = config['REDSHIFT']['CONN_ID']
os.environ['STAGING_EVENTS_TABLE'] = config['REDSHIFT']['STAGING_EVENTS_TABLE']
os.environ['STAGING_SONGS_TABLE'] = config['REDSHIFT']['STAGING_SONGS_TABLE']
os.environ['SONGPLAYS_TABLE'] = config['REDSHIFT']['SONGPLAYS_TABLE']
os.environ['SONGS_TABLE'] = config['REDSHIFT']['SONGS_TABLE']
os.environ['USERS_TABLE'] = config['REDSHIFT']['USERS_TABLE']
os.environ['ARTISTS_TABLE'] = config['REDSHIFT']['ARTISTS_TABLE']
os.environ['TIME_TABLE'] = config['REDSHIFT']['TIME_TABLE']

default_args = {
    'owner': 'terence',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

start_date = datetime.utcnow()

"""
max_active_runs: defines how many running concurrent instances of a DAG there are allowed to be
"""
dag = DAG('sparkify_dend_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False,
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create_redshift_tables = CreateTablesOperator(
#     task_id='Create_tables',
#     dag=dag,
#     redshift_conn_id=os.environ['CONN_ID']
# )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table=os.environ['STAGING_EVENTS_TABLE'],
    redshift_conn_id=os.environ['CONN_ID'],
    aws_credentials_id=os.environ['AWS_CREDENTIALS_ID'],
    s3_bucket=os.environ['S3_BUCKET'],
    s3_key=os.environ['EVENTS_S3_KEY'],
    json_path=os.environ['EVENTS_JSON_PATH'],
    file_format=os.environ['FILE_FORMAT'],
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table=os.environ['STAGING_SONGS_TABLE'],
    redshift_conn_id=os.environ['CONN_ID'],
    aws_credentials_id=os.environ['AWS_CREDENTIALS_ID'],
    s3_bucket=os.environ['S3_BUCKET'],
    s3_key=os.environ['SONGS_S3_KEY'],
    json_path=os.environ['SONGS_JSON_PATH'],
    file_format=os.environ['FILE_FORMAT'],
    execution_date=start_date
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=os.environ['CONN_ID'],
    table=os.environ['SONGPLAYS_TABLE'],
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=os.environ['CONN_ID'],
    table=os.environ['USERS_TABLE'],
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=os.environ['CONN_ID'],
    table=os.environ['SONGS_TABLE'],
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=os.environ['CONN_ID'],
    table=os.environ['ARTISTS_TABLE'],
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=os.environ['CONN_ID'],
    table=os.environ['TIME_TABLE'],
    sql_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=os.environ['CONN_ID'],
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator