import os
import configparser
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_CREDENTIALS_ID'] = config['AWS']['AWS_CREDENTIALS_ID']
os.environ['S3_BUCKET'] = config['S3']['S3_BUCKET']
os.environ['S3_KEY'] = config['S3']['S3_KEY']
os.environ['REGION'] = config['S3']['REGION']
os.environ['FILE_FORMAT'] = config['S3']['FILE_FORMAT']
os.environ['CONN_ID'] = config['REDSHIFT']['CONN_ID']
os.environ['EVENTS_TABLE'] = config['REDSHIFT']['EVENTS_TABLE']
os.environ['SONGS_TABLE'] = config['REDSHIFT']['SONGS_TABLE']
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'terence',
    'start_date': datetime(2018, 5, 1),
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
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table=os.environ['EVENTS_TABLE'],
    redshift_conn_id=os.environ['CONN_ID'],
    aws_credentials_id=os.environ['AWS_CREDENTIALS_ID'],
    s3_bucket=os.environ['S3_BUCKET'],
    s3_key=os.environ['S3_KEY'],
    region=os.environ['REGION'],
    file_format=os.environ['FILE_FORMAT'],
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table=os.environ['SONGS_TABLE'],
    redshift_conn_id=os.environ['CONN_ID'],
    aws_credentials_id=os.environ['AWS_CREDENTIALS_ID'],
    s3_bucket=os.environ['S3_BUCKET'],
    s3_key=os.environ['S3_KEY'],
    region=os.environ['REGION'],
    file_format=os.environ['FILE_FORMAT'],
    execution_date=start_date
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
