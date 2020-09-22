from datetime import datetime, timedelta
import os
from helpers.sql_queries import SqlQueries
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

"""
This DAG flow is like

                                                                      load_user_dimension_table  \
                 - stage_events_to_redshift -                       / 
                /                            \                      - load_song_dimension_table   -
start_operator -                               load_songplays_table                                 run_quality_checks - end_operator
                \                            /                      - load_artist_dimension_table -
                 - stage_songs_to_redshift -                        \ 
                                                                      load_time_dimension_table   /

- Its responsibility is to loading S3 Sparkify application data,
populate an OLAP Star Schema datawarehouse To further analyitical usage
"""

"""
Dag configuration elements
"""
default_args = {
    'owner': 'wagner',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

"""
Dag representative object, it will be populated with below tasks
"""
dag = DAG('sparkify_dw_populate_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


"""
-------------------------------------------------
Creating staging tables on Sparkify datawarehouse
-------------------------------------------------
"""

"""
staging_events

    Populate staging_events Redshift table using S3
    log_data information from public bucket s3://udacity-dend/log_data
    that contains Sparkify application user activity data.
"""
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    copy_options=(
                  'JSON', '\'s3://udacity-dend/log_json_path.json\'',
                  'timeformat', 'as', '\'epochmillisecs\'', 
                  'TRUNCATECOLUMNS', 'BLANKSASNULL', 'EMPTYASNULL'
                 ),
    dag=dag
)

"""
staging_songs

    Populate staging_songs Redshift table using S3
    log_data information from public bucket s3://udacity-dend/song_data
    that contains Sparkify application song database information.
"""
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    copy_options=('JSON', '\'auto\''),
    dag=dag
)

"""
-------------------------------------------
Creating an OLAP Star Schema datawarehouse
-------------------------------------------
"""

"""
Populating FACT table - songplays
"""
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table='songplays',
    query=SqlQueries.songplay_table_insert,
    dag=dag
)

"""
Populating DIMENTION tables - users, songs, artists, time
"""
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table='users',
    query = SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table='songs',
    query = SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table='artists',
    query = SqlQueries.artist_table_insert,
    dag=dag
)


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table='time',
    query = SqlQueries.time_table_insert,
    dag=dag
)

"""
Checking if OLAP datawarehouse Star Schema tables were 
correctly populated
"""
run_quality_checks = DummyOperator(
    task_id='Run_data_quality_checks',
    tables=('songplays','songs','users','artists','time'),
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


"""
Defining the DAG flow order
"""
start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
