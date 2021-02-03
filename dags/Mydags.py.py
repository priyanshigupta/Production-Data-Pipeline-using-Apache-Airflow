from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.subdag_operator import SubDagOperator
from subdag import function_quality_check

from subdag import function_create_table


start_date=datetime(2019, 1, 12)
default_args = {
    'owner': 'Priyanshi',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

#creating dags to execute operations
dag = DAG('Airflow_Project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

#dummy start task
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# Task to load events data from AWS S3 bucket to Redshift cluster
stage_events_to_redshift= StageToRedshiftOperator(
    task_id="Stage_logs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/",
    table="staging_events",
    region="us-west-2",
    json_format = "s3://udacity-dend/log_json_path.json"
    

)
# Task to load songs data from AWS S3 bucket to Redshift cluster
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/",
    table="staging_songs",
    region="us-west-2",
    json_format = "auto"

)

# Load songplays fact from stage
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql = "songplay_table_insert",
    table = "songplays"
    
)

# Load user dimension from stage
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql = "user_table_insert",
    table = "users"
    
)

# Load song dimension from stage
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql = "song_table_insert",
    table = "songs"
)

# Load artist dimension from stage
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql = "artist_table_insert",
    table = "artists"
)

# Load time dimension from stage
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql = "time_table_insert",
    table = "time"
     )




# calling subdag task
task_id="quality_check_subdag"
quality_subdag_task = SubDagOperator(
    subdag=function_quality_check(
        "Airflow_Project",
        task_id,
        "redshift",

        "songs",
        """ SELECT MAX(count) FROM (
            SELECT songid ,title ,artistid ,year ,duration, COUNT(*) as count
            FROM songs
            GROUP BY songid ,title ,artistid ,year ,duration )a""",
        
        "artists",
        """  SELECT MAX(count) FROM (
            SELECT artistid,name,location,lattitude,longitude, COUNT(*) as count
            FROM artists
            GROUP BY artistid,name,location,lattitude,longitude )a""",
        
        "users",
        """   SELECT MAX(count) FROM (
            SELECT userid,first_name,last_name,gender,level, COUNT(*) as count
            FROM users
            GROUP BY userid,first_name,last_name,gender,level )a""",
        
        "time",
        """    SELECT MAX(count) FROM (
            SELECT start_time,hour,day,week,month,year,weekday, COUNT(*) as count
            FROM time
            GROUP BY start_time,hour,day,week,month,year,weekday )a""",
        
        "songplays",
        """ SELECT MAX(count) FROM (
            SELECT playid  ,start_time ,userid ,level,songid ,artistid ,sessionid,location ,user_agent ,COUNT(*) as count
            FROM songplays GROUP BY playid  ,start_time ,userid ,level,songid ,artistid ,sessionid,location ,user_agent  )a""",
        start_date=start_date
        
),
    task_id=task_id,
    dag=dag
)

# calling subdag task
task_id="create_tables_subdag"
create_table_subdag_task = SubDagOperator(
    subdag=function_create_table(
        "Airflow_Project",
        task_id,
        "redshift",

        "staging_songs",
        SqlQueries.create_table_staging_songs,
        
        "staging_events",
        SqlQueries.create_table_staging_events,
        
        "songs",
        SqlQueries.create_table_songs,
        
        "artists",
        SqlQueries.create_table_artists,
        
        "users",
        SqlQueries.create_table_users,
        
        "time",
        SqlQueries.create_table_time,
        
        "songplays",
        SqlQueries.create_table_songplays,
        
        start_date=start_date
        
),
    task_id=task_id,
    dag=dag
)





end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# craeting DAG dependency
start_operator >> create_table_subdag_task
create_table_subdag_task >> stage_events_to_redshift >> load_songplays_table
create_table_subdag_task >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> quality_subdag_task
load_songplays_table >> load_song_dimension_table >> quality_subdag_task
load_songplays_table >> load_artist_dimension_table >> quality_subdag_task
load_songplays_table >> load_time_dimension_table >> quality_subdag_task
quality_subdag_task >> end_operator


