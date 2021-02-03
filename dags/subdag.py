import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import DataQualityOperator

#creating subdag for quality check to carry our common checks
def function_quality_check(
        
        parent_dag_name,
        task_id,
        redshift_conn_id,
    
        table1,
        query1,
    
        table2,
        query2,
    
        table3,
        query3,
        
        table4,
        query4,
    
        table5,
        query5,
    
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    data_quality_songs_task = DataQualityOperator(
        task_id=f"{table1}_quality_check",
        dag=dag,
        table=table1,
        redshift_conn_id=redshift_conn_id,
        check_query= query1
        
    )
    data_quality_artist_task = DataQualityOperator(
        task_id=f"{table2}_quality_checkt",
        dag=dag,
        table=table2,
        redshift_conn_id=redshift_conn_id,
        check_query= query2
        
    )
    
    data_quality_users_task = DataQualityOperator(
        task_id=f"{table3}_quality_checkt",
        dag=dag,
        table=table3,
        redshift_conn_id=redshift_conn_id,
        check_query= query3
        
    )
    data_quality_time_task = DataQualityOperator(
        task_id=f"{table4}_quality_check",
        dag=dag,
        table=table4,
        redshift_conn_id=redshift_conn_id,
        check_query= query4
        
    )
    
    data_quality_songplay_task = DataQualityOperator(
        task_id=f"{table5}_quality_check",
        dag=dag,
        table=table5,
        redshift_conn_id=redshift_conn_id,
        check_query= query5
        
    )
    
    return dag


#creating subdag for craeting tables to carry our common sql commands
def function_create_table(
        parent_dag_name,
        task_id,
        redshift_conn_id,
    
        table1,
        query1,
    
        table2,
        query2,
    
        table3,
        query3,
        
        table4,
        query4,
    
        table5,
        query5,
    
        table6,
        query6,
    
        table7,
        query7,
    
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
        
    create_table_song_staging_task = PostgresOperator(
        task_id=f"create_table_{table1}",
        dag=dag,
        #table=table1,
        postgres_conn_id=redshift_conn_id,
        sql= query1
        
    )
        
    create_table_event_staging_task = PostgresOperator(
        task_id=f"create_table_{table2}",
        dag=dag,
        #table=table2,
        postgres_conn_id=redshift_conn_id,
        sql= query2
        
    )  
    create_table_songs_task = PostgresOperator(
        task_id=f"create_table_{table3}",
        dag=dag,
        #table=table3,
        postgres_conn_id=redshift_conn_id,
        sql= query3
        
    )
    create_table_artist_task = PostgresOperator(
        task_id=f"create_table_{table4}",
        dag=dag,
        #table=table4,
        postgres_conn_id=redshift_conn_id,
        sql= query4
        
    )
    
    create_table_users_task = PostgresOperator(
        task_id=f"create_table_{table5}",
        dag=dag,
        #table=table5,
        postgres_conn_id=redshift_conn_id,
        sql= query5
        
    )
    create_table_time_task = PostgresOperator(
        task_id=f"create_table_{table6}",
        dag=dag,
        #table=table6,
        postgres_conn_id=redshift_conn_id,
        sql= query6
        
    )
    
    create_table_songplay_task = PostgresOperator(
        task_id=f"create_table_{table7}",
        dag=dag,
        #table=table7,
        postgres_conn_id=redshift_conn_id,
        sql= query7
        
    )
  
    return dag