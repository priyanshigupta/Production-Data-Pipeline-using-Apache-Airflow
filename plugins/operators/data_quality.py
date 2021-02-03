from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 check_query="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table= table
        #self.table_key=table_key
        self.check_query=check_query

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        

        records = redshift_hook.get_records(self.check_query)
        self.log.info("CHECK QUERY : %s" % self.check_query)
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} contains no rows")

        num_records = records[0][0]
        
        if num_records > 1:
            self.log.error(f"(ERROR):Data quality check failed. {self.table} contains duplicate records")
        else:    
            self.log.info(f"Data quality on table {self.table} check passed with 0 Duplicate records")
        