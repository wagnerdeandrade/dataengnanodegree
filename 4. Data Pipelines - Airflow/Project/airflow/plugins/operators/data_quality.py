from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 tables=tuple(),
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables  
    
    def execute(self, context):

        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            self.log.info('Starting DataQualityOperator for {table}'.format(table))

            records = self.hook.get_records("SELECT COUNT(*) FROM {table}".format(table))
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Failed. {table} is empty".format(table))
      