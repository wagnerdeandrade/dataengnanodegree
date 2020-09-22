from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table,
                 s3_bucket,
                 s3_key,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 verify=None,
                 copy_options=tuple(),
                 autocommit=False,
                 parameters=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        credentials = self.s3.get_credentials()
        

        self.log.info("Delete data from target Redshift STAGING table" + self.table)
        self.hook.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info("Table" + self.table + "is clean...")

        copy_options = ' '.join(self.copy_options)
        copy_query = SqlQueries.staging_copy.format(
                               table=self.table,
                               s3_bucket=self.s3_bucket,
                               s3_key=self.s3_key,
                               access_key=credentials.access_key,
                               secret_key=credentials.secret_key,
                               copy_options=copy_options)
        
        self.log.info("Populating STAGING table {}... ".format(self.table))
        self.log.info("Executing COPY command... " + copy_query)
        self.hook.run(copy_query, self.autocommit)
        self.log.info("Done, table {} is populated.".format(self.table))        


                      
 




