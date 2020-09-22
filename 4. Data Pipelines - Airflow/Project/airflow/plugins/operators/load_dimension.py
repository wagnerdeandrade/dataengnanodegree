from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 query,
                 redshift_conn_id='redshift',
                 verify=None,
                 copy_options=tuple(),
                 autocommit=False,
                 parameters=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.verify = verify
        self.autocommit = autocommit
        self.parameters = parameters


    def execute(self, context):
        """
        Truncate the target table removing previous dag execution data. 
        Using the staging tables, populate the target dimension table.
        """

        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Delete data from target Redshift DIMENSION table" + self.table)
        self.hook.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info("Table" + self.table + "is clean...")

        
        self.log.info("Populating DIMENSION table {}... ".format(self.table))
        self.log.info("Executing QUERY ... " + self.query)
        self.hook.run(self.query, self.autocommit)
        self.log.info("Done, table {} is populated.".format(self.table))