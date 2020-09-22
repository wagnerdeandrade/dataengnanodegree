from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 query,
                 redshift_conn_id='redshift',
                 verify=None,
                 autocommit=False,
                 parameters=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.verify = verify
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        """
            Truncate the target table removing previous dag execution data.
            Using the staging tables, populate the target fact table.
        """

        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Delete data from target Redshift FACT table" + self.table)
        self.hook.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info("Table" + self.table + "is clean...")
                      
        self.log.info("Populating FACT table {}... ".format(self.table))
        self.log.info("Executing QUERY ... " + self.query)
        self.hook.run(self.query, self.autocommit)
        self.log.info("Done, table {} is populated.".format(self.table))        
 