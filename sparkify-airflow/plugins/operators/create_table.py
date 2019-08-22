from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateRedshiftTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_query="",
                *args, **kwargs):

        super(CreateRedshiftTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
    
    def execute(self, context):
        """
        Creates table in Redshift. This operator can be combined with Loading operators
        to form a SubDag.
        """
        self.log(f'@CreateRedshiftTableOperator: Creating table {self.table}')
        redshift = PostgresHook(self.redshift_conn_id)

        redshift.run(self.sql_query)
        self.log.info(f"Success@create_table.py: Created table {self.table} on Redshift")
