from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                truncate="",
                sql_query="",
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
        self.sql_query = sql_query

    def execute(self, context):
        """
        Dimension loads are often done with truncate-insert pattern where the target table is emptied
        before the load.
        --------------------
        Records removed using TRUNCATE table is quicker since there is no resource overhead of logging
        the deletions, but records removed this way cannot be restored in a rollback operation.
        """
        self.log.info('Executing LoadDimensionOperator!')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            redshift.run(f'TRUNCATE TABLE {self.table}')
        
        formatted_sql = self.sql_query.format(self.table)
        redshift.run(formatted_sql)

        self.log.info(f"Success@load_dimension.py: Loaded dimensions {self.table} table into Redshift")
