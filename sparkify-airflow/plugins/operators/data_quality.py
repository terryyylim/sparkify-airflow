from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                tables=[],
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Run checks on the data itself. Data should be present if the previous insert
        operator ran successfully.
        """
        self.log.info('Executing DataQualityOperator!')
        redshift = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check failed. {table} returned no results')

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f'Data quality check failed. {table} contained 0 rows')

            self.log.info(f'Data quality on table {table} check passed with {num_records} records')
