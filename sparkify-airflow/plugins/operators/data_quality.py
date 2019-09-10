from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                tables=[],
                dq_checks=[],
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        """
        Run checks on the data itself. Data should be present if the previous insert
        operator ran successfully.
        """
        self.log.info('Executing DataQualityOperator!')
        redshift = PostgresHook(self.redshift_conn_id)
        error_count = 0
        failing_tests = []
        
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            table = check.get('table')

            records = redshift.get_records(sql)[0]

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append((table, sql))

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError(f'Data quality check failed')
        else:
            self.log.info(f'Data quality check on all tables passed')
