from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
It's not a good idea to have table creation as part of main dag, as it is run hourly and it will be updating the already existing tables.
It is better to have table creation as a separate DAG.
"""
class CreateTablesOperator(BaseOperator):
    ui_color = '#358140'
    sql_statement_file='create_tables.sql'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info(f'@CreateRedshiftTableOperator: Creating all tables')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        fd = open(CreateTablesOperator.sql_statement_file, 'r')
        sql_file = fd.read()
        fd.close()

        sql_commands = sql_file.split(';')

        for command in sql_commands:
            if command.rstrip() != '':
                redshift.run(command)
