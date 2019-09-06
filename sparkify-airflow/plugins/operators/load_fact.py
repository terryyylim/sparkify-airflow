from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_query="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        """
        Fact tables are usually so massive that they should only allow
        append type functionality.
        """
        self.log.info('Executing LoadFactOperator!')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Loading {self.table} table into Redshift")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_query
        )
        
        redshift.run(self.sql_query)
        self.log.info(f"Success@load_fact.py: Loaded {self.table} into Redshift")
