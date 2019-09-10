from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
        COMMIT;
    """
    
    delete_sql = """
        DELETE FROM {}
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_query="",
                append_data="",
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_data = append_data

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

        self.log.info(f"Loading dimension table {self.table} into Redshift")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.table,
            self.sql_query
        )
        if self.append_data:
            redshift.run(formatted_sql)
        else:
            del_sql = LoadDimensionOperator.delete_sql.format(
                self.table,
            )
            redshift.run(del_sql)
            redshift.run(formatted_sql)

        self.log.info(f"Success@load_dimension.py: Loaded dimension table {self.table} into Redshift")
