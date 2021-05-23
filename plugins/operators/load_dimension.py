from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This class loads the dimension tables as described in the README file
    Initially it loads the input parameters from the DAG and then inserts the actual data into the tables
    """
    ui_color = '#80BD9E'
    
    insert_sql = """
                 INSERT INTO {} {}
                 """

    @apply_defaults
    def __init__(self,
                 aws_redshift_id='',
                 sql='',
                 table_name='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.aws_redshift_id = aws_redshift_id
        self.table_name = table_name
        self.sql = sql

    def execute(self, context):
        """
        This function loads the data into the dimension tables. It requires the Redshift ID, Table name & SQL statements.
        """
        self.log.info("Populate Dimension DATA")
        redshift = PostgresHook(postgres_conn_id = self.aws_redshift_id)
        
        format_insert = LoadDimensionOperator.insert_sql.format(
            self.table_name,
            self.sql
        )
        
        redshift.run(format_insert)
        
        self.log.info('Finished Populate Dimension Data for TABLE {}'.format(self.table_name))
