from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This class loads the fact table as described in the README file
    Initially it loads the input parameters from the DAG and then inserts the actual data into the tables
    """
    ui_color = '#F98866'
    
    insert_sql = """
                 INSERT INTO {} {}
                 """
    
    @apply_defaults
    def __init__(self,
                 aws_redshift_id='',
                 sql='',
                 table_name='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_redshift_id = aws_redshift_id
        self.table_name = table_name
        self.sql = sql

    def execute(self, context):
        """
        This function loads the data into the fact table. It requires the Redshift ID, Table name & SQL statement.
        """
        self.log.info("Populate FACT data")
        redshift = PostgresHook(postgres_conn_id = self.aws_redshift_id)
        
        format_insert = LoadFactOperator.insert_sql.format(
            self.table_name,
            self.sql
        )
        
        redshift.run(format_insert)
        
        
        self.log.info('Populate FACT data finished')
