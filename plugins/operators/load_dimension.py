from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This class loads the fact table as described in the README file
    Initially it loads the input parameters from the DAG and then inserts the actual data into the tables
    """
    ui_color = '#F98866'
    
    insert_sql = """
                 INSERT INTO {} {}
                 """
    
    delete_sql = """
                 DELETE FROM {}
                 """

    @apply_defaults
    def __init__(self,
                 aws_redshift_id='',
                 sql='',
                 table_name='',
                 operation='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.aws_redshift_id = aws_redshift_id
        self.table_name = table_name
        self.sql = sql
        self.operation = operation

    def execute(self, context):
        """
        This function loads the data into the fact table. It requires the Redshift ID, Table name & SQL statement.
        Depending on the operation defined in the DAG it will first delete the table and then insert data OR
        append the data in the table
        """
        redshift = PostgresHook(postgres_conn_id = self.aws_redshift_id)
        
        if self.operation == 'insert':
            self.log.info("INSERT OPERATION: Populate Dimension DATA")
            format_insert = LoadDimensionOperator.insert_sql.format(
                self.table_name,
                self.sql)
       
            redshift.run(format_insert)
            self.log.info('Finished Populate Dimension Data for TABLE {}'.format(self.table_name))
        
        elif self.operation == 'truncate':
           self.log.info("TRUNCATE OPERATION: DELETE Dimension DATA First")
           format_delete = LoadDimensionOperator.delete_sql.format(self.table_name)
           
           self.log.info("Populate Dimension DATA")
           format_insert = LoadDimensionOperator.insert_sql.format(
                self.table_name,
                self.sql)
            
           redshift.run(format_delete)
           redshift.run(format_insert)
        
           self.log.info('Finished Populate Dimension Data for TABLE {}'.format(self.table_name))



