from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

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
        self.log.info("Populate FACT data")
        redshift = PostgresHook(postgres_conn_id = self.aws_redshift_id)
        
        format_insert = LoadFactOperator.insert_sql.format(
            self.table_name,
            self.sql
        )
        
        redshift.run(format_insert)
        
        
        self.log.info('Populate FACT data finished')
