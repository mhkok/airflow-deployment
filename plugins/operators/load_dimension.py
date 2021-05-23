from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

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
        self.log.info("Populate Dimension DATA")
        redshift = PostgresHook(postgres_conn_id = self.aws_redshift_id)
        
        format_insert = LoadDimensionOperator.insert_sql.format(
            self.table_name,
            self.sql
        )
        
        redshift.run(format_insert)
        
        
        self.log.info('Finished Populate Dimension Data for TABLE {}'.format(self.table_name))
