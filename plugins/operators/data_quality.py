from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    count_sql = """
                SELECT COUNT(*) FROM {}
                """

    @apply_defaults
    def __init__(self,
                 aws_redshift_id='',
                 table_names='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_redshift_id = aws_redshift_id
        self.table_names = table_names


    def execute(self, context):
        self.log.info("Start Data quality checks")
        redshift = PostgresHook(postgres_conn_id = self.aws_redshift_id)
        
        for table in self.table_names:
            records_count = redshift.get_records("SELECT COUNT (*) FROM {}".format(table))
            #format_count_sql = DataQualityOperator.count_sql.format(table)
            #count = redshift.run(format_count_sql)
            
            if len(records_count) < 1:
                self.log.error(f"Failed data quality check for table {table}")
            self.log.info(f"Data quality check finished for table {table} with {records_count} records")
            