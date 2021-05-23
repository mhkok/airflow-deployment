from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    This class stages the S3 data into the DWH REDSHIFT
    Based on input parameters from the DAG it'll run the specific commands required to import the data.
    Two specific SQL statements are defined to get COPY the data from S3 to DWH Redshift. 
    """
    ui_color = '#358140'
    
    copy_sql_songs =  """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                format as JSON 'auto'
                """
    
    copy_sql_events = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                FORMAT as JSON 's3://udacity-dend/log_json_path.json'
                """
    
    @apply_defaults
    def __init__(self,
                 aws_redshift_id='',
                 aws_creds='',
                 table_name='',
                 s3_bucket='',
                 s3_key='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_redshift_id = aws_redshift_id
        self.aws_credentials = aws_creds
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        """
        This function stages the s3 data based on their s3 key in the DWH REDSHIFT
        This requires:
        - the aws creds
        - Redshift connection
        """
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.aws_redshift_id)
        
        self.log.info("Delete old tables")
        redshift.run("DELETE FROM {}".format(self.table_name))
                       
        if self.s3_key == 'log_data':
            format_copy = StageToRedshiftOperator.copy_sql_events.format(
                self.table_name,
                self.s3_bucket,
                credentials.access_key,
                credentials.secret_key)
            redshift.run(format_copy)
            self.log.info("Copy Event data finished")
           
        elif self.s3_key == 'song_data':
            format_copy = StageToRedshiftOperator.copy_sql_songs.format(
                self.table_name,
                self.s3_bucket,
                credentials.access_key,
                credentials.secret_key)
            
            redshift.run(format_copy)           
            self.log.info("Copy Song data finished")
        
        else: 
            self.log.info("Wrong s3 key entered")
