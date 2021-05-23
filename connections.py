from airflow import settings
from airflow.models import Connection

aws_credentials = Connection(
        conn_id='aws_assume_role_creds',
        conn_type='Amazon Web Services',
        host='',
        login='',
        password=''
        #port=''
)
redshift = Connection(
        conn_id='aws_redshift_dwh',
        conn_type='Postgres',
        host='dwh-cluster.crgvp58iyzhl.us-west-2.redshift.amazonaws.com',
        login='dwhuser',
        password='',
        port='5439',
        schema='dwh'
)
session = settings.Session()
session.add(aws_credentials)
session.add(redshift)
session.commit()