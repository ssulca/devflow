from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.amazon.aws.operators.s3_bucket import S3CopyObjectOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.version import version
from datetime import datetime, timedelta
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, 'INFO'))
logger.info('Loading Lambda Function {}'.format(__name__))

def route_on_attribute(ti):
    files = ti.xcom_pull(key='return_value', task_ids=['list_3s_files'])
    logger.info(files)
    return files

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('s3client_dag',
         start_date=datetime(2019, 1, 1),
         max_active_runs=3,
         schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = DummyOperator(
        task_id='start'
    )

    t1 = S3ListOperator(
        task_id='list_3s_files',
        bucket='datalake-nonprod-raw',
        prefix='S3Upload/dwh5013-prefijos',
        delimiter='/',
        aws_conn_id='my_aws'
    )

    t2 = PythonOperator(
        task_id=f'python_files',
        python_callable=route_on_attribute
    )

    t3 = DummyOperator(
        task_id='end'
    )
    # t2 = S3CopyObjectOperator(
    #     source_bucket_key='source_file',
    #     dest_bucket_key='rfmtest',
    #     aws_conn_id='my_aws',
    #     source_bucket_name='source-bucket',
    #     dest_bucket_name='dest-bucket'
    # )

    t0 >> t1 >> t2 >> t3
