import pendulum
from common.common_func import get_sftp

from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    task_get_sftp = PythonOperator(task_id="task_get_sftp", python_callable=get_sftp)
