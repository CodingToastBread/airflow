from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import closing

from tornado.process import task_id
# https://airflow.apache.org/docs/apache-airflow-providers-postgres/6.0.0/_api/airflow/providers/postgres/index.html

from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator

with DAG(
    dag_id="dags_python_with_hook_bulk_load",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    schedule="0 7 * * *",
    catchup=False,
) as dag:

    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm, file_nm)

    insrt_postgres = PythonOperator(
        task_id="insrt_postgres",
        python_callable=insrt_postgres,
        op_kwargs={
            "postgres_conn_id": "conn-db-postgres-custom",
            "tbl_nm": "GlobalJobCounselLngMmRst_bulk1",
            "file_nm": "/opt/airflow/files/GlobalJobCounselLngMmRst/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}/GlobalJobCounselLngMmRst.csv",
        },
    )

    insrt_postgres
