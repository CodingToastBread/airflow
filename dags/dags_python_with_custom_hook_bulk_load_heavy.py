from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from hooks.custom_postgres_hook_advance import CustomPostgresAdvanceHook


with DAG(
    dag_id='dags_python_with_custom_hook_bulk_load_222',
    start_date=pendulum.datetime(2025, 6, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresAdvanceHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    insrt_postgres = PythonOperator(
        task_id="insrt_postgres",
        python_callable=insrt_postgres,
        op_kwargs={
            "postgres_conn_id": "conn-db-postgres-custom",
            "tbl_nm": "AL_D161_11",
            'file_nm': "/opt/airflow/files/csv_sample/AL_D161_11_20250607.csv",
        },
    )
