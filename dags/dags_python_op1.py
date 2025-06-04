from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from common.common_func import good_bye_world


with DAG(
    dag_id="dags_python_op1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    task_t1 = PythonOperator(
        task_id="task_t1",
        python_callable=good_bye_world,
        op_args=['coding_toast', 20, 'arg sample1'],
        op_kwargs={'key_sample1':'value_sample1'}
    )
    
    task_t1
