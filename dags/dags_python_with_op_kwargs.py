from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from common.common_func import regist2


with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    regist2_t1 = PythonOperator(
        task_id="regist2_t1",
        python_callable=regist2,
        op_args=["hjkim", "man", "kr", "seoul"],
        op_kwargs={"email": "codingToast@bread.com", "phone": "000-0000-0000"},
    )

    regist2_t1
