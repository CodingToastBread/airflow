from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum


with DAG(
    dag_id="dags_conn_test",
    schedule=None,
    start_date=pendulum.datetime(2025,5,31,tz="Asia/Seoul"),
    catchup=False
) as dag:

    # EmptyOperator 는 정말 아무것도 안하는 Operator 이다.
    t1 = EmptyOperator(task_id="t1")

    t2 = EmptyOperator(task_id="t2")

    t3 = EmptyOperator(task_id="t3")

    t4 = EmptyOperator(task_id="t4")

    t5 = EmptyOperator(task_id="t5")

    t6 = EmptyOperator(task_id="t6")

    t7 = EmptyOperator(task_id="t7")

    t8 = EmptyOperator(task_id="t8")

    t1 >> [t2, t3] >> t4 # type: ignore
    t5 >> t4  # type: ignore
    [t4, t7] >> t6 >> t8  # type: ignore
