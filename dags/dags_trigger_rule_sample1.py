from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum


with DAG(
    dag_id="dags_trigger_rule_sample1",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
) as dag:

    # 50 % 의 확률로 Exception 을 던지는 메소드 작성
    def half_and_half():
        import random
        random_num = random.randint(1,2) # 확률은 반반!
        if random_num == 1:
            raise AirflowException('뭔가 잘못됐음')
        elif random_num == 2:
            return "downstream_task2"

    pt1 = PythonOperator(task_id="pt1", python_callable=half_and_half)
    pt2 = PythonOperator(task_id="pt2", python_callable=half_and_half)

    downstream_task = BashOperator(
        task_id="downstream_task",
        bash_command='echo "All upstream task Success!"',
        trigger_rule="all_done", # 실패든 성공이든 상관없이 모든 상위 task 가 끝나면 실행!
    )

    [pt1, pt2] >> downstream_task
