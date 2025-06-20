from airflow import DAG
from airflow.decorators import task
import pendulum


with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task(task_id="python_xcom_push_by_return")
    def xcom_push_rewsult(**kwargs):
        return "Success"

    @task(task_id="python_xcom_pull_1")
    def xcom_pull_1(**kwargs):
        ti = kwargs["ti"]
        # task_ids 만 달랑 있으면, 자동으로 RETURN_VALUE 키에 매칭된는 값이 들어옴.
        value1 = ti.xcom_pull(task_ids="python_xcom_push_by_return")
        print("xcom_pull 메소드로 직접 찾은 리턴값: ", value1)

    @task(task_id="python_xcom_pull_2")
    def xcom_pull_2(status, **kwargs):
        print(status)

    python_xcom_push_by_return = xcom_push_rewsult()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()
