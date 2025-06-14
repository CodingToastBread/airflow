from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="dags_python_xcom_sample1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # 데코레이터로 PythonOperator 생성
    @task(task_id="pushing_task_1")
    def pushing_task_1(**kwargs):
        # TaskInstance 꺼내오기!
        ti = kwargs["ti"]

        # xcom_push 메소드로 공유 데이터 INSERT
        ti.xcom_push(key="key1", value="value1")
        ti.xcom_push(key="list1", value=[1, 2])
        ti.xcom_push(key="map1", value={"name": "coding_toast"})
        return "pushing_task 111111 return value"

    # @task(task_id="pushing_task_2")
    # def pushing_task_2(**kwargs):
    #     ti = kwargs["ti"]
    #     ti.xcom_push(key="key2", value="value2")
    #     ti.xcom_push(key="list2", value=[3, 4])
    #     return "pushing_task 222222 return value"

    @task(task_id="pulling_task")
    def pulling_task(**kwargs):
        ti = kwargs["ti"]
        keyVal = ti.xcom_pull(key="key1")
        listVal = ti.xcom_pull(key="list1")
        return_value = ti.xcom_pull(task_ids="pushing_task_1")
        print(keyVal)
        print(listVal)
        print(return_value)

    # xcom_push1() >> xcom_push2() >> xcom_pull()
    pushing_task_1() >> pulling_task()
