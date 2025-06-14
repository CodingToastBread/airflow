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
        ti.xcom_push(key="key", value="value11111")
        ti.xcom_push(key="list", value=[1, 2, 3])
        return "return 111111"

    @task(task_id="pushing_task_2")
    def pushing_task_2(**kwargs):
        ti = kwargs["ti"]
        ti.xcom_push(key="key", value="value22222")
        ti.xcom_push(key="list", value=[4, 5, 6])
        return "return 222222"

    @task(task_id="pulling_task")
    def pulling_task(**kwargs):

        # push task 의 구동 순서 때문에 pushing_task_2 의 세팅값이 조회됩니다.
        ti = kwargs["ti"]
        keyVal = ti.xcom_pull(key="key")
        listVal = ti.xcom_pull(key="list")
        return_value = ti.xcom_pull(key='return_value')

        print("task_ids 값 주지 않고 조회하기:")
        print(keyVal)
        print(listVal)
        print(return_value)

        # task_id 인자값을 줘야만 같은 key 명칭을 쓰더라도 정확히 구분해서 조회할 수 있습니다.
        ti = kwargs["ti"]
        keyVal = ti.xcom_pull(key="key", task_ids="pushing_task_1")
        listVal = ti.xcom_pull(key="list", task_ids="pushing_task_1")
        return_value = ti.xcom_pull(key='return_value' # key='return_value' 는 생략가능!
                                    , task_ids="pushing_task_1") 
        
        print('task_ids="pushing_task_1" 세팅 후 조회하기:')
        print(keyVal)
        print(listVal)
        print(return_value)

    pushing_task_1() >> pushing_task_2() >> pulling_task()
