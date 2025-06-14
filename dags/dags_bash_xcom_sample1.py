from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import pendulum


with DAG(
    dag_id="dags_bash_xcom_sample1",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    ### BashOperator 에서 push 하고 PythonOperator 에서 pull 하는 예시:

    # XCOM_PUSH 를 할 Bash Task 생성
    xcom_push_with_bash_task = BashOperator(
        task_id="xcom_push_with_bash_task",
        bash_command="echo PUSHING TO XCOM! "
        '{{ ti.xcom_push(key="name", value="CodingToastBread") }} && '
        "echo PUSH_COMPLETE",  # 마지막 출력문은 ti.xcom_push(key='return_value') 의 value 가 됩니다!
    )
    # 참고: 만약 마지막 출력문이 return_value 에 들어가지 않길 바라면
    # BashOperator 에서 do_xcom_push=False 처럼 파라미터 세팅.

    # XCOM_PULL 를 할 Python Task 생성
    @task(task_id="xcom_pull_with_python_task")
    def xcom_pull_with_python_task(**kwargs):
        ti = kwargs["ti"]
        status_value = ti.xcom_pull(key="name")
        return_value = ti.xcom_pull(task_ids="xcom_push_with_bash_task")
        print("xcom-data ==> name:", str(status_value))
        print("xcom-data ==> return_value:", return_value)


    xcom_push_with_bash_task >> xcom_pull_with_python_task()



    ### 반대로 PythonOperator 에서 push 하고 BashOperator 에서 pull 하는 예시:
    @task(task_id="push_with_python_task")
    def push_with_python_task(**kwargs):
        kwargs["ti"].xcom_push(
            key="personal_info", 
            value={"name": "CodingToastBread", "age": 0}
        )
        return "return value from push_with_python_task"


    pull_with_bash_task = BashOperator(
        task_id="pull_with_bash_task",
        env={
            "PERSONAL_INFO": '{{ ti.xcom_pull(task_ids="push_with_python_task")["personal_info"] }}',
            "RETURN_VALUE": '{{ ti.xcom_pull(task_ids="push_with_python_task")["return_value"] }}',
        },
        bash_command=
            'echo PERSONAL_INFO : $PERSONAL_INFO && '
            'echo RETURN_VALUE : $RETURN_VALUE'
    )

    push_with_python_task() >> pull_with_bash_task
