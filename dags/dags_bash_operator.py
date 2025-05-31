from airflow import DAG
import datetime
from airflow.operators.bash import BashOperator
import pendulum
from tornado.process import task_id


with DAG(
    dag_id="dags_bash_operator",  # 파일명과 일치시키는 것이 관례
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 5, 31, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout 60분
    # tags=["example", "example2"], # 태그, 검색시 좋음
    # params={"example_key": "example_value"}, # Task 들의 공통적으로 사용될 파라미터 작성
) as dag:
    # Task 인스턴스 명 = operator(~)
    bash_t1 = BashOperator(
        task_id="bash_t1",   # graph 에서 보이는 명칭, 인스턴스명칭과 통일하겠음
        bash_command="echo whoami" 
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",  # graph 에서 보이는 명칭, 인스턴스명칭과 통일하겠음
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2 # type: ignore