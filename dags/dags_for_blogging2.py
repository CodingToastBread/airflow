from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum

# 예시 DAG 작성
with DAG(
    dag_id="dags_for_blogging2",
    
    # 매일 오전 6시에 동작
    schedule="0 6 * * *",
    
    # 스케줄의 시작 시간은 2025-06-06 부터이고,
    # 현재 시각은 2025-06-06 14:00:00 입니다.
    start_date=pendulum.datetime(2025, 6, 6, tz="Asia/Seoul"),
    catchup=True,
) as dags:

    # 아무런 동작을 하지 않는 EmptyOperator 작성
    task_1 = EmptyOperator( task_id="task_1" )
    task_1
