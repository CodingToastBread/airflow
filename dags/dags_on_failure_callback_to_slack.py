from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

from config.on_failure_callback_to_slack import on_failure_callback_to_slack


with DAG(
    dag_id="dags_on_failure_callback_to_slack",
    start_date=pendulum.datetime(2025,7,1,tz="Asia/Seoul"),
    schedule="*/20 * * * *",
    catchup=False,
    default_args={
        'on_failure_callback': on_failure_callback_to_slack,
        'execution_timeout': timedelta(seconds=60)
    }
) as dag:
    task_slp_90 = BashOperator(
        task_id='task_slp_90',
        bash_command='sleep 90'
    )
    
    task_ext_1 = BashOperator(
        trigger_rule='all_done',
        task_id='task_ext_1',
        bash_command='exit 1'
    )

    # 2개의 task 모두 실패할 예정!
    task_slp_90 >> task_ext_1
