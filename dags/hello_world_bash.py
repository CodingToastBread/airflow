
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


with DAG(
    dag_id="hello_world_bash",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    task1 = BashOperator(
        task_id="task1",
        bash_command="scripts/helloworld.sh",
        env={'MY_NAME': 'CODING_TOAST'}
    )
    
    task1