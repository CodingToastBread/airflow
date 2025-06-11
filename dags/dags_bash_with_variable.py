from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
import pendulum


with DAG(
    dag_id="dags_bash_with_variable",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    # not recommended
    var_value = Variable.get("sample_key")
    
    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo variable:{var_value}",
    )

    # recommended!
    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        bash_command="echo variable: {{ var.value.sample_key }}"
    )
