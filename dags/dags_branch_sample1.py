from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
import pendulum


with DAG(
    dag_id="dags_branch_sample1",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
) as dag:

    def select_next_task():
        import random
        random_num = random.randint(1,3) # 1 ~ 3 까지의 숫자를 조회한다.
        if random_num == 1:
            return "downstream_task1"
        elif random_num == 2:
            return "downstream_task2"
        elif random_num == 3:
            return ["downstream_task1", "downstream_task2"]

    random_select_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_next_task        
    )

    # @task.branch(task_id="python_branch_task")
    # def select_random():
    #     import random
    #     item_list = ["A", "B", "C"]
    #     selected_item = random.choice(item_list)
    #     if selected_item == "A":
    #         return "task_a"
    #     elif selected_item in ["B", "C"]:
    #         return ["task_b", "task_c"]

    # def common_func(**kwargs):
    #     print(kwargs["selected"])

    downstream_task1 = BashOperator(
        task_id="downstream_task1", bash_command='echo "I AM TASK 11111"'
    )

    downstream_task2 = BashOperator(
        task_id="downstream_task2", bash_command='echo "I AM TASK 22222"'
    )

    random_select_task >> [downstream_task1, downstream_task2]
