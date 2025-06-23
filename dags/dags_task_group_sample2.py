from airflow import DAG
from airflow.decorators import task, task_group
import pendulum


with DAG(
    dag_id='dags_task_group_sample2',
    start_date=pendulum.datetime(2025, 6, 1, tz='Asia/Seoul'),
    schedule='@daily',
    catchup=False
) as dag:
    
    # 목표
    ## [그룹 1 : [태스트1 -> [INNER 그룹 : 태스트3] -> 태스트2]] -> [그룹 2 : [태스트1 -> 태스트2]]
    
    @task_group(group_id='group_1')
    def group_1():
        
        @task(task_id='group_1_task_1')
        def group_1_task_1():
            print('group_1_task_1 executed')
        
        @task(task_id='group_1_task_1')
        def group_1_task_2():
            print('group_1_task_2 executed')
            

        @task_group(group_id='group_in_group')
        def group_in_group():
            @task(task_id='task_in_group_in_group')
            def task_in_group_in_group():
                print('task_in_group_in_group executed')
            
            task_in_group_in_group()
        
        group_1_task_1() >> group_in_group() >> group_1_task_2()
    
    
    @task_group(group_id='group_2')
    def group_2():
        
        @task(task_id='group_2_task_1')
        def group_2_task_1():
            print('group_2_task_1 executed')
        
        @task(task_id='group_2_task_2')
        def group_2_task_2():
            print('group_2_task_2 executed')
        
        group_2_task_1() >> group_2_task_2()
    
    group_1() >> group_2()
    