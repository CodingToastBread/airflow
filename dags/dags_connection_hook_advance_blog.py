from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator
import pendulum


with DAG(
    dag_id="dags_connection_hook_advance_blog",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
) as dag:
    
    conn_id = 'jsonplaceholder.typicode'
    
    read_dummy_json_task = HttpOperator(
        task_id='read_dummy_json_task',
        headers={ 'Accept': 'application/json' },
        http_conn_id=conn_id,
        endpoint='/posts', method='GET'
    )

    @task(task_id='save_json_text_to_file')
    def save_json_text_to_file(**kwargs):
        import os
        from airflow.models import Variable
        from airflow.operators.python import get_current_context
        
        context = get_current_context()
        ti = context['ti']
        
        print(f'data_interval_end : {context["data_interval_end"].in_timezone("Asia/Seoul")}')
        # print(f'data_interval_end : {kwargs["data_interval_start"].in_timezone("Asia/Seoul")}')

        # read_dummy_json_task 메소드가 반환한 response.text 값을 받습니다.
        # ti = kwargs['ti']
        result_json = ti.xcom_pull(task_ids='read_dummy_json_task')
        
        # 공통 저장 디렉토리 경로 조회
        # file_save_dir = '/opt/airflow/files'
        file_save_dir = Variable.get("json_save_directory")
        os.makedirs(file_save_dir, exist_ok=True)
        with open(f'{file_save_dir}/data.json', 'w', encoding='UTF-8') as f:
            f.write(result_json) # json 문자열을 파일로 저장!

        print(f"write json success ===> {os.path.join(file_save_dir, 'data.json')}")

    read_dummy_json_task >> save_json_text_to_file()