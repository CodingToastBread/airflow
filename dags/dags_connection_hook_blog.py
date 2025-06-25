from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance
import pendulum


with DAG(
    dag_id="dags_connection_hook_blog",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
) as dag:
    
    @task(task_id='read_dummy_json_task')
    def read_dummy_json_task(ti: TaskInstance):
        import os
        from airflow.hooks.base import BaseHook
        from airflow.providers.http.hooks.http import HttpHook
        
        print(f'START {ti.task_id} [{ti.data_interval_end.in_timezone("Asia/Seoul")}]')

        conn_id = 'jsonplaceholder.typicode'

        # Hook 의 classMethod (=get_connection) 으로 connection_id 와 매칭되는 정보를 갖는 Connection 인스턴스 생성
        conn = BaseHook.get_connection(conn_id)
        
        # 설정한 정보를 조회할 수 있습니다.
        print(f'Connection Id: {conn.conn_id}')
        print(f'Connection Type: {conn.conn_type}')
        print(f'Connection host: {conn.host}')
        print(f'Connection port: {conn.port}')
        
        # Hook 의 get_conn 메소드로 실제 통신 객체를 참조해보겠습니다.
        hook = HttpHook(conn_id)
        
        # Connection 정보 조회
        # hook.get_connection() # BaseHook.get_connection(conn_id) 와 동일
        
        # 실제 Http 통신 인스턴스(requests.Session) 참조
        session = hook.get_conn() 

        # 주의!
        # - hook.get_connection 메소드는 airflow ui 에서 생성한 Connection 반환
        # - hook.get_conn 메소드는 Connection 타입에 따라 실제 통신을 가능케하는 인스턴스를 반환
        # 헷갈리지 마세요!

        # requests.Session 인스턴스의 메소드를 사용해서 json 데이터 받아오기
        response = session.get(f'{hook.base_url}/posts') 

        file_save_dir = '/opt/airflow/files'
        os.makedirs(file_save_dir, exist_ok=True)
        with open(f'{file_save_dir}/data.json', 'w', encoding='UTF-8') as f:
            f.write(response .text) # json 문자열을 파일로 저장!

    read_dummy_json_task()