from pathlib import Path
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.bash import BashSensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
import pendulum


with DAG(
    dag_id="dags_sensor_for_blog",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
) as dag:
    
    # 모든 센서들이 이 파일을 센싱하면 종료가 됩니다.
    # 초기에는 없지만, 제가 수동으로 생성할 예정입니다.
    filepath = '/opt/airflow/dags/sample_files/sense.txt'


    file_sensor = FileSensor(
        task_id='file_sensor',
        filepath=filepath,
        poke_interval=2, # 2초 주기
        soft_fail=False
    )

    def file_exists(filepath):
        find_result = Path.exists(filepath)
        print(f'does [{filepath}] file exists? {find_result}')
        return find_result
    
    python_sensor = PythonSensor(
        task_id='python_sensor',
        op_kwargs={ 'filepath': filepath },
        python_callable=file_exists,
        poke_interval=3, # 3 초 주기
        soft_fail=False
    )
    
    # BashSensor 의 경우 return 값은 exit 0 일 때 True 로 판단하고,
    # 그외의 exit status 는 모두 False 로 판단한다.
    bash_sensor = BashSensor(
        task_id='bash_sensor',
        env={'FILE': filepath},
        bash_command='''echo finding $FILE !! &&
        if [ -f $FILE ]; then
            exit 0
        else
            exit 1
        fi
        ''',
        poke_interval=4, # 4초 주기
        soft_fail=False,
    )
    
    # 위에 선언한 센서들이 모두 성공이든/실패든 완료되면 이 태스트를 실행
    @task(task_id="all_sensor_done", trigger_rule='all_done') # 모두 성공해야만 함.
    def all_sensor_done():
        print('모든 sensor 작업이 완료됐습니다.')
        

    # 참고로 위 센서들 모두 각각 poke_interval 이 다르므로, 서로 다른 시간에 완료될 가능성이 있다.
    [file_sensor, python_sensor, bash_sensor] >> all_sensor_done()
