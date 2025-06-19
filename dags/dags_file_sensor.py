from airflow import DAG
from airflow.sensors.filesystem import FileSensor
import pendulum


with DAG(
    dag_id="dags_file_sensor",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
) as dag:
    GlobalJobCounselLngMmRst_sensor = FileSensor(
        task_id='GlobalJobCounselLngMmRst_sensor',
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='GlobalJobCounselLngMmRst/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/GlobalJobCounselLngMmRst.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24,
        mode='reschedule'
    )