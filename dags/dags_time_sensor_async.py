from airflow import DAG
from airflow.sensors.date_time import DateTimeSensor, DateTimeSensorAsync
import pendulum


with DAG(
    dag_id="dags_time_sensor_async",
    start_date=pendulum.datetime(2025,7,1,0,0,0),
    end_date=pendulum.datetime(2025,7,1,1,0,0),
    schedule="*/10 * * * *",
    catchup=True
) as dag:
    sync_sensor = DateTimeSensorAsync(
        task_id = "sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}"""
    )
