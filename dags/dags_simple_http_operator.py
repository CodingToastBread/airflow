from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator
import pendulum


with DAG(
    dag_id="dags_simple_http_operator",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
) as dag:

    """서울시 서울글로벌센터 언어별(월) 상담실적"""
    """https://data.seoul.go.kr/dataList/OA-15738/S/1/datasetView.do"""
    tb_cycle_station_info = HttpOperator(
        task_id="tb_cycle_station_info",
        http_conn_id="openapi.seoul.go.kr",
        endpoint="{{var.value.apikey_openapi_seoul_go_kr}}/json/GlobalJobCounselLngMmRst/1/10/",
        method='GET',
        headers={
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }
    )

    @task(task_id="python_2")
    def python_2(**kwargs):
        ti = kwargs["ti"]
        rslt = ti.xcom_pull(task_ids="tb_cycle_station_info")
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    tb_cycle_station_info >> python_2()
