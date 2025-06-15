from airflow import DAG
import pendulum
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator


with DAG(
    dag_id="dags_seoul_api_corona",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
) as dag:

    """서울시 서울글로벌센터 언어별(월) 상담실적"""
    """https://data.seoul.go.kr/dataList/OA-15738/S/1/datasetView.do"""
    # GlobalJobCounselLngMmRst
    tb_global_job_sounsel = SeoulApiToCsvOperator(
        task_id="tb_global_job_sounsel",
        dataset_nm="GlobalJobCounselLngMmRst",
        path='/opt/airflow/files/GlobalJobCounselLngMmRst/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name="GlobalJobCounselLngMmRst.csv"
    )

    tb_global_job_sounsel
