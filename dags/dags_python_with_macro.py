from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import pendulum

# 예시 DAG 작성
with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    catchup=False,
) as dags:

    @task(
        task_id="task_using_macros",
        templates_dict={
            "start_date": ' {{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }} ',
            "end_date": ' {{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }} ',
        },
    )
    def get_dateitem_macro(**kwargs):
        template_dicts = kwargs.get("templates_dict") or {}
        if template_dicts:
            start_date = template_dicts.get('start_date') or 'no start_date'
            end_date = template_dicts.get('end_date') or 'no end_date'
            print(start_date)
            print(end_date)

    @task(task_id="task_direct_calc")
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta
        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(day=-1)
        print(prev_month_day_first)
        print(prev_month_day_last)

    get_dateitem_macro() >> get_datetime_calc()
