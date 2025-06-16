from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from tornado.process import task_id

with DAG(
    dag_id="dags_python_with_postgres",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
) as dag:

    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get("ti").task_id
                run_id = kwargs.get("ti").run_id
                msg = 'insrt 수행'
                sql = "insert into py_opr_drct_insrt values (%s,%s,%s,%s);"
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    isrt_postgres = PythonOperator(
        task_id='isrt_postgres',
        python_callable=insrt_postgres,
        op_args=['172.28.0.3', '5432', 'coding_toast' ,'postgres', 'postgres']
    )

    isrt_postgres
