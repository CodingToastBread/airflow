from airflow.hooks.base import BaseHook
from pandas.io.parsers import read_csv
import psycopg2
import pandas as pd
from sqlalchemy.types import String

class CustomPostgresAdvanceHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
            port=self.port
        )

    def bulk_load(self, table_name, file_name, delimiter:str, is_header:bool, is_replace: bool, encoding='utf-8'):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블:' + table_name)
        self.get_conn()
        header = 0 if is_header else None
        
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        first_loop = True
        
        for chunk in pd.read_csv(file_name, header=header, delimiter=delimiter, chunksize=10000, encoding=encoding):
            dtype = None
            for col in chunk.columns:
                try:
                    # string 문자열이 아닐 경우 continue
                    chunk[col] = chunk[col].str.replace('\r\n', '')
                    self.log.info(f'{table_name}.{col}: 개행문자 제거')
                    
                except:
                    continue
            
            self.log.info('[ADVANCE] 적재 건수:' + str(len(chunk)))
            dtype = {col: String for col in chunk.columns}
            engine = create_engine(uri)
            
            # 첫번째 loop 이고, is_replace=True 면 if
            if first_loop and is_replace:
                first_loop = False
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    schema='public',
                    if_exists='replace',
                    index=False,
                    dtype=dtype
                )

            else:
                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    schema='public',
                    if_exists='append',
                    index=False,
                    dtype=dtype
                )