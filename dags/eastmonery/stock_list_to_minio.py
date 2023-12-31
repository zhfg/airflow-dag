from airflow import DAG, task
from datetime import datetime
from airflow.operators.python import PythonVirtualenvOperator



with DAG(
    dag_id="sync_stack_list_from_east_monery_to_minio",
    start_date=datetime(2023,12,30),
    schedule="@daily",
):
    def stock_from_east_monery():
        # from tqdm import tqdm
        from threading import Thread
        from time import sleep, ctime
        import sys, os, time, json, io
        
        bucket = "stock"
        minio_endpoint = "192.168.1.151:9003"
        access_key = "Eecd8UOBiMxiVGnPHXcq"
        secret_key = "Ap2j4yY7aJ2bq870f6xuYp5axI66ZXcBKb6CeKwb"


        sys.path.append("/opt/bitnami/airflow/dags/git_airflow-dag/dags")
        from eastmonery.minio import create_minio_client, minio_update_file
        from eastmonery.stock import get_all_a_stock, get_kline, get_stock_detail
        minio_client = create_minio_client(
            endpoint=minio_endpoint,
            access_key=access_key,
            secret_key=secret_key
        )

        from concurrent.futures import ThreadPoolExecutor, as_completed

        stocks = get_all_a_stock()
        minio_update_file(
            minio_client,
            bucket=bucket,
            src=io.BytesIO(json.dumps({"all_stocks": stocks})),
            dest="test.json"
        )

    requirements = [
        'requests',
        'minio',
        # 'logging',
    ]
    task_1 = PythonVirtualenvOperator(
        task_id="stock_from_east_monery",
        requirements=requirements,
        python_callable=stock_from_east_monery,
    )

    task_1