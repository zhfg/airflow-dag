from airflow import DAG, task
from datetime import datetime
from airflow.operators.python import PythonVirtualenvOperator
from pendulum import datetime, duration
import sys
# sys.path.append("/opt/bitnami/airflow/dags/git_airflow-dag/dags")
# from eastmonery.minio import (
#     create_minio_client, 
#     minio_update_file, 
#     minio_upload_stock_list,
#     minio_get_stock_list,
#     minio_upload_daily_kline,
#     )

# bucket = "stock"
# minio_endpoint = "192.168.1.151:9003"
# access_key = "Eecd8UOBiMxiVGnPHXcq"
# secret_key = "Ap2j4yY7aJ2bq870f6xuYp5axI66ZXcBKb6CeKwb"

# minio_client = create_minio_client(
#     endpoint=minio_endpoint,
#     access_key=access_key,
#     secret_key=secret_key
# )



dag_args = {
    "retries": 5,
    "retry_delay": duration(seconds=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": duration(hours=3),
}

with DAG(
    dag_id="sync_stack_list_from_east_monery_to_minio",
    start_date=datetime(2023,12,30),
    schedule="@daily",
    default_args=dag_args,
):
    
    stocks = []
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
        from eastmonery.minio import create_minio_client, minio_update_file, minio_upload_stock_list
        from eastmonery.stock import get_all_a_stock, get_kline, get_stock_detail, minio_get_stock_list
        minio_client = create_minio_client(
            endpoint=minio_endpoint,
            access_key=access_key,
            secret_key=secret_key
        )

        from concurrent.futures import ThreadPoolExecutor, as_completed

        stocks = get_all_a_stock()
        stocks_str = json.dumps({"all_stocks": stocks})
        stocks_len = len(stocks_str)
        minio_upload_stock_list(
            minio_client,
            bucket=bucket,
            src=stocks_str,
        )
        # stocks = minio_get_stock_list(minio_client, bucket)

    def daily_kline_from_east_monery():
        # from tqdm import tqdm
        from threading import Thread
        from time import sleep, ctime
        import sys, os, time, json, io
        
        bucket = "stock"
        minio_endpoint = "192.168.1.151:9003"
        access_key = "Eecd8UOBiMxiVGnPHXcq"
        secret_key = "Ap2j4yY7aJ2bq870f6xuYp5axI66ZXcBKb6CeKwb"

        sys.path.append("/opt/bitnami/airflow/dags/git_airflow-dag/dags")
        from eastmonery.minio import (
            create_minio_client, 
            minio_update_file, 
            minio_upload_stock_list,
            minio_get_stock_list,
            minio_upload_daily_kline,
            )
        from eastmonery.stock import get_all_a_stock, get_kline, get_stock_detail
        minio_client = create_minio_client(
            endpoint=minio_endpoint,
            access_key=access_key,
            secret_key=secret_key
        )

        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # stocks = minio_get_stock_list(minio_client, bucket)
        for stock in stocks.get("all_stocks"):
            name = stock.get("name")
            market=stock.get("market")
            code = stock.get("code")
            data = get_kline(
                market=market,
                code = code,
            )
            minio_upload_daily_kline(
                minio_client,
                bucket=bucket,
                src=json.dumps(data), market=market,
                code=code,
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

    task_2 = PythonVirtualenvOperator(
        task_id = "daily_kline_from_east_monery",
        requirements=requirements,
        python_callable=daily_kline_from_east_monery,
    )
    
    # task_2_group = []
    # # stocks = minio_get_stock_list(minio_client, bucket)
    # task_2_group.append(
    #     PythonVirtualenvOperator(
    #         task_id = "daily_kline_from_east_monery",
    #         requirements=requirements,
    #         python_callable=daily_kline_from_east_monery,
    #     )
    # )
    task_1 >> task_2