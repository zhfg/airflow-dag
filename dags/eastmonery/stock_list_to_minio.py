from airflow import DAG, task
from datetime import datetime
from airflow.operators.python import PythonVirtualenvOperator

with DAG(
    dag_id="sync_stack_list_from_east_monery_to_minio",
    start_date=datetime(2023,12,30),
    schedule="@daily",
):
    def stock_from_east_monery():
        from .stock import get_all_a_stock, get_kline, get_stock_detail
        from tqdm import tqdm
        from threading import Thread
        from time import sleep, ctime
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        stocks = get_all_a_stock()
        print(stocks)

    requirements = [
        'requests',
        # 'logging',
    ]
    task_1 = PythonVirtualenvOperator(
        task_id="stock_from_east_monery",
        requirements=requirements,
        python_callable=stock_from_east_monery,
    )

    task_1