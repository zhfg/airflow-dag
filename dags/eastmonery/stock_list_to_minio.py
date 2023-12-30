from airflow import DAG, task
from datetime import datetime
from airflow.operators.python import PythonVirtualenvOperator

with DAG(
    dag_id="sync_stack_list_from_east_monery_to_minio",
    start_date=datetime(2023,12,30),
    schedule="@daily",
):
    def stock_from_east_monery():
        import requests
        import logging

        logger = logging.getLogger()
        session = requests.session()

        rsp = session.get("https://www.baidu.com")
        logger.info(rsp.text)

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