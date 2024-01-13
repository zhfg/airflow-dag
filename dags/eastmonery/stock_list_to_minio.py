from airflow import DAG, task
from datetime import datetime
from airflow.operators.python import PythonVirtualenvOperator, ExternalPythonOperator
from pendulum import datetime, duration


dag_args = {
    "retries": 1,
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
    bucket = "stock"
    minio_endpoint = "192.168.1.151:9003"
    access_key = "Eecd8UOBiMxiVGnPHXcq"
    secret_key = "Ap2j4yY7aJ2bq870f6xuYp5axI66ZXcBKb6CeKwb"
    
    stocks = []
    def task_stock_from_east_monery(
            bucket,
            minio_endpoint,
            access_key,
            secret_key,
                ):
        import sys
        sys.path.append("/opt/bitnami/airflow/dags/git_airflow-dag/dags")

        from eastmonery.utils.stock import stock_from_east_monery
        stock_from_east_monery(
            bucket,
            minio_endpoint,
            access_key,
            secret_key,
                )

    def task_daily_kline_from_east_monery(
            bucket,
            minio_endpoint,
            access_key,
            secret_key, 
    ):
        import sys
        sys.path.append("/opt/bitnami/airflow/dags/git_airflow-dag/dags")

        from eastmonery.utils.stock import daily_kline_from_east_monery
        daily_kline_from_east_monery(
            bucket,
            minio_endpoint,
            access_key,
            secret_key,
        )  
        
    def task_find_want_stocks(
        bucket,
        minio_endpoint,
        access_key,
        secret_key,
    ):
        import sys
        sys.path.append("/opt/bitnami/airflow/dags/git_airflow-dag/dags")

        from eastmonery.utils.analisys import find_want_stocks
        find_want_stocks(
            bucket,
            minio_endpoint,
            access_key,
            secret_key,
        )

    task_1 = ExternalPythonOperator(
        task_id="stock_from_east_monery",
        python_callable=task_stock_from_east_monery,
        python="/venvs/airflow/bin/python",
        op_args=(
            bucket,
            minio_endpoint,
            access_key,
            secret_key,
        ),
    )

    task_2 = ExternalPythonOperator(
        task_id = "daily_kline_from_east_monery",
        python="/venvs/airflow/bin/python",
        op_args=(
            bucket,
            minio_endpoint,
            access_key,
            secret_key,
        ),
        python_callable=task_daily_kline_from_east_monery,
    )

    task_3 = ExternalPythonOperator(
        task_id = "find_wiat_stocks",
        python="/venvs/airflow/bin/python",
        op_args=(
            bucket,
            minio_endpoint,
            access_key,
            secret_key,
        ),
        python_callable=task_find_want_stocks,
    )

    task_1 >> task_2 >> task_3
    # task_3