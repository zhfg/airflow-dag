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
        

    def find_want_stocks():
        from threading import Thread
        from time import sleep, ctime
        import sys, os, time, json, io
        bucket = "stock"
        minio_endpoint = "192.168.1.151:9003"
        access_key = "Eecd8UOBiMxiVGnPHXcq"
        secret_key = "Ap2j4yY7aJ2bq870f6xuYp5axI66ZXcBKb6CeKwb"

        sys.path.append("/opt/bitnami/airflow/dags/git_airflow-dag/dags")
        from eastmonery.utils._minio import (
            create_minio_client, 
            minio_update_file, 
            minio_upload_stock_list,
            minio_get_stock_list,
            minio_upload_daily_kline,
            minio_get_stock_kline,
            )
        from eastmonery.utils.stock import get_all_a_stock, get_kline, get_stock_detail
        minio_client = create_minio_client(
            endpoint=minio_endpoint,
            access_key=access_key,
            secret_key=secret_key
        )
        from eastmonery.utils.analisys import count_continual_limit_up, find_highst_close_index
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        stocks = minio_get_stock_list(client=minio_client, bucket=bucket, )
        satisfied_stocks: list = []
        for stock in stocks.get("all_stocks"):
            name = stock.get("name")
            market=stock.get("market")
            code = stock.get("code")
            day_kline = minio_get_stock_kline(client=minio_client, bucket=bucket, market=market, code=code)
            # 统计最后10个交易日的连续涨停
            dktotal = day_kline.get("data").get("dktotal")
            last_10_datas = day_kline.get("data").get("klines")[-30:]
            last_10_datas = [x.split(',') for x in last_10_datas]
            b, i, c = count_continual_limit_up(name, market, code, last_10_datas)

            ## 过滤连续2个或3个涨停的股票
            if c > 2 or c > 3:
                #  过去10个交易日最高价出现在这两个交易日上
                ## 找到最高收盘价所在的位置, 对比其所在的数据索引与连续涨停的位，判断其是否与最后一个涨停所在的位置是否一致
                highest_close_index, highest_close = find_highst_close_index(last_10_datas)
                if highest_close_index == i or highest_close_index == i + 1:
                    close_rate = float(last_10_datas[-1][2]) / highest_close  
                    print("name: {}, market: {}, code: {}, 最大连续涨停次数: {}, 最高价出现的位置: {}, 最高价格: {}, 连椟涨停最后一个位置: {}, 收盘价与最高价的比值: {}, 今日收盘: {}, 10日内最高价: {}".format(name, market, code, c, highest_close_index, highest_close, i, close_rate, last_10_datas[-1][2], highest_close))           
                    # if close_rate > 0.41 and close_rate < 0.51:
                    #     print("symbol: {}, 最大连续涨停次数: {}, 最高价出现的位置: {}, 最高价格: {}, 连椟涨停最后一个位置: {}, 收盘价与最高价的比值: {}, 今日收盘: {}, 10日内最高价: {}".format(stock.symbol, c, highest_close_index, highest_close, i, close_rate, last_10_datas[-1].close, highest_close))
                    #     satisfied_stocks.append(stock)  
                # 今日收盘价与最高价的比值在41%到51%之间的股票
                
        return satisfied_stocks

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

    # task_3 = PythonVirtualenvOperator(
    #     task_id = "find_wiat_stocks",
    #     requirements=requirements,
    #     python_callable=find_want_stocks,
    # )

    task_1 
    # task_3