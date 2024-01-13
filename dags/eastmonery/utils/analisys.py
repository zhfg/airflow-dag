import sys
sys.path.append("/opt/bitnami/airflow/dags/git_airflow-dag/dags")
from eastmonery.utils._requests import create_session
from eastmonery.utils._minio import create_minio_client, minio_update_file, minio_upload_stock_list, minio_get_stock_list, minio_get_stock_kline
import pandas as pd

def append_kdj(df):
    # 使用参数： 9,3,3
    # 9天最低价
    lowest = df['lower'].rolling(9).min()
    lowest.fillna(value=df['lower'].expanding().min(), inplace = True)

    # 9天最高价
    highest = df['high'].rolling(9).max()
    highest.fillna(value=df['high'].expanding().max(), inplace = True)

    # 计算RSV
    rsv = (df.close-lowest) / (highest - lowest) * 100

    # 前面9天是nan, 填充为 100.0
    rsv.fillna(value=100.0, inplace=True)

    df['k'] = rsv.ewm(com=2, adjust=False).mean()
    df['d'] = df['k'].ewm(com=2, adjust=False).mean()
    df['j'] = 3 * df['k'] - 2 * df['d']

def find_prevoid_highest_close(datas, days: int):
    datas = datas.tail(days)
    highest_close = datas.max()
    highest_close_index = datas.idxmax()
    return highest_close, highest_close_index

def find_prevoid_lowest_close(datas, days: int):
    datas = datas.tail(days)
    lowest_close = datas.min()
    lowest_close_index = datas.idxmin()
    return lowest_close, lowest_close_index

def count_continuous_limit_up(code, datas, limit_up_rate):
    counts = []
    count = 0
    is_first= True
    for chg in datas:
        if chg >= limit_up_rate:
            if is_first:
                counts.append(count)
                count = 1
            else:
                count += 1
            is_first = False
        else:
            if not is_first and count != 0:
                counts.append(count)
            is_first = True
            count = 0
            
    # 当最后一天为涨停时，循环结束后，加入统计
    if count != 0:
        counts.append(count) 
    return counts

def count_down_with_rate(code, datas, down_rate):
    count = 0
    is_first= True
    for chg in datas:
        if chg <= down_rate:
            count += 1
            
    return count

def get_up_limit_rate(market, code):
    if market==1:
        if code.startswith("688"):
            return 19.95
        else:
            return 9.95           
    if market==0:
        if code.startswith("30"):
            return 19.95
        elif code.startswith("00"):
            return 9.95
        
def find_want_stocks(bucket,minio_endpoint, minio_access_key,minio_secret_key):
    minio_client = create_minio_client(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key
    )
    stocks = minio_get_stock_list(minio_client, bucket)
    i = 0
    print("股票名称\t市场\t股票代码\t近期最高价\t近期最高价出现位置\t近期最低价\t近期最低价的位置")
    for stock in stocks.get("all_stocks"):
        i += 1
        name = stock.get("name")
        market=stock.get("market")
        code = stock.get("code")
        day_kline = minio_get_stock_kline(client=client, bucket=bucket, market=market, code=code)
        klines = day_kline.get("data").get("klines")
        kline_len = len(klines)
        datas = []
        for k in klines:
            k = k.split(",")
            k_time = k[0]
            k_open = float(k[1])
            k_close = float(k[2])
            k_high = float(k[3])
            k_lower = float(k[4])
            k_volume = float(k[5])
            k_amount = float(k[6])
            k_chg = float(k[8])
            datas.append([k_time, k_open, k_close, k_high, k_lower, k_volume, k_amount, k_chg])
            
        columes = ['time', 'open', 'close', 'high', 'lower', 'volume', 'amount', 'chg']
        df = pd.DataFrame(datas, columns=columes,)

        if len(df) < 200:
            continue
        append_kdj(df)
        # 找到最高价和近期最低价
        high_close, high_index = find_prevoid_highest_close(df['high'], 30)
        lowest_close, lowest_index = find_prevoid_lowest_close(df['close'], 5)

        if lowest_index >= high_index and lowest_index < kline_len:
            if high_close/lowest_close > 1.46 or high_close/lowest_close < 1.51:
                # 统计最后价前十个交易日的连续涨停情况
                up_limit_rate = get_up_limit_rate(market, code)
                counts = count_continuous_limit_up(code, df['chg'].head(high_index+1).tail(10), up_limit_rate)
                match = False
                for count in counts:
                    if count > 3:
                        match = True
                if match:
                    max_continuous_up_limit = 0
                    for limit in counts:
                        if max_continuous_up_limit < limit:
                            max_continuous_up_limit = limit
                    
                    # 剔除出现最高价后有大阴线的股票
                    # 1. 统计每天跌幅大于跌停70%的股票,删除天数大于前面统计的连续涨停中最大连续涨停的天数
                    # 2. 删除最高价出现那天以来，到今天不要5天的股票
                    # 3. 统计自最高价出现那天以来，日跌幅大于跌停70%的天数， 删除总天数超过自最高价出现以来到今天的交易总天数的1/5的股票
                    exclude_down_rate = (0-up_limit_rate) * 0.7
                    after_limit_up_days = kline_len - high_index
                    # if code == "002888":
                    #     print(after_limit_up_days)
                    if after_limit_up_days < 5:
                        continue
                    down_count = count_down_with_rate(code, df['chg'].tail(after_limit_up_days), exclude_down_rate)
                    down_match = False

                    
                    if down_count > max_continuous_up_limit and down_count/after_limit_up_days > 0.2:
                        down_match = True
                    if not down_match:
                        print("{}\t{}\t{}\t{}\t\t{}\t\t\t{}\t\t{}\t\t{}\t\t\t\t\t{}".format(
                            name, market, code, high_close, high_index, lowest_close, lowest_index, counts, up_limit_rate
                        ))