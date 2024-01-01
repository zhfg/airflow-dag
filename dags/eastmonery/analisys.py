

def is_limit_up(market: int, code: str, data: list):
    # print(symbol, data.timestamp, data.percent)
    high = data[]
    limit_rate = 
    if market==0:
        if code.startswith("688"):
            if data.percent >= 19.95:
                return True
        else:
            return False
    if market==1:
        if np.isclose(data.percent, 10) or data.percent >= 9.95:
            return True
        else:
            return False

def count_continual_limit_up(name, market, code, datas):
    # 统计一个k线序列的涨停数据

    # 返回：统计一个K线序列中最后一个连续涨停数据，最少一个涨停
    # 返回：是否有涨停，连续涨中的第一个涨停发生在第几个数据，一共连续多少天涨停
    # 返回：False/True, index, count

    # 数据序列的长度少于10直接返回
    if len(datas) < 10:
        return False, 0, 0
    last_10_high: float = 0.00  # 10日内最高价
    current_close: float = 0.00 # 当日收盘价
    count_limit_up: int = 0     # 连续张停计数
    is_last_day_limit_up: bool = False #标志当前成交日是最涨停
    last_day_limit_up_index: int = 0  #标记最后连续涨停位置
    is_first_limit_up: bool = False # 标记是否第一涨停
    index: int = 0
    for data in datas:
        if is_limit_up(market, code, data):
            if not is_last_day_limit_up:
                is_first_limit_up = True
                count_limit_up = 1
                is_last_day_limit_up = True
                last_day_limit_up_index = index
            else:
                count_limit_up += 1
                last_day_limit_up_index = index
        else:
            is_last_day_limit_up = False
            is_first_limit_up = False
        index += 1
    if count_limit_up > 0:
        return True, last_day_limit_up_index, count_limit_up
    else:
        return False, 0, 0