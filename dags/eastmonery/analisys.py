def is_limit_up(symbol, data: dict):
    # print(symbol, data.timestamp, data.percent)
    if symbol.startswith('SZ3') or symbol.startswith('SH688'):
        if np.isclose(data.percent, 20) or data.percent >= 19.95:
            return True
        else:
            return False
    if symbol.startswith('SH6') or symbol.startswith('SZ0'):
        if np.isclose(data.percent, 10) or data.percent >= 9.95:
            return True
        else:
            return False

def count_continual_limit_up(symbol, datas):
    # 统计一个k线序列的涨停数据

    # 返回：统计一个K线序列中最后一个连续涨停数据，最少一个涨停
    # 返回：是否有涨停，连续涨中的第一个涨停发生在第几个数据，一共连续多少天涨停
    # 返回：False/True, index, count

    # 数据序列的长度少于10直接返回
    if len(datas) < 10:
        return False, 0, 0
    last_10_high: float = 0.00
    current_close: float = 0.00
    count_limit_up: int = 0
    is_last_day_limit_up: bool = False
    last_day_limit_up_index: int = 0
    is_first_limit_up: bool = False
    index: int = 0
    for data in datas:
        if is_limit_up(symbol, data):
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