from eastmonery.utils._requests import create_session
from datetime import datetime
import json
from tqdm import tqdm
session = create_session()

def get_all_a_stock():
    all_stock_list = []

    sh_fm = "m:1+t:2,m:1+t:23"
    sz_fm = "m:0+t:6,m:0+t:80"
    sh_pre = "SH"
    sz_pre = "SZ"
    sh_a_url = "http://47.push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz={pz}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=|0|0|0|web&fid=f3&fs=m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1701597491665"
    sz_a_url = "http://47.push2.eastmoney.com/api/qt/clist/get?pn={pn}&pz={pz}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=|0|0|0|web&fid=f3&fs=m:0+t:6,m:0+t:80&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1701597491660"

    stock_configs = {
        "sh": dict(market=1, url_pattern = sh_a_url, fm=sh_fm, pre=sh_pre),
        "sz": dict(market=0, url_pattern = sz_a_url, fm=sz_fm, pre=sz_pre)
    }
    for market in stock_configs.keys():
        config = stock_configs.get(market)
        pn = 1
        pz = 1000
        is_last_page = False
        while not is_last_page:
            url = config.get("url_pattern").format(pn = pn, pz = pz)
            pn += 1
            rsp = session.get(url)
            if rsp.status_code == 200:
                rsp_json_data_diff = json.loads(rsp.text).get("data").get("diff")

                if len(rsp_json_data_diff) != pz:
                    is_last_page = True
                for i in rsp_json_data_diff:
                    if i.get("f2") == '-':
                        continue
                    code = i.get("f12")
                    symbol = "{}{}".format(config.get("pre"), code)
                    name = i.get("f14")
                    market_code = config.get("market")
                    all_stock_list.append({"market": market_code, "code": code, "name": name})
    return all_stock_list

def get_stock_detail(market, code, end_date=int(datetime.timestamp(datetime.now())*1000)):
    # parameters
    # market
    ## 0: 京，深
    ## 1: 上

    # return
    ## "f43": 今收,
    ## "f44": 最高,
    ## "f45": 最低,
    ## "f46": 今开,
    ## "f51": 涨停,
    ## "f52": 跌停,
    ## "f60": 昨收,
    ## "f57": code,
    ## "f58": 名称,
    ## "f168": 换手,
    ## "f116": 总市值,
    detail_url = "https://push2.eastmoney.com/api/qt/stock/get?invt=2&fltt=1&fields=f58%2Cf734%2Cf107%2Cf57%2Cf43%2Cf59%2Cf169%2Cf301%2Cf60%2Cf170%2Cf152%2Cf177%2Cf111%2Cf46%2Cf44%2Cf45%2Cf47%2Cf260%2Cf48%2Cf261%2Cf279%2Cf277%2Cf278%2Cf288%2Cf19%2Cf17%2Cf531%2Cf15%2Cf13%2Cf11%2Cf20%2Cf18%2Cf16%2Cf14%2Cf12%2Cf39%2Cf37%2Cf35%2Cf33%2Cf31%2Cf40%2Cf38%2Cf36%2Cf34%2Cf32%2Cf211%2Cf212%2Cf213%2Cf214%2Cf215%2Cf210%2Cf209%2Cf208%2Cf207%2Cf206%2Cf161%2Cf49%2Cf171%2Cf50%2Cf86%2Cf84%2Cf85%2Cf168%2Cf108%2Cf116%2Cf167%2Cf164%2Cf162%2Cf163%2Cf92%2Cf71%2Cf117%2Cf292%2Cf51%2Cf52%2Cf191%2Cf192%2Cf262%2Cf294%2Cf295%2Cf269%2Cf270%2Cf256%2Cf257%2Cf285%2Cf286%2Cf748%2Cf747&secid={market}.{code}&ut=fa5fd1943c7b386f172d6893dbfba10b&wbp2u=%7C0%7C0%7C0%7Cweb&_={end_date}"
    url = detail_url.format(market=market, code=code, end_date=end_date)
    data = json.loads(
        session.get(url).text
    )

    return data

def get_kline(market, code, klt=101, fq=0, pg_size=3000, end_data=int(datetime.timestamp(datetime.now())*1000)):

    kline_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={market}.{code}&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt={klt}&fqt={fq}&end=20500101&lmt={pg_size}&_={end_date}"
    # market
    ## 0: 京，深
    ## 1: 上
    
    # klt
    ## 101: daily
    ## 102: weekly
    ## 103: monthly
    ## 104: quotly
    ## 105: half yearly
    ## 106: yearly

    # fqt
    ## 0: 不复权
    ## 1：前复权
    ## 2: 后复权
    url = kline_url.format(market=market, code=code, klt=101, fq=0, pg_size=3000, end_date=int(datetime.timestamp(datetime.now())*1000))

    data = json.loads(
        session.get(url).text
    )

    return data
