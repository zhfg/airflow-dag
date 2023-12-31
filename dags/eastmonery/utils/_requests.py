from requests import Session, cookies
import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
 
# Define the retry strategy
retry_strategy = Retry(
    total=4,  # Maximum number of retries
    status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
)
# Create an HTTP adapter with the retry strategy and mount it to session
adapter = HTTPAdapter(max_retries=retry_strategy)

def create_session():
    headers  = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "stock.eastmoney.com",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "sec-ch-ua": 'Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
    }

    # get eastmonery url first to get headers and cookies
    cookies_jar = cookies.RequestsCookieJar()
    session = Session()
    # session.headers = headers
    # session.cookies = cookies_jar
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    # rsp = requests.get("http://47.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=|0|0|0|web&fid=f3&fs=m:1+t:2,m:1+t:23&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1701597491665")
    # print("======================{}=====================".format(rsp.text))
    return session