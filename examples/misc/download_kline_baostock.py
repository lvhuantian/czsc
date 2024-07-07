import os

import baostock as bs
import pandas as pd

from constants import *


def download_kline(code: str, start_date: str, end_date: str,
                   freq: str, market: str, result_path: str):
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)

    #### 获取沪深A股历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
    # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
    # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg

    code_label = f"{market}.{code}".lower()
    rs = bs.query_history_k_data_plus(code_label,
                                      "date,time,code,open,high,low,close,volume,amount,adjustflag",
                                      start_date=start_date, end_date=end_date,
                                      frequency=freq, adjustflag="1")

    print('query_history_k_data_plus respond error_code:' + rs.error_code)
    print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    df = pd.DataFrame(data_list, columns=rs.fields)

    df.to_parquet(os.path.join(result_path, f'{code}.{market}.parquet'))

    #### 登出系统 ####
    bs.logout()


if __name__ == '__main__':
    market = MARKET.SH
    code = "600733"
    start_date = "2023-01-02"
    end_date = "2024-07-05"
    freq = "15"
    result_path = "/Users/equation42/Desktop/CZSC2/baostock"

    download_kline(code=code, start_date=start_date, end_date=end_date,
                   freq=freq, market=market, result_path=result_path)
