import logging
import datetime
from multiprocessing import Queue, Process
from pytdx.hq import TdxHq_API
import functools
import asyncio
import sys
import time
from concurrent import futures
from pytdx.hq import TdxHq_API
from pytdx.config.hosts import hq_hosts
import pandas as pd
import os
import glob
import czsc


def search_best_tdx():
    def ping2(host):
        return ping(host[0], host[1], host[2])

    hosts = [(host[1], host[2], True) for host in hq_hosts]
    with futures.ThreadPoolExecutor() as executor:
        res = executor.map(ping2, hosts, timeout=2)
    x = [i for i in res if i[0] == True]
    x.sort(key=lambda item: item[1])
    return x


def ping(ip, port=7709, multithread=False, timeout=1):
    api = TdxHq_API(multithread=multithread)
    success = False
    starttime = time.time()
    success = False
    try:
        if api.connect(ip, port, time_out=timeout):
            # x = api.get_security_count(0)
            # x = api.get_index_bars(7, 1, '000001', 800, 100)
            x = api.get_security_bars(7, 0, '000001', 800, 100)
            if x:
                success = True
    except Exception as e:
        print(e)
        pass
    endtime = time.time()
    return (success, endtime - starttime, ip, port)


def get_exception_info():
    info = sys.exc_info()
    return "{}: {}".format(info[0].__name__, info[1])


def hku_catch(ret=None, trace=False, callback=None, retry=1, with_msg=False, re_raise=False):
    """捕获发生的异常, 包装方式: @hku_catch()
    :param ret: 异常发生时返回值, with_msg为True时, 返回为 (ret, errmsg)
    :param boolean trace: 打印异常堆栈信息
    :param func callback: 发生异常后的回调函数, 入参同func
    :param int retry: 尝试执行的次数
    :param boolean with_msg: 是否返回异常错误信息, 为True时, 函数返回为 (ret, errmsg)
    :param boolean re_raise: 是否将错误信息以异常的方式重新抛出
    """
    def hku_catch_wrap(func):
        @functools.wraps(func)
        def wrappedFunc(*args, **kargs):
            for i in range(retry):
                errmsg = ""
                try:
                    val = func(*args, **kargs)
                    return (val, errmsg) if with_msg else val
                except Exception:
                    errmsg = "{} [{}.{}]".format(get_exception_info(), func.__module__, func.__name__)
                    if i == (retry - 1):
                        if callback is not None:
                            callback(*args, **kargs)
                        if re_raise:
                            raise Exception(errmsg)
                except KeyboardInterrupt:
                    raise KeyboardInterrupt()
                except:
                    errmsg = "Unknown error! {} [{}.{}]".format(get_exception_info(), func.__module__, func.__name__)
                    if i == (retry - 1):
                        if callback is not None:
                            callback(*args, **kargs)
                        if re_raise:
                            raise Exception(errmsg)
                return (ret, errmsg) if with_msg else ret

        return wrappedFunc

    return hku_catch_wrap


class BaoStock():
    @staticmethod
    def get_raw_bars_baostock(symbol, freq, sdt, edt, fq='前复权', **kwargs):
        """获取 CZSC 库定义的标准 RawBar 对象列表

        :param symbol: 标的代码
        :param freq: 周期，支持 Freq 对象，或者字符串，如
                '1分钟', '5分钟', '15分钟', '30分钟', '60分钟', '日线', '周线', '月线', '季线', '年线'
        :param sdt: 开始时间
        :param edt: 结束时间
        :param fq: 除权类型
        :param kwargs:
        :return:
        """

        kwargs['fq'] = fq
        file = glob.glob(os.path.join(kwargs['cache_path'], "*", f"{symbol}.parquet"))[0]
        freq = czsc.Freq(freq)
        kline = pd.read_parquet(file)
        if 'dt' not in kline.columns:
            kline['dt'] = pd.to_datetime(kline['time'], format='%Y%m%d%H%M%S%f')

        kline['vol'] = kline['volume'].astype(float).round(1)
        kline['amount'] = kline['amount'].astype(float).round(1)
        kline['open'] = kline['open'].astype(float).round(2)
        kline['high'] = kline['high'].astype(float).round(2)
        kline['low'] = kline['low'].astype(float).round(2)
        kline['close'] = kline['close'].astype(float).round(2)

        kline['symbol'] = symbol

        kline = kline[(kline['dt'] >= pd.to_datetime(sdt)) & (kline['dt'] <= pd.to_datetime(edt))]
        if kline.empty:
            return []
        _bars = czsc.resample_bars(kline, freq, raw_bars=True, base_freq='5分钟')
        return _bars
