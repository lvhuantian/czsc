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


class MARKET:
    SH = 'SH'
    SZ = 'SZ'
    BJ = 'BJ'


g_market_list = [MARKET.SH, MARKET.SZ, MARKET.BJ]


class MARKETID:
    SH = 1
    SZ = 2
    BJ = 3


class STOCKTYPE:
    BLOCK = 0  # 板块
    A = 1  # A股
    INDEX = 2  # 指数
    B = 3  # B股
    FUND = 4  # 基金（非ETF）
    ETF = 5  # ETF
    ND = 6  # 国债
    BOND = 7  # 其他债券
    GEM = 8  # 创业板
    START = 9  # 科创板
    A_BJ = 11  # 北交所A股


def get_a_stktype_list():
    """获取A股市场证券类型元组，含B股"""
    return (STOCKTYPE.A, STOCKTYPE.INDEX, STOCKTYPE.B, STOCKTYPE.GEM, STOCKTYPE.START, STOCKTYPE.A_BJ)


def get_exception_info():
    info = sys.exc_info()
    return "{}: {}".format(info[0].__name__, info[1])


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


def search_best_tdx():
    def ping2(host):
        return ping(host[0], host[1], host[2])

    hosts = [(host[1], host[2], True) for host in hq_hosts]
    with futures.ThreadPoolExecutor() as executor:
        res = executor.map(ping2, hosts, timeout=2)
    x = [i for i in res if i[0] == True]
    x.sort(key=lambda item: item[1])
    return x


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


def guess_1min_n_step(last_datetime):
    last_date = int(last_datetime // 10000)
    today = datetime.date.today()

    last_y = last_date // 10000
    last_m = last_date // 100 - last_y * 100
    last_d = last_date - (last_y * 10000 + last_m * 100)

    n = int((today - datetime.date(last_y, last_m, last_d)).days * 240 // 800)
    step = 800
    if n < 1:
        step = (today - datetime.date(last_y, last_m, last_d)).days * 240
    elif n > 99:
        n = 99

    return (n, step)


def to_pytdx_market(market):
    """转换为pytdx的market"""
    pytdx_market = {'SZ': 0, 'SH': 1, 'BJ': 2}
    return pytdx_market[market.upper()]


class TDXParams:

    #市场

    MARKET_SZ = 0  # 深圳
    MARKET_SH = 1  # 上海

    #K线种类
    # K 线种类
    # 0 -   5 分钟K 线
    # 1 -   15 分钟K 线
    # 2 -   30 分钟K 线
    # 3 -   1 小时K 线
    # 4 -   日K 线
    # 5 -   周K 线
    # 6 -   月K 线
    # 7 -   1 分钟
    # 8 -   1 分钟K 线
    # 9 -   日K 线
    # 10 -  季K 线
    # 11 -  年K 线

    KLINE_TYPE_5MIN = 0
    KLINE_TYPE_15MIN = 1
    KLINE_TYPE_30MIN = 2
    KLINE_TYPE_1HOUR = 3
    KLINE_TYPE_DAILY = 4
    KLINE_TYPE_WEEKLY = 5
    KLINE_TYPE_MONTHLY = 6
    KLINE_TYPE_EXHQ_1MIN = 7
    KLINE_TYPE_1MIN = 8
    KLINE_TYPE_RI_K = 9
    KLINE_TYPE_3MONTH = 10
    KLINE_TYPE_YEARLY = 11


    # ref : https://github.com/rainx/pytdx/issues/7
    # 分笔行情最多2000条
    MAX_TRANSACTION_COUNT = 2000
    # k先数据最多800条
    MAX_KLINE_COUNT = 800


    # 板块相关参数
    BLOCK_SZ = "block_zs.dat"
    BLOCK_FG = "block_fg.dat"
    BLOCK_GN = "block_gn.dat"
    BLOCK_DEFAULT = "block.dat"


def import_one_stock_data(api, market, ktype, code, ip, port, startDate=199012191500):
    market = market.upper()
    pytdx_market = to_pytdx_market(market)

    last_datetime =startDate

    today = datetime.date.today()
    if ktype == 'DAY':
        n, step = guess_day_n_step(last_datetime)
        pytdx_kline_type = TDXParams.KLINE_TYPE_RI_K
        today_datetime = (today.year * 10000 + today.month * 100 + today.day) * 10000

    elif ktype == '1MIN':
        n, step = guess_1min_n_step(last_datetime)
        pytdx_kline_type = TDXParams.KLINE_TYPE_1MIN
        today_datetime = (today.year * 10000 + today.month * 100 + today.day) * 10000 + 1500

    elif ktype == '5MIN':
        n, step = guess_5min_n_step(last_datetime)
        pytdx_kline_type = TDXParams.KLINE_TYPE_5MIN
        today_datetime = (today.year * 10000 + today.month * 100 + today.day) * 10000 + 1500
    else:
        return 0

    if today_datetime <= last_datetime:
        return 0

    add_record_count = 0

    rows = []
    with api.connect(ip, port):
        while n >= 0:
            print(f">>>>>> {pytdx_kline_type} --- {pytdx_market} --- {code} --- {n} --- {step}")
            bar_list = api.get_security_bars(pytdx_kline_type, pytdx_market, code, n * 800, step)
            n -= 1
            if bar_list is None:
                print(f"invalid code: {code}")
                continue

            for bar in bar_list:
                print(f">>> origin bar: {bar}")
                try:
                    tmp = datetime.date(bar['year'], bar['month'], bar['day'])
                    bar_datetime = (tmp.year * 10000 + tmp.month * 100 + tmp.day) * 10000
                    if ktype != 'DAY':
                        bar_datetime += bar['hour'] * 100 + bar['minute']
                except Exception as e:
                    print(e)
                    continue

                if today_datetime >= bar_datetime > last_datetime \
                        and bar['high'] >= bar['open'] >= bar['low'] > 0 \
                        and bar['high'] >= bar['close'] >= bar['low'] > 0 \
                        and int(bar['vol']) != 0 and int(bar['amount']*0.001) != 0:
                    try:
                        dt = datetime.datetime.strptime(bar['datetime'], '%Y-%m-%d %H:%M')
                        row = [f'{code}.{market}', dt, bar['datetime'], bar['open'], bar['high'], bar['low'], bar['close'], bar['amount'], bar['vol']]
                        # row['datetime'] = bar_datetime
                        # row['open'] = bar['open'] * 1000
                        # row['high'] = bar['high'] * 1000
                        # row['low'] = bar['low'] * 1000
                        # row['close'] = bar['close'] * 1000
                        # row['amount'] = round(bar['amount'] * 0.001)
                        # row['vol'] = round(bar['vol'])
                        # print(f">>> bar: {row}")
                        rows.append(row)
                        add_record_count += 1
                    except Exception as e:
                        print(e)
                    last_datetime = bar_datetime

    df = pd.DataFrame(rows, columns=['symbol', 'dt', 'datetime', 'open', 'high', 'low', 'close', 'amount', 'vol'])
    print(">>>>>> head ")
    print(df.head())
    print(">>>>>> tail")
    print(df.tail())
    df.to_parquet(os.path.join('/Users/equation42/Desktop/CZSC/hikyuu', f'{code}.{market}.parquet'))
    print(f">>>>>> {len(rows)}")

    return add_record_count



# class ProgressBar:
#     def __init__(self, src):
#         self.src = src

#     def __call__(self, cur, total):
#         progress = (cur + 1) * 100 // total
#         # hku_info(f"{self.src.market} {self.src.ktype} 数据: {progress}%")
#         self.src.queue.put([self.src.task_name, self.src.market, self.src.ktype, progress, 0])


class ImportPytdxToH5:
    def __init__(self, config, market, ktype, ip, port, dest_dir, start_datetime):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.task_name = 'IMPORT_KDATA'
        self.config = config
        self.market = market
        self.ktype = ktype
        self.ip = ip
        self.port = port
        self.dest_dir = dest_dir
        self.startDatetime = start_datetime

    @hku_catch(trace=True)
    def __call__(self):
        count = 0
        try:
            api = TdxHq_API()
            count = import_one_stock_data(api, self.market, self.ktype, 
                                          self.config['code'], self.ip, self.port, self.startDatetime)
        except Exception as e:
            print(e)


class UsePytdxImportToH5Thread():

    def __init__(self):
        self.config = {
            'code': '300641',
            'ktype': {
                'day': False,
                'min5': False,
                'min': True,
                'trans': False,
                'time': False,
                'min_start_date': '2024-03-01',
                'day_start_date': '2024-01-01',
            },
            'pytdx': {
                'use_tdx_number': 10
            },
            'dest_dir': '/Users/equation42/Desktop/CZSC/hikyuu'
        }

        self.process_list = []
        self.hosts = []
        self.tasks = []


    def __del__(self):
        for p in self.process_list:
            if p.is_alive():
                p.terminate()


    def init_task(self):
        self.tasks = []

        task_count = 0
        market_count = len(g_market_list)
        if self.config['ktype']['day']:
            task_count += market_count
        if self.config['ktype']['min5']:
            task_count += market_count
        if self.config['ktype']['min']:
            task_count += market_count
        if self.config['ktype']['trans']:
            task_count += market_count
        if self.config['ktype']['time']:
            task_count += market_count

        print('搜索通达信服务器')
        self.hosts = search_best_tdx()
        if not self.hosts:
            print('无法连接通达信行情服务器！请检查网络设置！')
            return

        print(f"hosts: {self.hosts}")

        if task_count == 0:
            print('未选择需要导入的行情数据！')
            return

        use_tdx_number = min(
            task_count, len(self.hosts),
            self.config['pytdx']['use_tdx_number'])
        split = task_count // use_tdx_number
        use_hosts = []
        for i in range(use_tdx_number):
            for j in range(split):
                use_hosts.append((self.hosts[i][2], self.hosts[i][3]))
        if len(use_hosts) < task_count:
            for i in range(task_count - len(use_hosts)):
                use_hosts.insert(0, (self.hosts[0][2], self.hosts[0][3]))


        cur_host = 0

        if self.config['ktype']['min']:
            start_date = datetime.datetime.strptime(self.config['ktype']['min_start_date'], '%Y-%m-%d').date()
            for market in [MARKET.SZ]:
                self.tasks.append(
                    ImportPytdxToH5(
                        self.config, market,
                        '1MIN', use_hosts[cur_host][0],
                        use_hosts[cur_host][1], self.config['dest_dir'],
                        start_date.year * 100000000 +
                        start_date.month * 1000000 + start_date.day * 10000))
                cur_host += 1

        # if self.config['ktype']['day']:
        #     start_date = datetime.datetime.strptime(self.config['ktype']['day_start_date'], '%Y-%m-%d').date()
        #     for market in g_market_list:
        #         self.tasks.append(
        #             ImportPytdxToH5(
        #                 self.log_queue, self.queue, self.config, market, 'DAY',
        #                 self.quotations, use_hosts[cur_host][0],
        #                 use_hosts[cur_host][1], dest_dir,
        #                 start_date.year * 100000000 +
        #                 start_date.month * 1000000 + start_date.day * 10000))
        #         cur_host += 1

    def run(self):
        try:
            self.init_task()
            self._run()
        except Exception as e:
            print(e)


    @hku_catch(trace=True, re_raise=True)
    def _run(self):
        # create_database(connect)

        # pytdx_api = TdxHq_API()
        # hku_check(pytdx_api.connect(self.hosts[0][2], self.hosts[0][3]),
        #           "failed connect pytdx {}:{}", self.hosts[0][2],
        #           self.hosts[0][3])

        # self.logger.info("导入交易所休假日历")
        # import_new_holidays(connect)

        for task in self.tasks:
            p = Process(target=task)
            self.process_list.append(p)
            p.start()


if __name__ == '__main__':
    task = UsePytdxImportToH5Thread()
    task.run()
