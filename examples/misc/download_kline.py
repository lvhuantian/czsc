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
from utils import search_best_tdx, hku_catch
from constants import *


# 参考Hikyuu


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


def import_one_stock_data(api, market, ktype, code, ip, port, startDate=199012191500):
    market = market.upper()
    pytdx_market = to_pytdx_market(market)

    last_datetime = startDate

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
                        and int(bar['vol']) != 0 and int(bar['amount'] * 0.001) != 0:
                    try:
                        dt = datetime.datetime.strptime(bar['datetime'], '%Y-%m-%d %H:%M')
                        row = [f'{code}.{market}', dt, bar['datetime'], bar['open'],
                               bar['high'], bar['low'], bar['close'], bar['amount'], bar['vol']]
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


class ImportPytdx:
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


class UsePytdxImportThread():

    def __init__(self):
        self.process_list = []
        self.hosts = []
        self.tasks = []
        self.config = {}

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
                    ImportPytdx(
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
        #             ImportPytdx(
        #                 self.log_queue, self.queue, self.config, market, 'DAY',
        #                 self.quotations, use_hosts[cur_host][0],
        #                 use_hosts[cur_host][1], dest_dir,
        #                 start_date.year * 100000000 +
        #                 start_date.month * 1000000 + start_date.day * 10000))
        #         cur_host += 1

    def run(self, config):
        self.config = config
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
    config = {
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

    task = UsePytdxImportThread()
    task.run(config)
