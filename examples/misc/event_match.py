# -*- coding: utf-8 -*-
import sys

sys.path.insert(0, '..')
sys.path.insert(0, '../..')

import os
import pandas as pd
from copy import deepcopy
from loguru import logger
from czsc.objects import Event
from typing import List, Dict, Callable, Any
from czsc.traders.sig_parse import get_signals_freqs
from czsc.traders.base import generate_czsc_signals
from czsc.utils import save_json
from concurrent.futures import ProcessPoolExecutor, as_completed
import tushare as ts
from czsc.connectors.ts_connector import get_raw_bars, get_symbols, dc
from czsc.connectors import research
import glob
import czsc
from utils import BaoStock

# pro = ts.pro_api(token="fc90a1fe960ad1301e7b985ce2bf1eef4ac20483edc12f87f54dd3fb")

# 首次使用，需要设置 Tushare 的 token，注意，每台电脑只需要执行一次即可，token 会保存在本地文件中
# 没有 token 的用户，可以点击 https://tushare.pro/register?reg=7 进行注册
# import tushare as ts
# ts.set_token("fc90a1fe960ad1301e7b985ce2bf1eef4ac20483edc12f87f54dd3fb")

# 如果是每天选股，需要执行以下代码先清空缓存，否则会导致选股结果不更新
# dc.clear()

cache_path = os.environ.get('czsc_research_cache', r"D:\CZSC投研数据")
if not os.path.exists(cache_path):
    raise ValueError(f"请设置环境变量 czsc_research_cache 为投研共享数据的本地缓存路径，当前路径不存在：{cache_path}。\n\n"
                     f"投研数据共享说明（含下载地址）：https://s0cqcxuy3p.feishu.cn/wiki/wikcnzuPawXtBB7Cj7mqlYZxpDh")


# 将当前工作目录添加到系统路径中
# sys.path.append(os.getcwd())
sys.path.append('/Users/equation42/Hogwarts/GitHub/lvhuantian/czsc/examples')



class EventMatchSensor:
    def __init__(self, events: List[Dict[str, Any] | Event], symbols: List[str], read_bars: Callable, **kwargs) -> None:
        """
        事件匹配传感器

        :param event: 事件配置
        :param symbols: 事件匹配的标的
        :param read_bars: 读取K线数据的函数，函数签名如下：
            read_bars(symbol, freq, sdt, edt, fq='前复权', **kwargs) -> List[RawBar]

        :param kwargs: 读取K线数据的函数的参数

            - bar_sdt: K线数据的开始时间，格式：2020-01-01
            - sdt: 事件匹配的开始时间，格式：2020-01-01
            - edt: 事件匹配的结束时间，格式：2020-01-01
            - max_workers: 读取K线数据的函数的最大进程数
            - signals_module: 信号解析模块，如：czsc.signals
            - results_path: 事件匹配结果的保存路径

        """
        self.symbols = symbols
        self.read_bars = read_bars
        self.events = [Event.load(event) if isinstance(event, dict) else event for event in events]
        self.events_map = {event.name: event for event in self.events}
        self.events_name = [event.name for event in self.events]
        self.results_path = kwargs.pop("results_path")
        if os.path.exists(self.results_path):
            logger.warning(f"文件夹 {self.results_path} 已存在，程序将覆盖该文件夹下的所有文件")
        os.makedirs(self.results_path, exist_ok=True)
        save_json({e.name: e.dump() for e in self.events}, os.path.join(self.results_path, "events.json"))

        logger.add(os.path.join(self.results_path, "event_match_sensor.log"), rotation="1 day", encoding="utf-8")
        logger.info(f"事件匹配传感器初始化，共有{len(self.events)}个事件，{len(self.symbols)}个标的")

        self.signals_module = kwargs.pop("signals_module", "misc.signals")
        self.signals_config = self._get_signals_config()
        self.freqs = get_signals_freqs(self.signals_config)
        self.base_freq = self.freqs[0]
        logger.info(
            f"signals_moudle: {self.signals_module}, signals_config: {self.signals_config}, freqs: {self.freqs}"
        )

        self.bar_sdt = kwargs.pop("bar_sdt", "2017-01-01")
        self.sdt = kwargs.pop("sdt", "2018-01-01")
        self.edt = kwargs.pop("edt", "2022-01-01")
        logger.info(f"bar_sdt: {self.bar_sdt}, sdt: {self.sdt}, edt: {self.edt}")

        self.kwargs = kwargs
        # TODO cache_path这块重构一下
        self.kwargs['cache_path'] = cache_path
        logger.info(f"事件匹配传感器初始化完成，共有{len(self.events)}个事件，{len(self.symbols)}个标的")
        self.data = self._multi_symbols(self.symbols, max_workers=self.kwargs.pop("max_workers", 1))
        self.data.to_feather(os.path.join(self.results_path, "data.feather"))

        _res = []
        for event_name in self.events_name:
            df = self.get_event_csc(event_name)
            df = df.set_index("dt")
            _res.append(df)
        df = pd.concat(_res, axis=1, ignore_index=False).reset_index()
        file_csc = os.path.join(self.results_path, f"cross_section_counts.xlsx")
        df.to_excel(file_csc, index=False)
        logger.info(f"截面匹配次数计算完成，结果保存至：{file_csc}")

        # csc = cross section count，表示截面匹配次数
        self.csc = df

    def _get_signals_config(self):
        config = []
        for event in self.events:
            _c = event.get_signals_config(signals_module=self.signals_module)
            config.extend(_c)
        config = [dict(t) for t in {tuple(d.items()) for d in config}]
        return config

    def _single_symbol(self, symbol):
        """单个symbol的事件匹配"""
        try:
            bars = self.read_bars(symbol, freq=self.base_freq, sdt=self.bar_sdt, edt=self.edt, **self.kwargs)
            sigs = generate_czsc_signals(bars, deepcopy(self.signals_config), sdt=self.sdt, df=True)
            events = deepcopy(self.events)
            new_cols = []
            for event in events:
                e_name = event.name
                sigs[[e_name, f'{e_name}_F']] = sigs.apply(event.is_match, axis=1, result_type="expand")  # type: ignore
                new_cols.extend([e_name, f'{e_name}_F'])

            sigs = sigs[['symbol', 'dt', 'open', 'close', 'high', 'low', 'vol', 'amount'] + new_cols]  # type: ignore
            return sigs
        except Exception as e:
            logger.error(f"{symbol} 事件匹配失败：{e}")
            return pd.DataFrame()

    def _multi_symbols(self, symbols: List[str], max_workers=1):
        """多个symbol的事件匹配"""
        logger.info(f"开始事件匹配，共有{len(symbols)}个标的，max_workers={max_workers}")
        dfs = []
        if max_workers == 1:
            for symbol in symbols:
                dfs.append(self._single_symbol(symbol))
        else:
            with ProcessPoolExecutor(max_workers) as executor:
                futures = [executor.submit(self._single_symbol, symbol) for symbol in symbols]
                for future in as_completed(futures):
                    dfs.append(future.result())
        df = pd.concat(dfs, ignore_index=True)
        for event_name in self.events_name:
            df[event_name] = df[event_name].astype(int)
        return df

    def get_event_csc(self, event_name: str):
        """获取事件的截面匹配次数

        csc = cross section count，表示截面匹配次数

        :param event_name: 事件名称
        :return: DataFrame
        """
        df = self.data.copy()
        df = df[df[event_name] == 1]
        df = df.groupby(["symbol", "dt"])[event_name].sum().reset_index()
        df = df.groupby("dt")[event_name].sum().reset_index()
        return df


def use_event_matcher():
    from czsc.connectors.research import get_raw_bars, get_symbols

    symbols = research.get_symbols('baostock')

    events = [
        {
            "name": "三均线",
            "operate": "开多",
            # "signals_not": [
            #     "日线_D1_涨跌停V230331_跌停_任意_任意_0",
            #     "日线_D1_涨跌停V230331_涨停_任意_任意_0"
            # ],
            "factors": [
                {
                    "signals_all": [f"周线_D1T100#SMA#5#10#20_JX辅助V240616_多头_强势_任意_0",
                                    "5分钟_T1445#1450_买区间V240414_是_任意_任意_0"],
                    "signals_any": [],
                    "signals_not": [f"日线_D1_涨跌停V230331_涨停_任意_任意_0"],
                }
            ],
        },
        {
            "name": "向上笔",
            "operate": "开多",
            "factors": [
                {
                    "signals_all": [f"日线_D3B_笔向上_是_向上_任意_0",
                                    "5分钟_T1445#1450_买区间V240414_是_任意_任意_0"],
                    "signals_any": [],
                    "signals_not": [f"日线_D1_涨跌停V230331_涨停_任意_任意_0"],
                }
            ],
        },
    ]


    ems_params = {
        "events": events,
        "symbols": symbols,
        "read_bars": BaoStock.get_raw_bars_baostock,
        "bar_sdt": "2023-03-01",
        "sdt": "2024-04-09",
        "edt": "2024-07-03",
        "max_workers": 2,
        "results_path": r"/Users/equation42/Desktop/CZSC/ems_test",
    }

    ems = EventMatchSensor(**ems_params)


if __name__ == '__main__':
    use_event_matcher()
