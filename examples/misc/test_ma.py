# -*- coding: utf-8 -*-
import os
import sys
from pathlib import Path
import glob

import czsc
from loguru import logger
from czsc.connectors import research
from czsc import Event, Position
import pandas as pd


cache_path = os.environ.get('czsc_research_cache', r"D:\CZSC投研数据")
if not os.path.exists(cache_path):
    raise ValueError(f"请设置环境变量 czsc_research_cache 为投研共享数据的本地缓存路径，当前路径不存在：{cache_path}。\n\n"
                     f"投研数据共享说明（含下载地址）：https://s0cqcxuy3p.feishu.cn/wiki/wikcnzuPawXtBB7Cj7mqlYZxpDh")


# 将当前工作目录添加到系统路径中
# sys.path.append(os.getcwd())
sys.path.append('/Users/equation42/Hogwarts/GitHub/lvhuantian/czsc/examples')


def create_long_short(symbol, **kwargs):
    """
    三均线

    使用的信号函数：

    """

    opens = [
        {
            "operate": "开多",
            "signals_all": [],
            "signals_any": [],
            "signals_not": [],
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
            "operate": "开空",
            "signals_all": [],
            "signals_any": [],
            "signals_not": [],
            "factors": [
                {
                    "signals_all": [f"周线_D1T100#SMA#5#10#20_JX辅助V240616_其他_其他_任意_0",
                                    "5分钟_T1445#1450_卖区间V240414_是_任意_任意_0"],
                    "signals_any": [],
                    "signals_not": [f"日线_D1_涨跌停V230331_跌停_任意_任意_0"],
                }
            ],
        },
    ]

    exits = []

    pos = Position(
        name=f"三均线",
        symbol=symbol,
        opens=[Event.load(x) for x in opens],
        exits=[Event.load(x) for x in exits],
        interval=3600 * 12,
        timeout=16 * 30,
        stop_loss=500,
    )
    return pos


class Strategy(czsc.CzscStrategyBase):
    def __init__(self, **kwargs):
        kwargs['signals_module_name'] = 'misc.signals'
        super().__init__(**kwargs)
        self.is_stocks = kwargs.get('is_stocks', True)

    @property
    def positions(self):
        pos_list = [
            create_long_short(self.symbol),
        ]
        return pos_list


def get_raw_bars_baostock(symbol, freq, sdt, edt, fq='前复权', **kwargs):
    """获取 CZSC 库定义的标准 RawBar 对象列表

    :param symbol: 标的代码
    :param freq: 周期，支持 Freq 对象，或者字符串，如
            '1分钟', '5分钟', '15分钟', '30分钟', '60分钟', '日线', '周线', '月线', '季线', '年线'
    :param sdt: 开始时间
    :param edt: 结束时间
    :param fq: 除权类型，投研共享数据默认都是后复权，不需要再处理
    :param kwargs:
    :return:
    """
    # kwargs['fq'] = fq
    # if freq == "日线":
    #     file = glob.glob(os.path.join(cache_path, "stocks/day", f"{symbol}.parquet"))[0]
    # else:
    #     raise RuntimeError("Please set file path")
    #     file = glob.glob(os.path.join(cache_path, "*", f"{symbol}.parquet"))[0]
    # freq = czsc.Freq(freq)
    # kline = pd.read_parquet(file)
    # if 'dt' not in kline.columns:
    #     kline['dt'] = pd.to_datetime(kline['datetime'])
    # kline = kline[(kline['dt'] >= pd.to_datetime(sdt)) & (kline['dt'] <= pd.to_datetime(edt))]
    # if kline.empty:
    #     return []
    kwargs['fq'] = fq
    file = glob.glob(os.path.join(cache_path, "*", f"{symbol}.parquet"))[0]
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
    _bars = czsc.resample_bars(kline, freq, raw_bars=True, base_freq='15分钟')
    return _bars



if __name__ == '__main__':
    results_path = Path(r'/Users/equation42/Desktop/CZSC/result')
    logger.add(results_path / "czsc.log", rotation="1 week", encoding="utf-8")
    results_path.mkdir(exist_ok=True, parents=True)

    # symbols = research.get_symbols('hikyuu')
    symbols = research.get_symbols('baostock')
    symbol = symbols[0]
    tactic = Strategy(symbol=symbol, is_stocks=True)

    # 使用 logger 记录策略的基本信息
    logger.info(f"K线周期列表：{tactic.freqs}")
    logger.info(f"信号函数配置列表：{tactic.signals_config}")

    # replay 查看策略的编写是否正确，执行过程是否符合预期
    # bars = research.get_raw_bars(symbol, freq=tactic.base_freq, sdt='20240301', edt='20240531')
    bars = get_raw_bars_baostock(symbol, freq=tactic.base_freq, sdt='20230301', edt='20240531')
    trader = tactic.replay(bars, sdt='20240409', res_path=results_path / "replay", refresh=True)

    # 当策略执行过程符合预期后，将持仓策略保存到本地 json 文件中
    tactic.save_positions(results_path / "positions")

    # TODO 持票后隔日一字板卖不卖
    # TODO 扩展，均线多头持有
    # TODO 日志打印或记录事件买卖点、买卖价格
    # TODO 沪深300 中证2000 各个区间段的涨跌情况
