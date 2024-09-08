# -*- coding: utf-8 -*-
import os
import sys
from pathlib import Path
import glob

from utils import BaoStock

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
    opens = [
        {
            "operate": "开多",
            "signals_all": [],
            "signals_any": [],
            "signals_not": [],
            "factors": [
                {
                    "signals_all": [f"5分钟_T0935#1500_横盘震荡突破_是_任意_任意_0"],
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
                    "signals_all": ["5分钟_T1455#1500_卖区间V240914_是_任意_任意_0"],
                    "signals_any": [],
                    "signals_not": [f"日线_D1_涨跌停V230331_跌停_任意_任意_0"],
                }
            ],
        },
    ]

    exits = []

    pos = Position(
        name=f"突破",
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


def run(symbol: str):
    results_path = Path(f'/Users/equation42/Desktop/CZSC/result/{symbol}')
    logger.add(results_path / "czsc.log", rotation="1 week", encoding="utf-8")
    results_path.mkdir(exist_ok=True, parents=True)

    tactic = Strategy(symbol=symbol, is_stocks=True)

    # 使用 logger 记录策略的基本信息
    logger.info(f"K线周期列表：{tactic.freqs}")
    logger.info(f"信号函数配置列表：{tactic.signals_config}")

    # replay 查看策略的编写是否正确，执行过程是否符合预期
    bars = BaoStock.get_raw_bars_baostock(symbol, freq=tactic.base_freq, sdt='20240701', edt='20240906', cache_path=cache_path)
    trader = tactic.replay(bars, sdt='20240801', res_path=results_path / "replay", refresh=True)

    # 当策略执行过程符合预期后，将持仓策略保存到本地 json 文件中
    tactic.save_positions(results_path / "positions")


if __name__ == '__main__':
    symbols = research.get_symbols('baostock')
    # for symbol in symbols:
    #     run(symbol=symbol)
    run(symbol='600552.SH')


    # TODO 持票后隔日一字板卖不卖
    # TODO 日志打印或记录事件买卖点、买卖价格
