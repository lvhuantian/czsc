# -*- coding: utf-8 -*-
import os
import sys
from pathlib import Path
import glob
from multiprocessing import Pool
from tqdm import tqdm
from functools import partial

cache_path = '/Users/equation42/Desktop/CZSC2'
os.environ['czsc_research_cache'] = cache_path

from utils import BaoStock

import czsc
from loguru import logger
from czsc.connectors import research
from czsc import Event, Position
import pandas as pd


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


def run(symbol: str, bar_sdt: str, bar_edt: str, replay_sdt: str):
    results_path = Path(f'/Users/equation42/Desktop/CZSC/result/{symbol}')
    logger.add(results_path / "czsc.log", rotation="1 week", encoding="utf-8")
    results_path.mkdir(exist_ok=True, parents=True)

    tactic = Strategy(symbol=symbol, is_stocks=True)

    # 使用 logger 记录策略的基本信息
    logger.info(f"K线周期列表：{tactic.freqs}")
    logger.info(f"信号函数配置列表：{tactic.signals_config}")

    # replay 查看策略的编写是否正确，执行过程是否符合预期
    bars = BaoStock.get_raw_bars_baostock(symbol, freq=tactic.base_freq, sdt=bar_sdt, edt=bar_edt, cache_path=cache_path)
    trader = tactic.replay(bars, sdt=replay_sdt, res_path=results_path / "replay", refresh=True)

    # 当策略执行过程符合预期后，将持仓策略保存到本地 json 文件中
    tactic.save_positions(results_path / "positions")


if __name__ == '__main__':
    bar_sdt = '20240501'
    bar_edt = '20240906'
    replay_sdt = '20240501'
    symbols = research.get_symbols('baostock')


    # 设置进程数，可以根据CPU核心数进行调整
    num_processes = os.cpu_count() or 4

    # 创建一个进度条
    with tqdm(total=len(symbols), desc="Processing symbols") as pbar:
        def update_progress(*args):
            pbar.update()

        with Pool(num_processes) as pool:
            # 使用imap_unordered可以在结果ready时立即处理，提高效率
            results = pool.imap_unordered(
                partial(run, bar_sdt=bar_sdt, bar_edt=bar_edt, replay_sdt=replay_sdt),
                symbols
            )
            
            # 收集结果并更新进度条
            list(tqdm(results, total=len(symbols), desc="Processing symbols"))


    # TODO 持票后隔日一字板卖不卖
    # TODO 日志打印或记录事件买卖点、买卖价格
