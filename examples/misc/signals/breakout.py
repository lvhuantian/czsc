# -*- coding: utf-8 -*-
"""
扩展的信号函数
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List
from deprecated import deprecated
from collections import OrderedDict
import threading
from loguru import logger

from utils import get_signal_detail

from czsc import envs, CZSC, Signal
from czsc.traders.base import CzscSignals
from czsc.objects import RawBar
from czsc.utils.sig import check_pressure_support
from czsc.signals.tas import update_ma_cache
from czsc.utils.bar_generator import freq_end_time
from czsc.utils import single_linear, freq_end_time, get_sub_elements, create_single_signal
from czsc.objects import BI, ZS
from czsc.enum import Direction


# 5min一个交易日48根K线
# K线可能存在缺失，直接使用48根K线作为当天的所有K线不合适
NUM_OF_5MIN_BARS_IN_A_DAY = 48


_holding_stocks = {}
_holding_lock = threading.RLock()

def update_holding_stocks(code: str, dt: datetime):
    with _holding_lock:
        _holding_stocks[code] = dt

def get_holding_stocks(code: str):
    with _holding_lock:
        if code in _holding_stocks:
            return _holding_stocks[code]
        return None

def release_holding_stocks(code: str):
    with _holding_lock:
        if code in _holding_stocks:
            del _holding_stocks[code]


_recent_up_limit_stocks = {}
_recent_up_limit_lock = threading.RLock()

def update_recent_up_limit_stocks(code: str, dt: datetime):
    with _recent_up_limit_lock:
        _recent_up_limit_stocks[code] = dt

def get_recent_up_limit_stocks(code: str):
    with _recent_up_limit_lock:
        if code in _recent_up_limit_stocks:
            return _recent_up_limit_stocks[code]
        return None

def release_recent_up_limit_stocks(code: str):
    with _recent_up_limit_lock:
        if code in _recent_up_limit_stocks:
            del _recent_up_limit_stocks[code]


def bar_closing_sell_V240914(c: CZSC, **kwargs) -> OrderedDict:
    """日内操作时间区间，c 必须是基础周期的 CZSC 对象

    参数模板："{freq}_T{t1}#{t2}_卖区间V240914"

    **信号列表：**

    - Signal('5分钟_T1455#1500_卖区间V240914_是_任意_任意_0')
    - Signal('5分钟_T1455#1500_卖区间V240914_否_任意_任意_0')

    :param c: 基础周期的 CZSC 对象
    :return: s
    """
    t1 = kwargs.get("t1", "1455")
    t2 = kwargs.get("t2", "1500")
    freq = c.freq.value
    k1, k2, k3 = f"{freq}_T{t1}#{t2}_卖区间V240914".split("_")

    v = "否"
    dt: datetime = c.bars_raw[-1].dt
    symbol: str = c.bars_raw[-1].symbol
    if t1 < dt.strftime("%H%M") <= t2:
        holding_dt = get_holding_stocks(symbol)
        if holding_dt:
            if dt.strftime("%Y%m%d") > holding_dt.strftime("%Y%m%d"):
                v = "是" 
                release_holding_stocks(symbol)
                release_recent_up_limit_stocks(symbol)
                logger.info(f"[sell] release holding stocks, symbol: {symbol}, dt: {dt}")
                logger.info(f"[sell] release recent up limit stocks, symbol: {symbol}, dt: {dt}")
                if hit_up_limit(c):
                    # 只针对5min，当前是15:00的K线，使用9:35的K线进行更新
                    day_bars = get_5min_day_bars(c)
                    update_recent_up_limit_stocks(day_bars[0].symbol, day_bars[0].dt)
                    logger.info(f"[sell] update recent up limit stocks, symbol: {symbol}, dt: {dt}")
    else:
        v = "否"
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)


def get_5min_day_bars(c: CZSC) -> List[RawBar]:
    """获取当天的所有5分钟K线"""
    cur_bar = c.bars_raw[-1]
    day_bars = []
    if cur_bar.dt.strftime("%H%M") == "1500":
        i = 1
        while c.bars_raw[-i].dt.strftime("%Y%m%d") == cur_bar.dt.strftime("%Y%m%d"):
            day_bars.append(c.bars_raw[-i])
            i += 1
        day_bars = day_bars[::-1]
    else:
        return day_bars

    if len(day_bars) < NUM_OF_5MIN_BARS_IN_A_DAY:
        logger.warning(f"[不足{NUM_OF_5MIN_BARS_IN_A_DAY}根K线]code: {cur_bar.symbol} {cur_bar.dt}")
    elif len(day_bars) > NUM_OF_5MIN_BARS_IN_A_DAY:
        logger.error(f"[超过{NUM_OF_5MIN_BARS_IN_A_DAY}根K线]code: {cur_bar.symbol} {cur_bar.dt}")
        raise ValueError(f"[超过{NUM_OF_5MIN_BARS_IN_A_DAY}根K线]code: {cur_bar.symbol} {cur_bar.dt}")

    return day_bars


def hit_up_limit(c: CZSC) -> bool:
    """只适用于5分钟K线"""

    cur_bar = c.bars_raw[-1]
    day_bars = get_5min_day_bars(c)
    if len(day_bars) == 0:
        return False
    # if cur_bar.dt.strftime("%H%M") == "1500":
    #     # day_bars = get_sub_elements(c.bars_raw, di=1, n=48)
    #     i = 1
    #     while c.bars_raw[-i].dt.strftime("%Y%m%d") == cur_bar.dt.strftime("%Y%m%d"):
    #         day_bars.append(c.bars_raw[-i])
    #         i += 1

    #     day_bars = day_bars[::-1]
    # else:
    #     return False

    # if len(day_bars) < NUM_OF_5MIN_BARS_IN_A_DAY:
    #     logger.warning(f"[不足{NUM_OF_5MIN_BARS_IN_A_DAY}根K线]code: {cur_bar.symbol} {cur_bar.dt}")
    # elif len(day_bars) > NUM_OF_5MIN_BARS_IN_A_DAY:
    #     logger.error(f"[超过{NUM_OF_5MIN_BARS_IN_A_DAY}根K线]code: {cur_bar.symbol} {cur_bar.dt}")
    #     raise ValueError(f"[超过{NUM_OF_5MIN_BARS_IN_A_DAY}根K线]code: {cur_bar.symbol} {cur_bar.dt}")

    day_high = max([x.high for x in day_bars])
    # 上一个交易日的最后一根K线
    last_day_end_bar = c.bars_raw[-len(day_bars)-1]
    if cur_bar.close == day_high and cur_bar.close > last_day_end_bar.close:
        incr = (cur_bar.close - last_day_end_bar.close) / last_day_end_bar.close
        # 主板 创业板 北交所
        # TODO ST 如何计算
        if round(incr, 2) not in [0.1, 0.2, 0.3]:
            return False
        else:
            return True
    return False


def record_signal_detail(c: CZSC):
    """记录信号细节"""

    # 当前第一根K线
    cur_bar = c.bars_raw[-1]

    last_day_end_bar = c.bars_raw[-2]

    incr = (cur_bar.open - last_day_end_bar.close) / last_day_end_bar.close

    recent_up_limit_dt = get_recent_up_limit_stocks(cur_bar.symbol)

    i = 1
    days = 1
    vol = 0
    while c.bars_raw[-2-i].dt.strftime("%Y%m%d") >= recent_up_limit_dt.strftime("%Y%m%d"):
        vol += c.bars_raw[-2-i].amount
        if c.bars_raw[-2-i].dt.strftime("%H%M") == "0935":
            days += 1
        i += 1
        if len(c.bars_raw) <= 3+i:
            logger.error(f">>>debug>>> 没有找到足够的K线 code: {cur_bar.symbol} {c.bars_raw[-1-i].dt}")
            break

    avg_vol = vol / days

    # TODO 根据横盘调整时间长短进行推荐

    k = cur_bar.dt.strftime("%Y%m%d") + "#" + cur_bar.symbol
    signal_detail = get_signal_detail()
    if k not in signal_detail:
        signal_detail[k] = {
            "code": cur_bar.symbol,
            "avg_vol": avg_vol,
            "incr": incr,
            "recent_up_limit_dt": recent_up_limit_dt,
            "fluctuate_days": days,
        }


def check_higher_than_last_day(c: CZSC) -> bool:
    # 使用当天的第一根K线进行判断
    cur_bar = c.bars_raw[-1]
    if cur_bar.dt.strftime("%H%M") != "0935":
        return False

    # 上一根K线是收盘价
    assert c.bars_raw[-2].dt.strftime("%H%M") == "1500"
    last_day_end_bar = c.bars_raw[-2]

    # 上一个交易日的第一根K线
    last_day_start_bar = None
    i = 1
    while c.bars_raw[-2-i].dt.strftime("%Y%m%d") == c.bars_raw[-2].dt.strftime("%Y%m%d"):
        if c.bars_raw[-2-i].dt.strftime("%H%M") == "0935":
            last_day_start_bar = c.bars_raw[-2-i]
            break
        i += 1

    recent_up_limit_dt = get_recent_up_limit_stocks(cur_bar.symbol)

    if last_day_start_bar is None or recent_up_limit_dt is None:
        return False

    if last_day_start_bar.dt.strftime("%Y%m%d") == recent_up_limit_dt.strftime("%Y%m%d"):
        return False

    if cur_bar.dt.strftime("%Y%m%d") <= (recent_up_limit_dt + timedelta(days=20)).strftime("%Y%m%d"):
        # 前一天是阴线
        if last_day_end_bar.close < last_day_start_bar.open and cur_bar.open > last_day_start_bar.open and \
                (cur_bar.open - last_day_start_bar.open) / last_day_start_bar.open >= 0.01:
            update_holding_stocks(cur_bar.symbol, cur_bar.dt)
            record_signal_detail(c)
            return True

        # 前一天是阳线
        if last_day_end_bar.close > last_day_start_bar.open and cur_bar.open > last_day_end_bar.close and \
                (cur_bar.open - last_day_end_bar.close) / last_day_end_bar.close >= 0.01:
            update_holding_stocks(cur_bar.symbol, cur_bar.dt)
            record_signal_detail(c)
            return True
    
    return False


def bar_fluctuate_breakout(c: CZSC, **kwargs) -> OrderedDict:
    """横盘震荡突破

    参数模板："{freq}_T0935#1500_横盘震荡突破"

    **信号列表：**

    - Signal('5分钟_T0935#1500_横盘震荡突破_是_任意_任意_0')
    - Signal('5分钟_T0935#1500_横盘震荡突破_否_任意_任意_0')

    :param c: 基础周期的 CZSC 对象
    :return: s
    """

    # 涨停之后调整震荡
    # 目前只考虑5min的K线，当前的第一个K线9:35，最后一个K线15:00
    # 5min一天共48个K线
    # 考虑假期不能使用day+1计算下一个交易日
    #
    # 从涨停之后开始计算
    # TODO bar_zdt_V230331这个计算有些粗糙，考虑比较前一天的日K
    #

    k1, k2, k3 = f"5分钟_T0935#1500_横盘震荡突破".split("_")
    v = "否"

    cur_bar = c.bars_raw[-1]

    holding_dt = get_holding_stocks(cur_bar.symbol)
    if holding_dt:
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)

    recent_up_limit_dt = get_recent_up_limit_stocks(cur_bar.symbol)

    if not recent_up_limit_dt:
        v = "否"
        if hit_up_limit(c):
            update_recent_up_limit_stocks(cur_bar.symbol, cur_bar.dt)
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)


    day_bars = get_5min_day_bars(c)
    # 上一个交易日的最后一根K线
    last_day_end_bar = c.bars_raw[-len(day_bars)-1]

    # print(f">>>debug>>> {cur_bar.dt}")

    if hit_up_limit(c):
        # 连续涨停，跳过
        if last_day_end_bar.dt.strftime("%Y%m%d") == recent_up_limit_dt.strftime("%Y%m%d"):
            update_recent_up_limit_stocks(cur_bar.symbol, cur_bar.dt)
            v = "否"
            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)
        elif last_day_end_bar.dt.strftime("%Y%m%d") > recent_up_limit_dt.strftime("%Y%m%d"):
            # 和上一个交易日的K线比较
            if check_higher_than_last_day(c):
                v = "是"
                logger.info(f"[buy] code: {cur_bar.symbol}, dt: {cur_bar.dt}, recent_up_limit_dt: {recent_up_limit_dt}")
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)
            else:
                v = "否"
                return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)
        else:
            v = "否"
            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)
    else:
        # 涨停后第二个交易日非涨停，看下一个交易日的K线
        if last_day_end_bar.dt.strftime("%Y%m%d") == recent_up_limit_dt.strftime("%Y%m%d"):
            v = "否"
            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)
        # 和上一个交易日的K线比较
        if check_higher_than_last_day(c):
            v = "是"
            logger.info(f"[buy] code: {cur_bar.symbol}, dt: {cur_bar.dt}, recent_up_limit_dt: {recent_up_limit_dt}")
            return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)

    v = "否"
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)
