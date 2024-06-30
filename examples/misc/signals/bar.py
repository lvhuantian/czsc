# -*- coding: utf-8 -*-
"""
扩展的信号函数
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List
from loguru import logger
from deprecated import deprecated
from collections import OrderedDict
from czsc import envs, CZSC, Signal
from czsc.traders.base import CzscSignals
from czsc.objects import RawBar
from czsc.utils.sig import check_pressure_support
from czsc.signals.tas import update_ma_cache
from czsc.utils.bar_generator import freq_end_time
from czsc.utils import single_linear, freq_end_time, get_sub_elements, create_single_signal
import threading


holding_stocks = {}
lock =  threading.RLock()
def update_holding_stocks(code: str, dt: datetime):
    lock.acquire()
    try:
        holding_stocks[code] = dt
    except Exception as e:
        print(e)
    finally:
        lock.release()

def get_holding_stocks():
    lock.acquire()
    try:
        if len(holding_stocks.keys()) == 0:
            return None, None
        k = list(holding_stocks.keys())[0]
        return k, holding_stocks[k]
    except Exception as e:
        print(e)
    finally:
        lock.release()
    return None, None


def bar_triple_V240413(c: CZSC, **kwargs) -> OrderedDict:
    """三K加速形态配合成交量变化

    参数模板："{freq}_D{di}三K加速_裸K形态V240413"

     **信号逻辑：**

    1. 连续三根阳线，【三连涨】，如果高低点不断创新高，【新高涨】
    2. 连续三根阴线，【三连跌】，如果高低点不断创新低，【新低跌】
    3. 加入成交量变化的判断，成交量逐渐放大 或 成交量逐渐缩小

     **信号列表：**

    - Signal('日线_D1三K加速_裸K形态V240413_三连涨_依次放量_任意_0')
    - Signal('日线_D1三K加速_裸K形态V240413_其他_其他_任意_0')

    :param c: CZSC对象
    :param kwargs: 参数字典
     :return: 返回信号结果


     涨-涨-涨-跌-跌-涨
    """
    di = int(kwargs.get("di", 1))
    freq = c.freq.value
    k1, k2, k3 = f"{freq}_D{di}三K加速_裸K形态V240413".split('_')
    v1 = '其他'

    # if len(c.bars_raw) < 7:
    #     return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    bars = get_sub_elements(c.bars_raw, di=di, n=6)
    b1 = bars[0]
    b2 = bars[1]
    b3 = bars[2]
    b4 = bars[3]
    b5 = bars[4]
    b6 = bars[5]

    v1 = '其他'
    v2 = '其他'

    if b1.high < b2.high and b2.high < b3.high and \
       b4.high < b3.high and b4.open > b4.close and \
       b5.high < b4.high and b5.open > b5.close and \
       b4.vol < b3.vol and b5.vol < b3.vol and \
       b6.high > b3.high and \
       b6.vol > b4.vol and b6.vol > b5.vol:
        v1 = '三连涨'
        v2 = '依次放量'

        update_holding_stocks(b6.symbol, b6.dt)

        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2)
    else:
        v1 = '其他'
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2)


    # if b1.close > b1.open and b2.close > b2.open and b3.close > b3.open:
    #     v1 = '三连涨'
    #     if b1.high > b2.high > b3.high and b1.low > b2.low > b3.low:
    #         v1 = "新高涨"

    # if b1.close < b1.open and b2.close < b2.open and b3.close < b3.open:
    #     v1 = '三连跌'
    #     if b1.high < b2.high < b3.high and b1.low < b2.low < b3.low:
    #         v1 = "新低跌"

    # if v1 == '其他':
    #     return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    # if b1.vol > b2.vol > b3.vol:
    #     v2 = '依次放量'
    # elif b1.vol < b2.vol < b3.vol:
    #     v2 = '依次缩量'
    # else:
    #     v2 = '量柱无序'

    # return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2)


def bar_closing_buy_V240414(c: CZSC, **kwargs) -> OrderedDict:
    """日内操作时间区间，c 必须是基础周期的 CZSC 对象

    参数模板："{freq}_T{t1}#{t2}_买区间V240414"

    **信号列表：**

    - Signal('5分钟_T1445#1450_买区间V240414_是_任意_任意_0')
    - Signal('5分钟_T1445#1450_买区间V240414_否_任意_任意_0')

    :param c: 基础周期的 CZSC 对象
    :return: s
    """
    t1 = kwargs.get("t1", "1445")
    t2 = kwargs.get("t2", "1450")
    freq = c.freq.value
    k1, k2, k3 = f"{freq}_T{t1}#{t2}_买区间V240414".split("_")

    v = "否"
    dt: datetime = c.bars_raw[-1].dt
    if t1 <= dt.strftime("%H%M") < t2:
        v = "是" 
    else:
        v = "否"
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)


def bar_closing_sell_V240414(c: CZSC, **kwargs) -> OrderedDict:
    """日内操作时间区间，c 必须是基础周期的 CZSC 对象

    参数模板："{freq}_T{t1}#{t2}_卖区间V240414"

    **信号列表：**

    - Signal('5分钟_T1445#1450_卖区间V240414_是_任意_任意_0')
    - Signal('5分钟_T1445#1450_卖区间V240414_否_任意_任意_0')

    :param c: 基础周期的 CZSC 对象
    :return: s
    """
    t1 = kwargs.get("t1", "1445")
    t2 = kwargs.get("t2", "1450")
    freq = c.freq.value
    k1, k2, k3 = f"{freq}_T{t1}#{t2}_卖区间V240414".split("_")

    v = "否"
    dt: datetime = c.bars_raw[-1].dt
    if t1 <= dt.strftime("%H%M") < t2:
        _, b_dt = get_holding_stocks()
        if b_dt:
            if dt.strftime("%Y%m%d") >= b_dt.strftime("%Y%m%d"):
                v = "是" 
    else:
        v = "否"
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v)


def tas_double_ma_V240616(c: CZSC, **kwargs) -> OrderedDict:
    """双均线多空和强弱信号

    参数模板："{freq}_D{di}T{th}#{ma_type}#{timeperiod1}#{timeperiod2}#{timeperiod3}_JX辅助V240616"

    **信号逻辑：**

    1. ma1 > ma2，多头；反之，空头
    2. ma1 离开 ma2 的距离大于 th，强势；反之，弱势

    **信号列表：**

    - Signal('周线_D1T100#SMA#5#10#20_JX辅助V240616_多头_强势_任意_0')
    - Signal('周线_D1T100#SMA#5#10#20_JX辅助V240616_其他_其他_任意_0')

    :param c: CZSC对象
    :param di: 信号计算截止倒数第i根K线
    :param ma_type: 均线类型，必须是 `ma_type_map` 中的 key
    :param ma_seq: 快慢均线计算周期，快线在前
    :param th: ma1 相比 ma2 的距离阈值，单位 BP
    :return: 信号识别结果
    """
    di = int(kwargs.get('di', 1))
    th = int(kwargs.get('th', 100))
    ma_type = kwargs.get('ma_type', 'SMA').upper()
    timeperiod1 = int(kwargs.get('timeperiod1', 5))
    timeperiod2 = int(kwargs.get('timeperiod2', 10))
    timeperiod3 = int(kwargs.get('timeperiod3', 20))

    # assert timeperiod1 < timeperiod2, "快线周期必须小于慢线周期"
    ma1 = update_ma_cache(c, ma_type=ma_type, timeperiod=timeperiod1)
    ma2 = update_ma_cache(c, ma_type=ma_type, timeperiod=timeperiod2)
    ma3 = update_ma_cache(c, ma_type=ma_type, timeperiod=timeperiod3)

    k1, k2, k3 = f"{c.freq.value}_D{di}T{th}#{ma_type}#{timeperiod1}#{timeperiod2}#{timeperiod3}_JX辅助V240616".split('_')
    bars = get_sub_elements(c.bars_raw, di=di, n=3)
    ma01v = bars[0].cache[ma1]
    ma02v = bars[0].cache[ma2]
    ma03v = bars[0].cache[ma3]

    ma11v = bars[1].cache[ma1]
    ma12v = bars[1].cache[ma2]
    ma13v = bars[1].cache[ma3]

    ma21v = bars[2].cache[ma1]
    ma22v = bars[2].cache[ma2]
    ma23v = bars[2].cache[ma3]

    if np.isnan(ma01v) or np.isnan(ma02v) or np.isnan(ma03v) \
       or np.isnan(ma11v) or np.isnan(ma12v) or np.isnan(ma13v) \
       or np.isnan(ma21v) or np.isnan(ma22v) or np.isnan(ma23v):
        logger.warning("没有足够的K线计算MA")

    if ma21v > ma11v > ma01v \
       and ma22v > ma12v > ma02v \
       and ma23v > ma13v > ma03v \
       and ma21v > ma22v > ma23v \
       and ma11v > ma12v > ma13v \
       and ma01v > ma02v > ma03v:
        v1 = "多头"
        v2 = "强势"
    else:
        v1 = "空头"
        v2 = "弱势"
        
    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1, v2=v2)


def bar_zdt_V230331(c: CZSC, **kwargs) -> OrderedDict:
    """计算倒数第di根K线的涨跌停信息

    参数模板："{freq}_D{di}_涨跌停V230331"

    **信号逻辑：**

    - close等于high大于等于前一根K线的close，近似认为是涨停；反之，跌停。

    **信号列表：**

    - Signal('15分钟_D1_涨跌停V230331_涨停_任意_任意_0')
    - Signal('15分钟_D1_涨跌停V230331_跌停_任意_任意_0')

    :param c: 基础周期的 CZSC 对象
    :param kwargs:
        - di: 倒数第 di 根 K 线
    :return: 信号识别结果
    """
    di = int(kwargs.get("di", 1))
    freq = c.freq.value
    k1, k2, k3 = f"{freq}_D{di}_涨跌停V230331".split("_")
    v1 = "其他"
    if len(c.bars_raw) < di + 2:
        return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)

    b1, b2 = c.bars_raw[-di], c.bars_raw[-di - 1]
    if b1.close == b1.high >= b2.close:
        v1 = "涨停"
    elif b1.close == b1.low <= b2.close:
        v1 = "跌停"

    return create_single_signal(k1=k1, k2=k2, k3=k3, v1=v1)
