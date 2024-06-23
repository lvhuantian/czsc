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


class TDXParams:

    # 市场

    MARKET_SZ = 0  # 深圳
    MARKET_SH = 1  # 上海

    # K线种类
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
