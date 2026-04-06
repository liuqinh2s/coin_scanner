"""
合约代币扫描器 — 扫描脚本 (GitHub Actions / 本地运行)

数据源: Bitget USDT 永续合约公开 API (无需 API Key)
标签条件: 趋势共振(默认) · 成交量异动 · BTC大盘方向 · 防追高 · 资金费率
         · 龙头币 · 仙人指路 · 波动充足 · 小量大涨 · 盘整突破
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiohttp

# ── 路径 ──
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
CONFIG_PATH = ROOT / "config.local.json"

BITGET_API = "https://api.bitget.com"
PRODUCT_TYPE = "USDT-FUTURES"
CYCLES = ["1D", "4H", "1H", "15m"]

# ── 日志 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scan")

# ── 代理配置 ──
# 优先级: 环境变量 PROXY_URL > config.local.json > 无代理
proxy_url: str | None = os.environ.get("PROXY_URL")
if proxy_url:
    log.info("已启用代理 (环境变量): %s", proxy_url)
else:
    try:
        if CONFIG_PATH.exists():
            cfg = json.loads(CONFIG_PATH.read_text())
            p = cfg.get("proxy", {})
            if p.get("enabled"):
                proxy_url = f"http://{p['host']}:{p['port']}"
                log.info("已启用代理 (本地配置): %s", proxy_url)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  数据获取 (aiohttp 高并发)
# ══════════════════════════════════════════════════════════════

async def fetch_json(session: aiohttp.ClientSession, url: str) -> any:
    for attempt in range(5):
        try:
            async with session.get(url, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    return await resp.json()
                log.warning("HTTP %d: %s", resp.status, url[:80])
        except Exception as e:
            log.warning("请求失败 (%d/5) %s: %s", attempt + 1, url[:60], e)
        await asyncio.sleep(2 * (attempt + 1))
    return None


async def fetch_all_symbols(session: aiohttp.ClientSession) -> list[str]:
    """获取所有 USDT 永续合约交易对"""
    url = f"{BITGET_API}/api/v2/mix/market/tickers?productType={PRODUCT_TYPE}"
    data = await fetch_json(session, url)
    if not data or data.get("code") != "00000" or not data.get("data"):
        log.error("tickers 返回异常: %s", str(data)[:200] if data else "None")
        return []
    symbols = [item["symbol"] for item in data["data"]]
    log.info("获取到 %d 个 USDT 永续合约", len(symbols))
    return symbols


async def fetch_klines(session: aiohttp.ClientSession, symbol: str, granularity: str, limit: int = 200):
    """获取单个币种单个周期的 K 线"""
    url = (f"{BITGET_API}/api/v2/mix/market/candles"
           f"?symbol={symbol}&productType={PRODUCT_TYPE}"
           f"&granularity={granularity}&limit={limit}")
    data = await fetch_json(session, url)
    if not data or data.get("code") != "00000" or not data.get("data"):
        return symbol, granularity, []
    # Bitget 返回格式: [[ts, o, h, l, c, vol, quoteVol], ...]
    return symbol, granularity, data["data"]


async def fetch_fund_rates(session: aiohttp.ClientSession, symbols: list[str]) -> dict[str, float]:
    """逐个获取当前资金费率（Bitget 没有批量接口）"""
    sem = asyncio.Semaphore(10)
    result: dict[str, float] = {}

    async def _fetch_one(sym):
        async with sem:
            url = (f"{BITGET_API}/api/v2/mix/market/current-fund-rate"
                   f"?symbol={sym}&productType={PRODUCT_TYPE}")
            data = await fetch_json(session, url)
            if data and data.get("code") == "00000" and data.get("data"):
                result[sym] = float(data["data"].get("fundingRate", 0))

    await asyncio.gather(*[_fetch_one(s) for s in symbols], return_exceptions=True)
    return result


async def fetch_all_data(session: aiohttp.ClientSession, symbols: list[str], max_concurrent: int = 50) -> dict:
    """高并发批量获取所有币种的多周期 K 线"""
    all_sym: dict = {}
    sem = asyncio.Semaphore(max_concurrent)

    async def _limited(sym, granularity):
        async with sem:
            return await fetch_klines(session, sym, granularity)

    # 一次性创建所有任务 (symbols × cycles)，全部并发
    tasks = []
    for sym in symbols:
        for cycle in CYCLES:
            tasks.append(_limited(sym, cycle))

    total = len(tasks)
    log.info("开始并发获取 K 线: %d 个请求, 并发上限 %d", total, max_concurrent)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, tuple) and len(r) == 3:
            sym, granularity, data = r
            if data:
                all_sym.setdefault(sym, {})[granularity] = {"data": data}

    log.info("K 线获取完成: %d 个币种有数据", len(all_sym))
    return all_sym


# ══════════════════════════════════════════════════════════════
#  技术指标计算
# ══════════════════════════════════════════════════════════════

def ema(data: list[float], span: int) -> list[float]:
    k = 2 / (span + 1)
    result = [data[0]]
    for i in range(1, len(data)):
        result.append(data[i] * k + result[-1] * (1 - k))
    return result


def sma(data: list[float], window: int) -> list[float]:
    result = []
    for i in range(len(data)):
        if i < window - 1:
            result.append(float("nan"))
        else:
            result.append(sum(data[i - window + 1: i + 1]) / window)
    return result


def calc_bollinger(closes: list[float], window: int = 20, num_std: int = 2):
    mid = sma(closes, window)
    upper, lower = [], []
    for i in range(len(closes)):
        if math.isnan(mid[i]):
            upper.append(float("nan"))
            lower.append(float("nan"))
            continue
        sum_sq = sum((closes[j] - mid[i]) ** 2 for j in range(i - window + 1, i + 1))
        std = math.sqrt(sum_sq / window)
        upper.append(mid[i] + std * num_std)
        lower.append(mid[i] - std * num_std)
    return {"mid": mid, "upper": upper, "lower": lower}


def calc_macd(closes: list[float], short_span=12, long_span=26, signal_span=9):
    ema_short = ema(closes, short_span)
    ema_long = ema(closes, long_span)
    macd_line = [s - l for s, l in zip(ema_short, ema_long)]
    signal_line = ema(macd_line, signal_span)
    return {"macdLine": macd_line, "signalLine": signal_line}


def calc_rsi(closes: list[float], period: int = 14) -> list[float]:
    if len(closes) < period + 1:
        return [float("nan")] * len(closes)
    result = [float("nan")] * period
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
    result.append(100 - 100 / (1 + rs))
    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = diff if diff > 0 else 0
        loss = -diff if diff < 0 else 0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
        result.append(100 - 100 / (1 + rs))
    return result


def calc_volume_osc(data: list, short: int = 5, long: int = 20) -> list[float]:
    """成交量振荡器: (短期均量 - 长期均量) / 长期均量 * 100"""
    vols = [float(x[6]) for x in data]
    short_ma = sma(vols, short)
    long_ma = sma(vols, long)
    result = []
    for s, l in zip(short_ma, long_ma):
        if math.isnan(s) or math.isnan(l) or l == 0:
            result.append(float("nan"))
        else:
            result.append((s - l) / l * 100)
    return result


def compute_indicators(all_sym: dict):
    for symbol in list(all_sym.keys()):
        for cycle in list(all_sym[symbol].keys()):
            try:
                data = all_sym[symbol][cycle]["data"]
                closes = [float(x[4]) for x in data]
                if len(closes) < 26:
                    continue
                all_sym[symbol][cycle]["bolling"] = calc_bollinger(closes)
                all_sym[symbol][cycle]["macd"] = calc_macd(closes)
                # 盘整放量突破所需指标 (仅 1H 周期且数据充足时计算)
                if cycle == "1H" and len(closes) >= 200:
                    for w in (30, 60, 120, 160, 200):
                        all_sym[symbol][cycle][f"ma{w}"] = sma(closes, w)
                    all_sym[symbol][cycle]["rsi"] = calc_rsi(closes)
                    all_sym[symbol][cycle]["volume_osc"] = calc_volume_osc(data)
            except (KeyError, IndexError, ValueError):
                pass


# ══════════════════════════════════════════════════════════════
#  策略: 多周期趋势共振 (来源: bitget_bot/core/strategy.py)
# ══════════════════════════════════════════════════════════════

def is_15m_trend_up(sym: dict) -> bool:
    b = sym["15m"]["bolling"]
    m = sym["15m"]["macd"]
    return b["mid"][-1] > b["mid"][-2] and m["macdLine"][-1] > 0


def is_1h_trend_up(sym: dict) -> bool:
    b = sym["1H"]["bolling"]
    m = sym["1H"]["macd"]
    if b["mid"][-1] <= b["mid"][-2] * 0.999:
        return False
    return (
        m["macdLine"][-1] >= m["signalLine"][-1]
        and m["macdLine"][-1] >= m["macdLine"][-2]
        and m["signalLine"][-1] >= m["signalLine"][-2]
    )


def is_4h_trend_up(sym: dict) -> bool:
    b = sym["4H"]["bolling"]
    m = sym["4H"]["macd"]
    if b["mid"][-1] <= b["mid"][-2] * 0.999:
        return False
    return m["macdLine"][-1] >= m["macdLine"][-2]


def is_1d_trend_up(sym: dict) -> bool:
    b = sym["1D"]["bolling"]
    close = float(sym["1D"]["data"][-1][4])
    return (
        b["mid"][-1] > b["mid"][-2] > b["mid"][-3] > b["mid"][-4]
        and b["upper"][-1] > b["upper"][-2] > b["upper"][-3] > b["upper"][-4]
        and close > b["mid"][-1]
    )


# ── BTC 大盘方向 ──

def is_btc_trend_up(all_sym: dict) -> bool:
    btc = all_sym.get("BTCUSDT")
    if not btc or "1D" not in btc or not btc["1D"]["data"]:
        return False
    bar = btc["1D"]["data"][-1]
    return float(bar[4]) > float(bar[1]) * 1.02


def is_btc_trend_down(all_sym: dict) -> bool:
    btc = all_sym.get("BTCUSDT")
    if not btc or "1D" not in btc or "1H" not in btc:
        return True
    d = btc["1D"]["data"]
    h = btc["1H"]["data"]
    if len(d) < 7 or len(h) < 25:
        return True
    close = float(d[-1][4])
    return any([
        float(d[-1][1]) > close * 1.02,
        float(h[-25][4]) > close * 1.02,
        float(d[-7][4]) > close * 1.05,
        float(d[-5][4]) > close * 1.05,
        float(d[-3][4]) > close * 1.03,
        float(d[-4][4]) > close * 1.04,
    ])


# ══════════════════════════════════════════════════════════════
#  加分项检测 (来源: bitget_bot/core/scanner.py + live_trading.py)
# ══════════════════════════════════════════════════════════════

def _sum_vol(data, n, j, frm, to):
    s = 0.0
    for i in range(frm, to):
        idx = n + i + j
        if 0 <= idx < len(data):
            s += float(data[idx][6])
    return s


def _is_15m_step_up(sym, j):
    mid = sym["15m"]["bolling"]["mid"]
    for i in range(-2, 0):
        diff_cur = mid[len(mid) + i + j] - mid[len(mid) + i - 1 + j]
        diff_prev = mid[len(mid) + i - 1 + j] - mid[len(mid) + i - 2 + j]
        if diff_cur < diff_prev * 0.9999:
            return False
        if diff_prev * 0.9999 < 0:
            return False
    return True


def _is_15m_anomaly(all_sym, symbol, j):
    sym = all_sym[symbol]
    data = sym["15m"]["data"]
    n = len(data)
    upper = sym["15m"]["bolling"]["upper"][n - 3 + j]
    lower = sym["15m"]["bolling"]["lower"][n - 3 + j]
    if upper > lower * 1.1 and not _is_15m_step_up(sym, j):
        return False
    if "1H" in sym and "bolling" in sym["1H"]:
        n1h = len(sym["1H"]["bolling"]["upper"])
        u1h = sym["1H"]["bolling"]["upper"][n1h - 3 + j]
        l1h = sym["1H"]["bolling"]["lower"][n1h - 3 + j]
        if u1h > l1h * 1.22:
            return False
    vol_sum_9 = _sum_vol(data, n, j, -11, -2)
    vol_sum_19 = vol_sum_9 + _sum_vol(data, n, j, -21, -11)
    bar_vol = float(data[n - 2 + j][6])
    bar_close = float(data[n - 2 + j][4])
    bar_open = float(data[n - 2 + j][1])
    vol_short = bar_vol >= vol_sum_9 and bar_vol >= 100_000
    vol_long = bar_vol >= vol_sum_19 and bar_vol >= 40_000
    price_ok = bar_open * 0.992 < bar_close < bar_open * 1.23
    return (vol_short or vol_long) and price_ok


def _is_1h_anomaly(all_sym, symbol, j):
    data = all_sym[symbol]["1H"]["data"]
    n = len(data)
    bar_vol = float(data[n - 1 + j][6])
    if bar_vol < 400_000:
        return False
    vol_sum = _sum_vol(data, n, j, -6, -1)
    if bar_vol < vol_sum:
        return False
    bar_close = float(data[n - 1 + j][4])
    bar_open = float(data[n - 1 + j][1])
    return bar_open * 1.02 < bar_close < bar_open * 1.5


def _is_4h_anomaly(all_sym, symbol, j):
    data = all_sym[symbol]["4H"]["data"]
    n = len(data)
    bar_vol = float(data[n - 1 + j][6])
    if bar_vol < 800_000:
        return False
    vol_sum = _sum_vol(data, n, j, -5, -1)
    if bar_vol < vol_sum:
        return False
    bar_close = float(data[n - 1 + j][4])
    bar_open = float(data[n - 1 + j][1])
    return bar_open * 1.05 < bar_close < bar_open * 2


def _has_recent_anomaly(all_sym, symbol):
    try:
        for i in range(-7, 0):
            if _is_15m_anomaly(all_sym, symbol, i):
                return True
            if _is_1h_anomaly(all_sym, symbol, i):
                return True
        for i in range(-5, 0):
            if _is_4h_anomaly(all_sym, symbol, i):
                return True
    except (IndexError, KeyError, ValueError):
        pass
    return False


def detect_volume_anomaly(all_sym, symbol) -> str:
    try:
        if _has_recent_anomaly(all_sym, symbol):
            return ""
        if _is_15m_anomaly(all_sym, symbol, 0):
            return "15m"
        if _is_1h_anomaly(all_sym, symbol, 0):
            return "1H"
        if _is_4h_anomaly(all_sym, symbol, 0):
            return "4H"
    except (IndexError, KeyError, ValueError):
        pass
    return ""


# ── 防追高 ──

def _min_price_7d(sym):
    data = sym["1D"]["data"]
    days = min(7, len(data))
    return min(float(data[-i][3]) for i in range(1, days + 1))


def check_anti_chase(sym) -> bool:
    try:
        data = sym["1D"]["data"]
        close = float(data[-1][4])
        b = sym["1D"]["bolling"]
        not_overextended = close < _min_price_7d(sym) * 2.7 and b["upper"][-1] < b["lower"][-1] * 2.7
        not_above_upper = close < b["upper"][-1] * 1.1
        return not_overextended and not_above_upper
    except (IndexError, KeyError, ValueError):
        return False


# ── 龙头币 ──

def find_leading_coins(all_sym) -> set[str]:
    result = set()
    for key in all_sym:
        data = all_sym[key].get("1D", {}).get("data")
        if not data or len(data) < 20:
            continue
        for i in range(-5, -1):
            try:
                idx1 = len(data) - 1 + i
                idx2 = len(data) - 5 + i
                if idx2 < 0:
                    continue
                if float(data[idx1][4]) > float(data[idx2][4]) * 1.2:
                    result.add(key)
                    break
            except (IndexError, ValueError):
                continue
    return result


# ── 仙人指路 ──

def find_fairy_guide(all_sym, candidates) -> set[str]:
    result = set()
    for sym in candidates:
        if sym not in all_sym or "1D" not in all_sym[sym]:
            continue
        data = all_sym[sym]["1D"]["data"]
        if len(data) < 20:
            continue
        for i in range(len(data) - 10, len(data)):
            if i < 10:
                continue
            try:
                vol_sum = sum(float(data[j][6]) for j in range(i - 10, i - 1))
                bar = data[i]
                o, h, c, v = float(bar[1]), float(bar[2]), float(bar[4]), float(bar[6])
                if v > vol_sum and o * 1.2 < h < o * 1.6 and h * 0.92 > c > o:
                    result.add(sym)
                    break
            except (IndexError, ValueError):
                continue
    return result


# ── 小成交量 + 不错涨幅 ──

def is_low_vol_good_move(sym) -> bool:
    """日线最高价 > 开盘价×1.2 且成交额 < 600万"""
    try:
        bar = sym["1D"]["data"][-1]
        high, open_p, quote_vol = float(bar[2]), float(bar[1]), float(bar[6])
        return high > open_p * 1.2 and quote_vol < 6_000_000
    except (IndexError, KeyError, ValueError):
        return False


# ── 盘整放量突破 ──

def detect_consolidation_breakout(sym: dict, cycle: str = "1H") -> bool:
    """
    检测盘整初期放量突破：均线收敛 → 放量 → 突破新高

    条件：
    1. 涨幅适中（1%~6%），收盘站上所有均线，实体 > 上影线
    2. 成交量放量（volume_osc > 40%，或 > 15% 且均线多头排列）
    3. 收盘创近 120 根 K 线新高，且高于前 3 根最高价
    4. 均线收敛（最大最小均线差 < 2.8%）
    5. RSI 在 58~80
    6. 回溯 10 根 K 线，每根均线宽度 < 3.5% 且 RSI 在 38~82
    """
    try:
        c = sym.get(cycle)
        if not c:
            return False

        data = c.get("data", [])
        rsi = c.get("rsi", [])
        vol_osc = c.get("volume_osc", [])

        if len(data) < 200 or len(rsi) < 12 or len(vol_osc) < 2:
            return False

        ma_keys = ["ma30", "ma60", "ma120", "ma160", "ma200"]
        for k in ma_keys:
            if k not in c or len(c[k]) < 12:
                return False

        close = float(data[-1][4])
        open_ = float(data[-1][1])
        high = float(data[-1][2])

        # 条件 1：涨幅 1%~6%，站上所有均线，实体 > 上影线
        zf = (close - open_) / open_ if open_ > 0 else 0
        ma_values = [c[k][-1] for k in ma_keys]
        valid_ma = [v for v in ma_values if v is not None and v == v]
        if len(valid_ma) < 5:
            return False
        ma_max = max(valid_ma)
        ma_min = min(valid_ma)
        if not (0.01 < zf < 0.06):
            return False
        if close <= ma_max:
            return False
        if (close - open_) <= (high - close):
            return False

        # 条件 2：放量
        cur_vol_osc = vol_osc[-1]
        if math.isnan(cur_vol_osc):
            return False
        ma30_val, ma60_val = c["ma30"][-1], c["ma60"][-1]
        ma120_val, ma200_val = c["ma120"][-1], c["ma200"][-1]
        bullish_aligned = (
            ma30_val and ma60_val and ma120_val and ma200_val
            and ma30_val > ma60_val > ma120_val > ma200_val
        )
        if not (cur_vol_osc > 40 or (cur_vol_osc > 15 and bullish_aligned)):
            return False

        # 条件 3：创 120 根 K 线新高
        recent_highs = [float(data[i][2]) for i in range(-4, -1)]
        closes_120 = [float(data[i][4]) for i in range(-121, -1)]
        if not closes_120:
            return False
        if close <= max(closes_120):
            return False
        if not all(close > h for h in recent_highs):
            return False

        # 条件 4：均线收敛 < 2.8%
        if ma_min <= 0:
            return False
        if (ma_max - ma_min) / ma_min >= 0.028:
            return False

        # 条件 5：RSI 58~80
        cur_rsi = rsi[-1]
        if math.isnan(cur_rsi) or not (58 < cur_rsi < 80):
            return False

        # 条件 6：回溯 10 根 K 线，均线宽度 < 3.5% 且 RSI 38~82
        for i in range(2, 11):
            i_ma_values = [c[k][-i] for k in ma_keys]
            i_valid = [v for v in i_ma_values if v is not None and v == v]
            if len(i_valid) < 5:
                return False
            i_max, i_min = max(i_valid), min(i_valid)
            if i_min <= 0 or (i_max - i_min) / i_max >= 0.035:
                return False
            if len(rsi) >= i and (math.isnan(rsi[-i]) or not (38 < rsi[-i] < 82)):
                return False

        return True

    except (KeyError, IndexError, ValueError, TypeError):
        return False


# ── 波动充足 ──

def is_not_rubbish(sym) -> bool:
    try:
        data = sym["1D"]["data"]
        for i in range(-3, 0):
            if float(data[i][2]) > float(data[i][3]) * 1.1:
                return True
        return False
    except (IndexError, KeyError, ValueError):
        return False


# ══════════════════════════════════════════════════════════════
#  数据校验
# ══════════════════════════════════════════════════════════════

def is_valid(sym: dict) -> bool:
    for tf in ("4H", "1H", "15m"):
        if tf not in sym or not sym[tf].get("data") or len(sym[tf]["data"]) < 26:
            return False
    if "1D" not in sym or not sym["1D"].get("data") or len(sym["1D"]["data"]) < 20:
        return False
    return True


def has_indicators(sym: dict) -> bool:
    for tf in ("1D", "4H", "1H", "15m"):
        if tf not in sym or "bolling" not in sym[tf] or "macd" not in sym[tf]:
            return False
    return True


# ══════════════════════════════════════════════════════════════
#  主扫描流程
# ══════════════════════════════════════════════════════════════

async def main():
    scan_start = time.time()
    bj_tz = timezone(timedelta(hours=8))
    scan_time = datetime.now(bj_tz).strftime("%Y-%m-%d %H:%M:%S")
    log.info("========== SCAN START: %s ==========", scan_time)

    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    async with aiohttp.ClientSession(headers=headers) as session:
        # 1. 获取所有交易对
        log.info("获取 Bitget USDT 永续合约交易对...")
        symbols = await fetch_all_symbols(session)
        if not symbols:
            log.error("无法获取交易对，退出")
            sys.exit(1)

        # 2. 高并发获取 K 线数据
        all_sym = await fetch_all_data(session, symbols, max_concurrent=10)

        # 3. 获取资金费率
        log.info("获取资金费率...")
        fund_rates = await fetch_fund_rates(session, symbols)

    # 4. 计算技术指标
    log.info("计算技术指标...")
    compute_indicators(all_sym)

    # 5. BTC 大盘方向
    btc_up = is_btc_trend_up(all_sym)
    btc_down = is_btc_trend_down(all_sym)
    btc_direction = "up" if btc_up else ("down" if btc_down else "neutral")
    log.info("BTC 方向: %s", btc_direction)

    # 6. 全市场扫描，独立检测每个标签条件
    log.info("全市场扫描，检测各标签条件...")
    result_tokens = []
    valid_count = 0

    # 预计算龙头币（需要全市场数据）
    leading = find_leading_coins(all_sym)

    for key in all_sym:
        if key == "BTCUSDT":
            continue
        sym = all_sym[key]
        if not is_valid(sym) or not has_indicators(sym):
            continue
        valid_count += 1

        tags = []

        # 多周期趋势共振
        try:
            trend_all_up = (
                is_15m_trend_up(sym)
                and is_1h_trend_up(sym)
                and is_4h_trend_up(sym)
                and is_1d_trend_up(sym)
            )
            if trend_all_up:
                tags.append("趋势共振")
        except (IndexError, KeyError, ValueError):
            pass

        # 成交量异动
        anomaly_tf = detect_volume_anomaly(all_sym, key)
        if anomaly_tf:
            tags.append(f"成交量异动({anomaly_tf})")

        # BTC 大盘方向
        if btc_up:
            tags.append("BTC看多")

        # 防追高
        if check_anti_chase(sym):
            tags.append("未追高")

        # 资金费率
        fr = fund_rates.get(key, 0)
        if fr < -0.0001:
            tags.append(f"负费率({fr * 100:.4f}%)")

        # 波动充足
        if is_not_rubbish(sym):
            tags.append("波动充足")

        # 龙头币
        if key in leading:
            tags.append("龙头币")

        # 小成交量+不错涨幅
        if is_low_vol_good_move(sym):
            tags.append("小量大涨")

        # 盘整放量突破
        if detect_consolidation_breakout(sym, "1H"):
            tags.append("盘整突破")

        # 跳过没有任何标签的币
        if not tags:
            continue

        last_bar = sym["1D"]["data"][-1]
        close = float(last_bar[4])
        high = float(last_bar[2])
        low = float(last_bar[3])
        open_p = float(last_bar[1])
        change_pct = ((close - open_p) / open_p * 100) if open_p else 0

        result_tokens.append({
            "symbol": key,
            "price": close,
            "high_24h": high,
            "low_24h": low,
            "change_pct": round(change_pct, 2),
            "fund_rate": round(fr, 6),
            "tags": tags,
        })

    # 仙人指路（需要候选列表）
    fairy = find_fairy_guide(all_sym, [t["symbol"] for t in result_tokens])
    for t in result_tokens:
        if t["symbol"] in fairy:
            t["tags"].append("仙人指路")

    # 按标签数量排序
    result_tokens.sort(key=lambda x: len(x["tags"]), reverse=True)

    # 默认组合筛选数量
    DEFAULT_TAGS = {"趋势共振", "波动充足", "未追高"}
    default_count = sum(
        1 for t in result_tokens
        if DEFAULT_TAGS.issubset({tag.split("(")[0] for tag in t["tags"]})
    )
    elapsed = round(time.time() - scan_start, 1)
    log.info("完成: %d个交易对, %d个可分析, %d个有标签, 默认组合%d个, 耗时%ss",
             len(symbols), valid_count, len(result_tokens), default_count, elapsed)

    # 9. 写入结果
    result = {
        "scanTime": scan_time,
        "totalSymbols": len(symbols),
        "validSymbols": valid_count,
        "filteredCount": default_count,
        "totalTagged": len(result_tokens),
        "btcDirection": btc_direction,
        "elapsed": elapsed,
        "tokens": result_tokens,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    bj_now = datetime.now(bj_tz)
    scan_id = bj_now.strftime("%Y-%m-%dT%H-%M-%S")
    scan_file = DATA_DIR / f"{scan_id}.json"
    scan_file.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    log.info("写入 %s", scan_file)

    # 清理 7 天前的数据
    cutoff = time.time() - 7 * 86400
    cleaned = 0
    for f in DATA_DIR.glob("*.json"):
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})\.json", f.name)
        if not m:
            continue
        y, mo, d, h, mi, s = m.groups()
        file_ts = datetime(int(y), int(mo), int(d), int(h), int(mi), int(s), tzinfo=timezone.utc).timestamp()
        if file_ts < cutoff:
            f.unlink()
            cleaned += 1
    if cleaned:
        log.info("清理 %d 个旧数据文件", cleaned)


if __name__ == "__main__":
    asyncio.run(main())
