"""
╔══════════════════════════════════════════════════════════════╗
║           XAUUSD GOLD SWING BOT v1.0                        ║
║    Professional SMC Swing Strategy for Gold                  ║
║    4H Structure · 1H Entry · Daily Trend                     ║
║    MetaAPI Cloud SDK · Railway Deploy                        ║
╚══════════════════════════════════════════════════════════════╝

Strategy (based on professional swing trading research):
- Daily EMA 50/200 Golden Cross for macro trend
- 4H BOS/CHoCH for market structure + directional bias
- 4H Premium/Discount zone filter (only buy in discount)
- 1H Order Block + FVG for entry zones
- 1H Liquidity Sweep for entry trigger
- RSI 14 divergence confirmation
- ADX > 20 trend strength filter
- BB squeeze detection for breakout entries
- ATR-based dynamic SL (ATR×3.0 on 1H)
- RR 1:3.0 | Partial 50% at 1.5R | Trail runner
- Max 2 trades at a time | 3-8 per week target
- 60-second cycle for swing pace
"""

import os
import sys
import asyncio
import logging
import sqlite3
import time
import signal
from datetime import datetime, timedelta, timezone
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from collections import deque

try:
    from metaapi_cloud_sdk import MetaApi
except ImportError:
    print("pip install metaapi-cloud-sdk")
    sys.exit(1)

try:
    import aiohttp
except ImportError:
    print("pip install aiohttp")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

@dataclass
class SwingConfig:
    # ─── MetaAPI ─────────────────────────────────────────────────
    META_API_TOKEN: str = os.getenv("METAAPI_TOKEN", "")
    ACCOUNT_ID: str = os.getenv("ACCOUNT_ID", "")

    # ─── Telegram ────────────────────────────────────────────────
    TELEGRAM_TOKEN: str = os.getenv("TG_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TG_CHAT", "")
    TELEGRAM_RATE_LIMIT: float = 1.5

    # ─── Symbol ──────────────────────────────────────────────────
    SYMBOL: str = "XAUUSD"
    POINT: float = 0.01

    # ─── Timeframes ──────────────────────────────────────────────
    TF_TREND: str = "1d"       # Daily for macro trend (EMA 50/200)
    TF_STRUCTURE: str = "4h"   # 4H for market structure (BOS/CHoCH)
    TF_ENTRY: str = "1h"       # 1H for entry zones (OB/FVG)
    CANDLE_LOOKBACK_DAILY: int = 60
    CANDLE_LOOKBACK_4H: int = 100
    CANDLE_LOOKBACK_1H: int = 100

    # ─── Sessions (UTC) ─────────────────────────────────────────
    LONDON_START: int = 7
    LONDON_END: int = 12
    NY_START: int = 12
    NY_END: int = 17
    OVERLAP_START: int = 12
    OVERLAP_END: int = 15

    # ─── Risk Management ─────────────────────────────────────────
    RISK_PERCENT: float = 1.0        # 1% per swing trade
    MAX_DAILY_LOSS_PERCENT: float = 3.0
    MAX_TOTAL_DRAWDOWN_PERCENT: float = 10.0
    MAX_CONCURRENT_TRADES: int = 2   # max 2 swings at a time
    MAX_DAILY_TRADES: int = 3        # max 3 per day (target 3-8/week)
    MAX_CONSECUTIVE_LOSSES: int = 3

    # ─── Spread Filter ───────────────────────────────────────────
    MAX_SPREAD_POINTS: float = 4.0   # wider tolerance for swings
    SPREAD_CHECK_ENABLED: bool = True

    # ─── SL/TP (ATR-based on 1H) ────────────────────────────────
    ATR_PERIOD: int = 14
    ATR_SL_MULTIPLIER: float = 3.0   # wider SL for swings
    MIN_SL_POINTS: float = 5.0       # minimum $5 SL
    MAX_SL_POINTS: float = 30.0      # max $30 SL
    RR_RATIO: float = 3.0            # 1:3 RR for swings

    # ─── Partial Close ───────────────────────────────────────────
    PARTIAL_PERCENT: float = 0.50    # close 50% at TP1
    TP1_RR_RATIO: float = 1.5       # TP1 at 1.5R
    MOVE_SL_TO_BE: bool = True      # SL to breakeven after TP1

    # ─── Trailing Stop (runner) ──────────────────────────────────
    USE_TRAILING_STOP: bool = True
    TRAIL_ACTIVATION_RR: float = 2.0  # start trailing at 2.0R
    TRAIL_ATR_MULT: float = 1.5       # trail distance = ATR × 1.5
    TRAIL_STEP: float = 1.0           # min $1 improvement per trail

    # ─── SMC Parameters ─────────────────────────────────────────
    SWING_LOOKBACK: int = 5          # wider swings for 4H
    OB_MAX_AGE_CANDLES: int = 30     # OBs valid for 30 candles
    FVG_MIN_SIZE_ATR: float = 0.3    # FVG min size
    BOS_MIN_BREAK_ATR: float = 0.5   # BOS must break by 0.5 ATR

    # ─── Indicators ──────────────────────────────────────────────
    EMA_FAST: int = 50               # Golden Cross EMA
    EMA_SLOW: int = 200              # Golden Cross EMA
    RSI_PERIOD: int = 14
    RSI_OVERSOLD: float = 30.0
    RSI_OVERBOUGHT: float = 70.0
    ADX_THRESHOLD: float = 20.0      # only trade trending markets
    BB_PERIOD: int = 20
    BB_STD: float = 2.0
    BB_SQUEEZE_ATR_RATIO: float = 0.5  # BB width < ATR × 0.5 = squeeze

    # ─── Mean Reversion ──────────────────────────────────────────
    USE_MEAN_REVERSION: bool = True
    MR_CONFLUENCE_SCORE: int = 2

    # ─── Confluence ──────────────────────────────────────────────
    MIN_CONFLUENCE: int = 4          # higher for swing quality

    # ─── Timing ──────────────────────────────────────────────────
    MAIN_LOOP_SECONDS: int = 60      # 60-second cycle for swings
    HEARTBEAT_INTERVAL: int = 1800   # every 30 min
    WATCHDOG_TIMEOUT: int = 900      # 15 min
    TRADE_COOLDOWN_SECONDS: int = 3600   # 1 hour between trades
    LOSS_COOLDOWN_SECONDS: int = 7200    # 2 hours after loss

    # ─── Database ────────────────────────────────────────────────
    DB_PATH: str = "gold_swing.db"


# ═══════════════════════════════════════════════════════════════════
#  ENUMS & DATA
# ═══════════════════════════════════════════════════════════════════

class Direction(Enum):
    LONG = "buy"
    SHORT = "sell"

class TradePhase(Enum):
    OPEN = "open"
    TP1_HIT = "tp1_hit"
    TRAILING = "trailing"
    CLOSED = "closed"

class MarketRegime(Enum):
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    RANGING = "ranging"
    SQUEEZE = "squeeze"

class Session(Enum):
    LONDON = "london"
    NY_OVERLAP = "overlap"
    NEW_YORK = "new_york"
    OFF = "off"

@dataclass
class SwingPoint:
    price: float
    bar_index: int
    direction: str  # "high" or "low"

@dataclass
class OrderBlock:
    direction: Direction
    high: float
    low: float
    bar_index: int
    mitigated: bool = False

@dataclass
class FVG:
    direction: Direction
    high: float
    low: float
    bar_index: int
    filled: bool = False

@dataclass
class SwingTrade:
    id: str = ""
    direction: Direction = Direction.LONG
    entry: float = 0.0
    sl: float = 0.0
    tp1: float = 0.0
    tp: float = 0.0
    lots: float = 0.0
    phase: TradePhase = TradePhase.OPEN
    open_time: float = 0.0
    pnl: float = 0.0
    reason: str = ""

@dataclass
class BotState:
    def __init__(self, cfg: SwingConfig):
        self.cfg = cfg
        self.running = True
        self.balance = 0.0
        self.equity = 0.0
        self.start_balance = 0.0
        self.trade_date = ""
        self.daily_pnl = 0.0
        self.daily_trades = 0
        self.daily_wins = 0
        self.daily_losses = 0
        self.consecutive_losses = 0
        self.last_trade_time = 0.0
        self.last_loss_time = 0.0
        self.heartbeat = 0.0

        # Candles
        self.candles_daily: List[dict] = []
        self.candles_4h: List[dict] = []
        self.candles_1h: List[dict] = []

        # Market state
        self.macro_trend: Optional[Direction] = None   # Daily EMA 50/200
        self.structure_bias: Optional[Direction] = None  # 4H BOS/CHoCH
        self.regime: MarketRegime = MarketRegime.RANGING
        self.session: Session = Session.OFF
        self.premium_discount: str = "neutral"  # "premium", "discount", "neutral"

        # Indicators
        self.ema50: float = 0.0
        self.ema200: float = 0.0
        self.rsi: float = 50.0
        self.adx: float = 0.0
        self.bb_upper: float = 0.0
        self.bb_lower: float = 0.0
        self.bb_squeeze: bool = False
        self.atr_1h: float = 5.0

        # Active trades
        self.active_trades: Dict[str, SwingTrade] = {}

        # Telegram
        self.last_tg_time: float = 0.0
        self.last_heartbeat_time: float = 0.0


# ═══════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════

def setup_logging():
    logger = logging.getLogger("swing")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s", "%H:%M:%S")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    fh = logging.FileHandler("gold_swing.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

log = setup_logging()


# ═══════════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════════

class Database:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS swing_trades (
                id TEXT PRIMARY KEY,
                direction TEXT, entry REAL, sl REAL, tp REAL,
                lots REAL, phase TEXT, open_time TEXT,
                close_time TEXT, pnl REAL DEFAULT 0.0, reason TEXT
            );
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY, trades INTEGER,
                wins INTEGER, losses INTEGER, pnl REAL
            );
        """)
        self.conn.commit()

    def save_trade(self, t: SwingTrade):
        self.conn.execute("""
            INSERT OR REPLACE INTO swing_trades
            (id, direction, entry, sl, tp, lots, phase, open_time, pnl, reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (t.id, t.direction.value, t.entry, t.sl, t.tp, t.lots,
              t.phase.value, datetime.fromtimestamp(t.open_time, tz=timezone.utc).isoformat(),
              t.pnl, t.reason))
        self.conn.commit()

    def save_daily(self, date, trades, wins, losses, pnl):
        self.conn.execute("""
            INSERT OR REPLACE INTO daily_stats (date, trades, wins, losses, pnl)
            VALUES (?, ?, ?, ?, ?)
        """, (date, trades, wins, losses, pnl))
        self.conn.commit()

    def close(self):
        self.conn.close()


# ═══════════════════════════════════════════════════════════════════
#  TELEGRAM
# ═══════════════════════════════════════════════════════════════════

class Telegram:
    def __init__(self, state: BotState):
        self.state = state
        self.cfg = state.cfg

    async def send(self, text: str, silent: bool = False):
        if not self.cfg.TELEGRAM_TOKEN or not self.cfg.TELEGRAM_CHAT_ID:
            return
        now = time.time()
        if now - self.state.last_tg_time < self.cfg.TELEGRAM_RATE_LIMIT:
            await asyncio.sleep(self.cfg.TELEGRAM_RATE_LIMIT)
        try:
            url = f"https://api.telegram.org/bot{self.cfg.TELEGRAM_TOKEN}/sendMessage"
            payload = {
                "chat_id": self.cfg.TELEGRAM_CHAT_ID,
                "text": text, "parse_mode": "HTML",
                "disable_notification": silent
            }
            async with aiohttp.ClientSession() as session:
                await session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10))
            self.state.last_tg_time = time.time()
        except Exception as e:
            log.warning(f"TG error: {e}")

    async def swing_opened(self, t: SwingTrade, confluence: int, session: str):
        emoji = "🟢" if t.direction == Direction.LONG else "🔴"
        await self.send(
            f"{emoji} <b>SWING {t.direction.value.upper()}</b>\n"
            f"Entry: ${t.entry:.2f} | SL: ${t.sl:.2f}\n"
            f"TP1: ${t.tp1:.2f} | TP: ${t.tp:.2f}\n"
            f"Lots: {t.lots} | Score: {confluence}\n"
            f"Session: {session} | {t.reason}\n"
            f"Trend: {self.state.macro_trend.value if self.state.macro_trend else 'N/A'} | "
            f"Structure: {self.state.structure_bias.value if self.state.structure_bias else 'N/A'}"
        )

    async def swing_partial(self, t: SwingTrade):
        await self.send(
            f"✂️ <b>50% CLOSED</b> @ ${t.tp1:.2f} → SL to BE\n"
            f"🏃 50% runner active — trailing at {self.cfg.TRAIL_ACTIVATION_RR}R",
            silent=True
        )

    async def swing_trailing(self, t: SwingTrade, new_sl: float, profit_r: float):
        if profit_r >= 2.5:
            await self.send(
                f"🎯 <b>RUNNER TRAILING</b>\n"
                f"SL → ${new_sl:.2f} | Locked: {profit_r:.1f}R",
                silent=True
            )

    async def swing_closed(self, t: SwingTrade):
        e = "✅" if t.pnl > 0 else "❌"
        await self.send(
            f"{e} <b>SWING CLOSED</b> ${t.pnl:+.2f}\n"
            f"Daily: ${self.state.daily_pnl:+.2f} | "
            f"W/L: {self.state.daily_wins}/{self.state.daily_losses}"
        )

    async def heartbeat(self, price, spread, open_count):
        now = time.time()
        if now - self.state.last_heartbeat_time < self.cfg.HEARTBEAT_INTERVAL:
            return
        self.state.last_heartbeat_time = now
        trend = self.state.macro_trend.value if self.state.macro_trend else "N/A"
        struct = self.state.structure_bias.value if self.state.structure_bias else "N/A"
        await self.send(
            f"💓 <b>SWING BOT</b>\n"
            f"${price:.2f} | Spread: ${spread:.2f}\n"
            f"Trend: {trend} | Structure: {struct}\n"
            f"Regime: {self.state.regime.value}\n"
            f"Zone: {self.state.premium_discount}\n"
            f"RSI: {self.state.rsi:.0f} | ADX: {self.state.adx:.0f}\n"
            f"Open: {open_count} | Daily: {self.state.daily_trades}\n"
            f"PnL: ${self.state.daily_pnl:+.2f}",
            silent=True
        )

    async def daily_report(self):
        wr = self.state.daily_wins / max(self.state.daily_trades, 1) * 100
        await self.send(
            f"📊 <b>SWING DAILY REPORT</b>\n"
            f"Trades: {self.state.daily_trades}\n"
            f"W/L: {self.state.daily_wins}/{self.state.daily_losses}\n"
            f"PnL: ${self.state.daily_pnl:+.2f}\n"
            f"WR: {wr:.0f}% | Balance: ${self.state.balance:.2f}"
        )


# ═══════════════════════════════════════════════════════════════════
#  SESSION MANAGER
# ═══════════════════════════════════════════════════════════════════

class SessionMgr:
    def __init__(self, cfg: SwingConfig):
        self.cfg = cfg

    def get(self, hour: int) -> Session:
        if self.cfg.OVERLAP_START <= hour < self.cfg.OVERLAP_END:
            return Session.NY_OVERLAP
        if self.cfg.LONDON_START <= hour < self.cfg.LONDON_END:
            return Session.LONDON
        if self.cfg.NY_START <= hour < self.cfg.NY_END:
            return Session.NEW_YORK
        return Session.OFF

    def is_tradeable(self, hour: int) -> bool:
        return self.get(hour) in (Session.LONDON, Session.NY_OVERLAP, Session.NEW_YORK)


# ═══════════════════════════════════════════════════════════════════
#  SWING ANALYZER — Multi-Timeframe SMC
# ═══════════════════════════════════════════════════════════════════

class SwingAnalyzer:
    def __init__(self, cfg: SwingConfig):
        self.cfg = cfg

    # ─── ATR ──────────────────────────────────────────────────────
    def atr(self, candles: List[dict], period: int = 14) -> float:
        if len(candles) < period + 1:
            return 5.0
        trs = []
        for i in range(1, len(candles)):
            h, l = candles[i].get("high", 0), candles[i].get("low", 0)
            pc = candles[i-1].get("close", 0)
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return sum(trs[-period:]) / period if len(trs) >= period else sum(trs) / max(len(trs), 1)

    # ─── EMA ──────────────────────────────────────────────────────
    def ema(self, candles: List[dict], period: int) -> float:
        if len(candles) < period:
            return 0.0
        closes = [c.get("close", 0) for c in candles]
        mult = 2 / (period + 1)
        val = sum(closes[:period]) / period
        for c in closes[period:]:
            val = (c - val) * mult + val
        return val

    # ─── RSI ──────────────────────────────────────────────────────
    def rsi(self, candles: List[dict], period: int = 14) -> float:
        if len(candles) < period + 2:
            return 50.0
        closes = [c.get("close", 0) for c in candles[-(period + 1):]]
        gains, losses = [], []
        for i in range(1, len(closes)):
            d = closes[i] - closes[i-1]
            gains.append(max(d, 0))
            losses.append(max(-d, 0))
        ag = sum(gains) / period
        al = sum(losses) / period
        if al == 0:
            return 100.0
        return 100 - (100 / (1 + ag / al))

    # ─── ADX ──────────────────────────────────────────────────────
    def adx(self, candles: List[dict], period: int = 14) -> float:
        if len(candles) < period * 2:
            return 0.0
        plus_dm, minus_dm, tr_list = [], [], []
        for i in range(1, len(candles)):
            h = candles[i].get("high", 0)
            l = candles[i].get("low", 0)
            c = candles[i-1].get("close", 0)
            ph = candles[i-1].get("high", 0)
            pl = candles[i-1].get("low", 0)
            tr = max(h - l, abs(h - c), abs(l - c))
            pd_val = max(h - ph, 0) if (h - ph) > (pl - l) else 0
            md_val = max(pl - l, 0) if (pl - l) > (h - ph) else 0
            tr_list.append(tr)
            plus_dm.append(pd_val)
            minus_dm.append(md_val)
        if len(tr_list) < period:
            return 0.0
        atr_s = sum(tr_list[:period])
        plus_s = sum(plus_dm[:period])
        minus_s = sum(minus_dm[:period])
        dx_list = []
        for i in range(period, len(tr_list)):
            atr_s = atr_s - (atr_s / period) + tr_list[i]
            plus_s = plus_s - (plus_s / period) + plus_dm[i]
            minus_s = minus_s - (minus_s / period) + minus_dm[i]
            if atr_s > 0:
                pdi = (plus_s / atr_s) * 100
                mdi = (minus_s / atr_s) * 100
            else:
                pdi = mdi = 0
            di_sum = pdi + mdi
            dx = abs(pdi - mdi) / di_sum * 100 if di_sum > 0 else 0
            dx_list.append(dx)
        if len(dx_list) < period:
            return 0.0
        return sum(dx_list[-period:]) / period

    # ─── Bollinger Bands ──────────────────────────────────────────
    def bollinger(self, candles: List[dict], period: int = 20, std: float = 2.0) -> Tuple[float, float, float]:
        if len(candles) < period:
            return 0, 0, 0
        closes = [c.get("close", 0) for c in candles[-period:]]
        mid = sum(closes) / period
        variance = sum((c - mid) ** 2 for c in closes) / period
        sd = variance ** 0.5
        return mid, mid + std * sd, mid - std * sd

    # ─── Swing Points ────────────────────────────────────────────
    def swings(self, candles: List[dict], lookback: int = 5) -> Tuple[List[SwingPoint], List[SwingPoint]]:
        highs, lows = [], []
        for i in range(lookback, len(candles) - lookback):
            h = candles[i].get("high", 0)
            l = candles[i].get("low", 0)
            is_high = all(h >= candles[i+j].get("high", 0) and h >= candles[i-j].get("high", 0)
                          for j in range(1, lookback + 1))
            is_low = all(l <= candles[i+j].get("low", 99999) and l <= candles[i-j].get("low", 99999)
                         for j in range(1, lookback + 1))
            if is_high:
                highs.append(SwingPoint(price=h, bar_index=i, direction="high"))
            if is_low:
                lows.append(SwingPoint(price=l, bar_index=i, direction="low"))
        return highs, lows

    # ─── BOS / CHoCH Detection (4H) ──────────────────────────────
    def detect_structure(self, candles: List[dict], atr: float) -> Optional[Direction]:
        """Detect Break of Structure or Change of Character on 4H.
        BOS in uptrend = price breaks above swing high → bullish continuation
        CHoCH = price breaks below swing low in uptrend → bearish reversal
        """
        if len(candles) < 30:
            return None

        highs, lows = self.swings(candles, 5)
        if len(highs) < 2 or len(lows) < 2:
            return None

        price = candles[-1].get("close", 0)
        min_break = atr * self.cfg.BOS_MIN_BREAK_ATR

        # Check last 3 swing highs/lows
        recent_highs = highs[-3:]
        recent_lows = lows[-3:]

        # Higher highs + higher lows = bullish structure
        hh = len(recent_highs) >= 2 and recent_highs[-1].price > recent_highs[-2].price
        hl = len(recent_lows) >= 2 and recent_lows[-1].price > recent_lows[-2].price

        # Lower highs + lower lows = bearish structure
        lh = len(recent_highs) >= 2 and recent_highs[-1].price < recent_highs[-2].price
        ll = len(recent_lows) >= 2 and recent_lows[-1].price < recent_lows[-2].price

        # BOS: price breaks above last swing high (bullish) or below last swing low (bearish)
        last_sh = recent_highs[-1].price
        last_sl = recent_lows[-1].price

        if price > last_sh + min_break and (hh or hl):
            return Direction.LONG   # Bullish BOS
        if price < last_sl - min_break and (lh or ll):
            return Direction.SHORT  # Bearish BOS

        # CHoCH: break of structure against the trend
        if hh and hl and price < last_sl - min_break:
            return Direction.SHORT  # CHoCH from bullish to bearish
        if lh and ll and price > last_sh + min_break:
            return Direction.LONG   # CHoCH from bearish to bullish

        return None

    # ─── Premium / Discount Zone (4H) ────────────────────────────
    def premium_discount(self, candles: List[dict]) -> str:
        """Determine if price is in premium (above 50%) or discount (below 50%)
        of the current 4H range. Buy in discount, sell in premium."""
        highs, lows = self.swings(candles, 5)
        if not highs or not lows:
            return "neutral"

        range_high = max(h.price for h in highs[-5:])
        range_low = min(l.price for l in lows[-5:])
        if range_high <= range_low:
            return "neutral"

        price = candles[-1].get("close", 0)
        equilibrium = (range_high + range_low) / 2

        if price < equilibrium:
            return "discount"   # good for buys
        elif price > equilibrium:
            return "premium"    # good for sells
        return "neutral"

    # ─── Order Blocks (1H) ────────────────────────────────────────
    def order_blocks(self, candles: List[dict]) -> List[OrderBlock]:
        obs = []
        for i in range(2, len(candles)):
            prev = candles[i-1]
            curr = candles[i]
            pb = abs(prev.get("close", 0) - prev.get("open", 0))
            cb = abs(curr.get("close", 0) - curr.get("open", 0))

            # Bullish OB: bearish candle followed by strong bullish
            if (curr["close"] > curr["open"] and prev["close"] < prev["open"]
                    and cb > pb * self.cfg.FVG_MIN_SIZE_ATR * 2
                    and curr["close"] > prev["high"]):
                obs.append(OrderBlock(
                    direction=Direction.LONG,
                    high=prev["high"], low=prev["low"],
                    bar_index=i, mitigated=False
                ))
            # Bearish OB
            elif (curr["close"] < curr["open"] and prev["close"] > prev["open"]
                    and cb > pb * self.cfg.FVG_MIN_SIZE_ATR * 2
                    and curr["close"] < prev["low"]):
                obs.append(OrderBlock(
                    direction=Direction.SHORT,
                    high=prev["high"], low=prev["low"],
                    bar_index=i, mitigated=False
                ))
        return obs[-self.cfg.OB_MAX_AGE_CANDLES:]

    # ─── FVGs (1H) ───────────────────────────────────────────────
    def fvgs(self, candles: List[dict], atr: float) -> List[FVG]:
        fvg_list = []
        min_gap = atr * self.cfg.FVG_MIN_SIZE_ATR
        for i in range(2, len(candles)):
            c1_high = candles[i-2].get("high", 0)
            c3_low = candles[i].get("low", 0)
            c1_low = candles[i-2].get("low", 0)
            c3_high = candles[i].get("high", 0)

            # Bullish FVG
            if c3_low > c1_high and (c3_low - c1_high) >= min_gap:
                fvg_list.append(FVG(direction=Direction.LONG,
                                    high=c3_low, low=c1_high, bar_index=i))
            # Bearish FVG
            elif c1_low > c3_high and (c1_low - c3_high) >= min_gap:
                fvg_list.append(FVG(direction=Direction.SHORT,
                                    high=c1_low, low=c3_high, bar_index=i))
        return fvg_list[-20:]

    # ─── Liquidity Sweep (1H) ────────────────────────────────────
    def liquidity_sweep(self, candles: List[dict],
                         highs: List[SwingPoint], lows: List[SwingPoint],
                         atr: float) -> Optional[Direction]:
        if len(candles) < 3:
            return None
        last = candles[-1]
        ch, cl, cc = last.get("high", 0), last.get("low", 0), last.get("close", 0)
        co = last.get("open", cc)

        # Sweep below swing low → buy
        for sl in lows[-5:]:
            if cl < sl.price and cc > sl.price:
                wick = min(cc, co) - cl
                if wick > atr * 0.3:
                    return Direction.LONG

        # Sweep above swing high → sell
        for sh in highs[-5:]:
            if ch > sh.price and cc < sh.price:
                wick = ch - max(cc, co)
                if wick > atr * 0.3:
                    return Direction.SHORT

        return None

    # ─── RSI Divergence ───────────────────────────────────────────
    def rsi_divergence(self, candles: List[dict], rsi_val: float) -> Optional[Direction]:
        if len(candles) < 10 or rsi_val == 0:
            return None
        price_now = candles[-1].get("close", 0)
        price_prev = candles[-8].get("close", 0)

        # Bullish divergence: price lower low, RSI higher low
        if rsi_val < 35 and price_now > price_prev:
            return Direction.LONG
        # Bearish divergence: price higher high, RSI lower high
        if rsi_val > 65 and price_now < price_prev:
            return Direction.SHORT
        return None

    # ─── Momentum Candle ──────────────────────────────────────────
    def is_momentum_candle(self, candle: dict, atr: float) -> Optional[Direction]:
        body = abs(candle.get("close", 0) - candle.get("open", 0))
        total = candle.get("high", 0) - candle.get("low", 0)
        if total <= 0 or body / total < 0.6:
            return None
        if body >= atr * 0.8:
            if candle["close"] > candle["open"]:
                return Direction.LONG
            return Direction.SHORT
        return None

    # ─── BB Squeeze Detection ─────────────────────────────────────
    def detect_squeeze(self, candles: List[dict], atr: float) -> bool:
        """BB squeeze = BB width < ATR * threshold → breakout incoming."""
        _, upper, lower = self.bollinger(candles, self.cfg.BB_PERIOD, self.cfg.BB_STD)
        if upper <= 0 or lower <= 0:
            return False
        bb_width = upper - lower
        return bb_width < atr * self.cfg.BB_SQUEEZE_ATR_RATIO

    # ─── Mean Reversion (BB extremes + RSI) ───────────────────────
    def mean_reversion(self, candles: List[dict], price: float,
                        rsi_val: float) -> Optional[Tuple[Direction, int, str]]:
        _, upper, lower = self.bollinger(candles, self.cfg.BB_PERIOD, self.cfg.BB_STD)
        if upper <= 0 or lower <= 0:
            return None
        bb_range = upper - lower
        if bb_range <= 0:
            return None
        pct_b = (price - lower) / bb_range

        if pct_b <= 0.05 and rsi_val <= self.cfg.RSI_OVERSOLD:
            return Direction.LONG, 2, "MR:bb_low+rsi_os"
        elif pct_b >= 0.95 and rsi_val >= self.cfg.RSI_OVERBOUGHT:
            return Direction.SHORT, 2, "MR:bb_high+rsi_ob"
        return None

    # ─── Double Bottom/Top ────────────────────────────────────────
    def detect_double_pattern(self, candles: List[dict],
                               highs: List[SwingPoint],
                               lows: List[SwingPoint],
                               atr: float) -> Optional[Direction]:
        if len(candles) < 15:
            return None
        price = candles[-1].get("close", 0)
        tol = atr * 0.4

        if len(lows) >= 2:
            l1, l2 = lows[-2].price, lows[-1].price
            if abs(l1 - l2) < tol and price > max(l1, l2):
                return Direction.LONG

        if len(highs) >= 2:
            h1, h2 = highs[-2].price, highs[-1].price
            if abs(h1 - h2) < tol and price < min(h1, h2):
                return Direction.SHORT
        return None


# ═══════════════════════════════════════════════════════════════════
#  SWING SIGNAL GENERATOR
# ═══════════════════════════════════════════════════════════════════

class SwingSignal:
    def __init__(self, state: BotState, analyzer: SwingAnalyzer, session_mgr: SessionMgr):
        self.state = state
        self.az = analyzer
        self.sm = session_mgr
        self.cfg = state.cfg

    def evaluate(self, price: float, spread: float) -> Optional[Tuple]:
        hour = datetime.now(timezone.utc).hour

        # ─── Gate 1: Session ──────────────────────────────────────
        if not self.sm.is_tradeable(hour):
            return None

        # ─── Gate 2: Spread ───────────────────────────────────────
        if self.cfg.SPREAD_CHECK_ENABLED and spread > self.cfg.MAX_SPREAD_POINTS:
            return None

        # ─── Gate 3: Daily limits ─────────────────────────────────
        if self.state.daily_trades >= self.cfg.MAX_DAILY_TRADES:
            return None
        if len(self.state.active_trades) >= self.cfg.MAX_CONCURRENT_TRADES:
            return None

        # ─── Gate 4: Daily loss ───────────────────────────────────
        if self.state.start_balance > 0:
            max_loss = self.state.start_balance * (self.cfg.MAX_DAILY_LOSS_PERCENT / 100)
            if self.state.daily_pnl <= -max_loss:
                return None

        # ─── Gate 5: Drawdown ─────────────────────────────────────
        if self.state.start_balance > 0 and self.state.balance > 0:
            dd = (self.state.start_balance - self.state.balance) / self.state.start_balance * 100
            if dd >= self.cfg.MAX_TOTAL_DRAWDOWN_PERCENT:
                return None

        # ─── Gate 6: Consecutive losses ───────────────────────────
        if self.state.consecutive_losses >= self.cfg.MAX_CONSECUTIVE_LOSSES:
            cooldown = self.cfg.LOSS_COOLDOWN_SECONDS * 2
            if time.time() - self.state.last_loss_time < cooldown:
                return None
            self.state.consecutive_losses = 0

        # ─── Gate 7: Trade cooldown ───────────────────────────────
        if time.time() - self.state.last_trade_time < self.cfg.TRADE_COOLDOWN_SECONDS:
            return None

        # ─── Gate 8: Loss cooldown ────────────────────────────────
        if time.time() - self.state.last_loss_time < self.cfg.LOSS_COOLDOWN_SECONDS:
            return None

        # ─── Gate 9: ADX trend filter ─────────────────────────────
        if self.state.adx < self.cfg.ADX_THRESHOLD:
            return None  # don't trade ranging markets

        # ─── Data check ──────────────────────────────────────────
        if not self.state.candles_1h or not self.state.candles_4h:
            return None

        # ─── Compute indicators ──────────────────────────────────
        atr_1h = self.az.atr(self.state.candles_1h, self.cfg.ATR_PERIOD)
        self.state.atr_1h = atr_1h

        # 1H swings
        highs_1h, lows_1h = self.az.swings(self.state.candles_1h, 3)
        obs_1h = self.az.order_blocks(self.state.candles_1h)
        fvgs_1h = self.az.fvgs(self.state.candles_1h, atr_1h)

        last_candle = self.state.candles_1h[-1]

        # ─── Build confluence ─────────────────────────────────────
        confluence = 0
        reasons = []
        direction_votes: Dict[Direction, int] = {Direction.LONG: 0, Direction.SHORT: 0}

        # 1. Daily EMA Golden Cross (macro trend)
        if self.state.macro_trend:
            direction_votes[self.state.macro_trend] += 2  # strong weight
            reasons.append(f"macro_{self.state.macro_trend.value}")

        # 2. 4H Structure Bias (BOS/CHoCH)
        if self.state.structure_bias:
            direction_votes[self.state.structure_bias] += 2  # strong weight
            reasons.append(f"struct_{self.state.structure_bias.value}")

        # 3. Premium/Discount zone alignment
        if self.state.premium_discount == "discount":
            direction_votes[Direction.LONG] += 1
            reasons.append("discount")
        elif self.state.premium_discount == "premium":
            direction_votes[Direction.SHORT] += 1
            reasons.append("premium")

        # 4. Liquidity Sweep on 1H
        sweep = self.az.liquidity_sweep(self.state.candles_1h, highs_1h, lows_1h, atr_1h)
        if sweep:
            direction_votes[sweep] += 2
            reasons.append("sweep")

        # 5. Order Block on 1H
        for ob in obs_1h:
            if ob.mitigated:
                continue
            if ob.direction == Direction.LONG and ob.low <= price <= ob.high:
                direction_votes[Direction.LONG] += 1
                reasons.append("ob")
                break
            elif ob.direction == Direction.SHORT and ob.low <= price <= ob.high:
                direction_votes[Direction.SHORT] += 1
                reasons.append("ob")
                break

        # 6. FVG on 1H
        for fvg in fvgs_1h:
            if fvg.filled:
                continue
            if fvg.direction == Direction.LONG and fvg.low <= price <= fvg.high:
                direction_votes[Direction.LONG] += 1
                reasons.append("fvg")
                break
            elif fvg.direction == Direction.SHORT and fvg.low <= price <= fvg.high:
                direction_votes[Direction.SHORT] += 1
                reasons.append("fvg")
                break

        # 7. Momentum candle
        momentum = self.az.is_momentum_candle(last_candle, atr_1h)
        if momentum:
            direction_votes[momentum] += 1
            reasons.append("momentum")

        # 8. RSI Divergence
        rsi_div = self.az.rsi_divergence(self.state.candles_1h, self.state.rsi)
        if rsi_div:
            direction_votes[rsi_div] += 1
            reasons.append("rsi_div")

        # 9. BB Squeeze breakout
        if self.state.bb_squeeze:
            momentum_dir = self.az.is_momentum_candle(last_candle, atr_1h)
            if momentum_dir:
                direction_votes[momentum_dir] += 1
                reasons.append("bb_squeeze")

        # 10. Mean Reversion
        if self.cfg.USE_MEAN_REVERSION:
            mr = self.az.mean_reversion(self.state.candles_1h, price, self.state.rsi)
            if mr:
                mr_dir, mr_score, mr_reason = mr
                direction_votes[mr_dir] += self.cfg.MR_CONFLUENCE_SCORE
                reasons.append(mr_reason)

        # 11. Double Bottom/Top
        dbl = self.az.detect_double_pattern(self.state.candles_1h, highs_1h, lows_1h, atr_1h)
        if dbl:
            direction_votes[dbl] += 1
            reasons.append("dbl_pattern")

        # ─── Determine direction ──────────────────────────────────
        long_score = direction_votes[Direction.LONG]
        short_score = direction_votes[Direction.SHORT]

        if long_score > short_score and long_score >= 2:
            direction = Direction.LONG
            confluence += long_score
        elif short_score > long_score and short_score >= 2:
            direction = Direction.SHORT
            confluence += short_score
        else:
            return None

        # ─── Alignment filters ────────────────────────────────────

        # Macro trend alignment (must agree for swings)
        if self.state.macro_trend and direction != self.state.macro_trend:
            # Allow mean reversion counter-trend
            if "MR:" not in str(reasons):
                return None

        # Structure alignment
        if self.state.structure_bias and direction != self.state.structure_bias:
            if "MR:" not in str(reasons):
                return None

        # Premium/Discount alignment
        if direction == Direction.LONG and self.state.premium_discount == "premium":
            return None  # don't buy in premium
        if direction == Direction.SHORT and self.state.premium_discount == "discount":
            return None  # don't sell in discount

        # ─── Minimum confluence ───────────────────────────────────
        if confluence < self.cfg.MIN_CONFLUENCE:
            return None

        # ─── Calculate SL/TP ──────────────────────────────────────
        sl_dist = max(atr_1h * self.cfg.ATR_SL_MULTIPLIER, self.cfg.MIN_SL_POINTS)
        sl_dist = min(sl_dist, self.cfg.MAX_SL_POINTS)

        tp_dist = sl_dist * self.cfg.RR_RATIO
        tp1_dist = sl_dist * self.cfg.TP1_RR_RATIO

        if direction == Direction.LONG:
            sl = price - sl_dist
            tp = price + tp_dist
            tp1 = price + tp1_dist
        else:
            sl = price + sl_dist
            tp = price - tp_dist
            tp1 = price - tp1_dist

        reason_str = " | ".join(reasons)
        log.info(f"⚡ SWING SIGNAL: {direction.value.upper()} | Score: {confluence} | {reason_str}")

        return direction, sl, tp1, tp, confluence, reason_str


# ═══════════════════════════════════════════════════════════════════
#  POSITION MANAGER
# ═══════════════════════════════════════════════════════════════════

class PositionMgr:
    def __init__(self, state, conn, db, tg):
        self.state = state
        self.conn = conn
        self.db = db
        self.tg = tg
        self.cfg = state.cfg

    async def open_swing(self, direction, price, sl, tp1, tp, confluence, reason):
        sl_dist = abs(price - sl)
        if sl_dist <= 0:
            return

        risk = self.state.balance * (self.cfg.RISK_PERCENT / 100)
        lots = risk / (sl_dist * 100)
        lots = max(0.01, min(round(lots, 2), 1.0))

        try:
            order = await asyncio.wait_for(
                self.conn.create_market_buy_order(self.cfg.SYMBOL, lots, sl, tp,
                    {"comment": f"swing_{confluence}"})
                if direction == Direction.LONG else
                self.conn.create_market_sell_order(self.cfg.SYMBOL, lots, sl, tp,
                    {"comment": f"swing_{confluence}"}),
                timeout=15
            )

            tid = order.get("positionId") or order.get("orderId", "")
            if tid:
                trade = SwingTrade(
                    id=tid, direction=direction, entry=price,
                    sl=sl, tp1=tp1, tp=tp, lots=lots,
                    phase=TradePhase.OPEN, open_time=time.time(),
                    reason=reason
                )
                self.state.active_trades[tid] = trade
                self.state.daily_trades += 1
                self.state.last_trade_time = time.time()
                self.db.save_trade(trade)

                session = self.state.session.value
                await self.tg.swing_opened(trade, confluence, session)
                log.info(f"OPENED: {direction.value} {lots}L @ ${price:.2f} | SL: ${sl:.2f} | TP: ${tp:.2f}")

        except Exception as e:
            log.error(f"Open error: {e}")

    async def manage_partials(self, price: float):
        for tid, t in list(self.state.active_trades.items()):
            if t.phase != TradePhase.OPEN:
                continue
            hit = ((t.direction == Direction.LONG and price >= t.tp1) or
                   (t.direction == Direction.SHORT and price <= t.tp1))
            if not hit:
                continue
            close_lots = round(t.lots * self.cfg.PARTIAL_PERCENT, 2)
            if close_lots < 0.01:
                continue
            try:
                await asyncio.wait_for(
                    self.conn.close_position_partially(tid, close_lots), timeout=10
                )
                t.phase = TradePhase.TP1_HIT
                log.info(f"TP1 HIT: {close_lots}L closed, SL → BE")
                if self.cfg.MOVE_SL_TO_BE:
                    await self.conn.modify_position(tid, stop_loss=t.entry, take_profit=t.tp)
                    t.sl = t.entry
                self.db.save_trade(t)
                await self.tg.swing_partial(t)
            except Exception as e:
                log.error(f"Partial error: {e}")

    async def manage_trailing(self, price: float):
        if not self.cfg.USE_TRAILING_STOP:
            return
        for tid, t in list(self.state.active_trades.items()):
            if t.phase not in (TradePhase.TP1_HIT, TradePhase.TRAILING):
                continue
            sl_dist = abs(t.tp - t.entry) / self.cfg.RR_RATIO if self.cfg.RR_RATIO > 0 else abs(t.entry - t.sl)
            if sl_dist <= 0:
                sl_dist = self.state.atr_1h * self.cfg.ATR_SL_MULTIPLIER

            if t.direction == Direction.LONG:
                profit_r = (price - t.entry) / sl_dist
            else:
                profit_r = (t.entry - price) / sl_dist

            if profit_r < self.cfg.TRAIL_ACTIVATION_RR:
                continue

            t.phase = TradePhase.TRAILING
            trail_dist = self.state.atr_1h * self.cfg.TRAIL_ATR_MULT

            if t.direction == Direction.LONG:
                new_sl = price - trail_dist
                if new_sl > t.sl + self.cfg.TRAIL_STEP:
                    try:
                        await asyncio.wait_for(
                            self.conn.modify_position(tid, stop_loss=new_sl), timeout=10
                        )
                        t.sl = new_sl
                        log.info(f"🎯 TRAIL: SL → ${new_sl:.2f} | {profit_r:.1f}R")
                        await self.tg.swing_trailing(t, new_sl, profit_r)
                    except Exception as e:
                        log.error(f"Trail error: {e}")
            else:
                new_sl = price + trail_dist
                if new_sl < t.sl - self.cfg.TRAIL_STEP:
                    try:
                        await asyncio.wait_for(
                            self.conn.modify_position(tid, stop_loss=new_sl), timeout=10
                        )
                        t.sl = new_sl
                        log.info(f"🎯 TRAIL: SL → ${new_sl:.2f} | {profit_r:.1f}R")
                        await self.tg.swing_trailing(t, new_sl, profit_r)
                    except Exception as e:
                        log.error(f"Trail error: {e}")

    async def sync_positions(self):
        try:
            positions = await self.conn.get_positions()
            open_ids = {p.get("id") for p in positions if p.get("symbol") == self.cfg.SYMBOL}

            for tid, t in list(self.state.active_trades.items()):
                if tid not in open_ids and t.phase != TradePhase.CLOSED:
                    t.phase = TradePhase.CLOSED

                    # PnL from deals history first
                    try:
                        now = datetime.now(timezone.utc).replace(tzinfo=None)
                        start = now - timedelta(hours=4)
                        history = await asyncio.wait_for(
                            self.conn.get_deals_by_time_range(start, now), timeout=10
                        )
                        if history:
                            for deal in history:
                                if deal.get("positionId") == tid and deal.get("profit", 0) != 0:
                                    t.pnl = deal.get("profit", 0) + deal.get("swap", 0) + deal.get("commission", 0)
                                    break
                    except Exception:
                        pass

                    # Fallback: price estimate
                    if t.pnl == 0:
                        try:
                            tick = await self.conn.get_symbol_price(self.cfg.SYMBOL)
                            cp = tick.get("bid", 0) if t.direction == Direction.LONG else tick.get("ask", 0)
                            if cp > 0:
                                if t.direction == Direction.LONG:
                                    t.pnl = (cp - t.entry) * t.lots * 100
                                else:
                                    t.pnl = (t.entry - cp) * t.lots * 100
                        except Exception:
                            pass

                    self.state.daily_pnl += t.pnl
                    if t.pnl > 0:
                        self.state.daily_wins += 1
                        self.state.consecutive_losses = 0
                    else:
                        self.state.daily_losses += 1
                        self.state.consecutive_losses += 1
                        self.state.last_loss_time = time.time()

                    self.db.save_trade(t)
                    del self.state.active_trades[tid]
                    log.info(f"CLOSED: {tid} | PnL: ${t.pnl:+.2f}")
                    await self.tg.swing_closed(t)
        except Exception as e:
            log.error(f"Sync error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  MAIN BOT
# ═══════════════════════════════════════════════════════════════════

class GoldSwingBot:
    def __init__(self):
        self.cfg = SwingConfig()
        self.state = BotState(self.cfg)
        self.az = SwingAnalyzer(self.cfg)
        self.sm = SessionMgr(self.cfg)
        self.db = Database(self.cfg.DB_PATH)
        self.tg = Telegram(self.state)
        self.sig = SwingSignal(self.state, self.az, self.sm)
        self.pos: Optional[PositionMgr] = None
        self.api = None
        self.account = None
        self.conn = None

    async def connect(self):
        log.info("Connecting to MetaAPI...")
        if not self.cfg.META_API_TOKEN or not self.cfg.ACCOUNT_ID:
            log.error("Set METAAPI_TOKEN and ACCOUNT_ID!")
            sys.exit(1)

        self.api = MetaApi(self.cfg.META_API_TOKEN)
        self.account = await self.api.metatrader_account_api.get_account(self.cfg.ACCOUNT_ID)

        if self.account.state != "DEPLOYED":
            await self.account.deploy()

        await self.account.wait_connected()
        self.conn = self.account.get_rpc_connection()
        await self.conn.connect()
        await self.conn.wait_synchronized()

        info = await self.conn.get_account_information()
        self.state.start_balance = info.get("balance", 0)
        self.state.balance = self.state.start_balance
        self.state.trade_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        self.pos = PositionMgr(self.state, self.conn, self.db, self.tg)

        log.info(f"Connected! Balance: ${self.state.start_balance:.2f}")
        await self.tg.send(
            f"🤖 <b>Gold Swing Bot v1.0 Started</b>\n"
            f"Balance: ${self.state.start_balance:.2f}\n"
            f"SL: ATR×{self.cfg.ATR_SL_MULTIPLIER} | RR: 1:{self.cfg.RR_RATIO}\n"
            f"TP1: {self.cfg.TP1_RR_RATIO}R | Partial: {self.cfg.PARTIAL_PERCENT*100:.0f}%\n"
            f"Trail: {self.cfg.TRAIL_ACTIVATION_RR}R | Max: {self.cfg.MAX_CONCURRENT_TRADES} trades\n"
            f"Risk: {self.cfg.RISK_PERCENT}%"
        )

    async def _reconnect(self):
        for attempt in range(3):
            try:
                log.info(f"Reconnect attempt {attempt + 1}/3...")
                try:
                    await self.conn.close()
                except Exception:
                    pass
                await asyncio.sleep(5 * (attempt + 1))
                self.conn = self.account.get_rpc_connection()
                await self.conn.connect()
                await asyncio.wait_for(self.conn.wait_synchronized(), timeout=30)
                test = await asyncio.wait_for(self.conn.get_account_information(), timeout=10)
                if test and "balance" in test:
                    self.pos.conn = self.conn
                    log.info("Reconnected!")
                    await self.tg.send("✅ <b>RECONNECTED</b>")
                    return True
            except Exception as e:
                log.warning(f"Reconnect {attempt + 1}/3 failed: {e}")
        # Hard reconnect
        try:
            log.info("Hard reconnect...")
            await self.account.undeploy()
            await asyncio.sleep(15)
            await self.account.deploy()
            await asyncio.sleep(30)
            self.conn = self.account.get_rpc_connection()
            await self.conn.connect()
            await asyncio.wait_for(self.conn.wait_synchronized(), timeout=60)
            self.pos.conn = self.conn
            log.info("Hard reconnect success!")
            await self.tg.send("✅ <b>RECONNECTED via REDEPLOY</b>")
            return True
        except Exception as e:
            log.error(f"Hard reconnect failed: {e}")
            return False

    async def fetch_data(self):
        try:
            self.state.candles_1h = await asyncio.wait_for(
                self.account.get_historical_candles(
                    self.cfg.SYMBOL, self.cfg.TF_ENTRY,
                    datetime.now(timezone.utc), self.cfg.CANDLE_LOOKBACK_1H
                ), timeout=15
            ) or []
            self.state.candles_4h = await asyncio.wait_for(
                self.account.get_historical_candles(
                    self.cfg.SYMBOL, self.cfg.TF_STRUCTURE,
                    datetime.now(timezone.utc), self.cfg.CANDLE_LOOKBACK_4H
                ), timeout=15
            ) or []
            self.state.candles_daily = await asyncio.wait_for(
                self.account.get_historical_candles(
                    self.cfg.SYMBOL, self.cfg.TF_TREND,
                    datetime.now(timezone.utc), self.cfg.CANDLE_LOOKBACK_DAILY
                ), timeout=15
            ) or []
        except Exception as e:
            log.warning(f"Candle fetch error: {e}")

    async def get_price_and_spread(self):
        try:
            tick = await asyncio.wait_for(
                self.conn.get_symbol_price(self.cfg.SYMBOL), timeout=10
            )
            bid = tick.get("bid", 0)
            ask = tick.get("ask", 0)
            return (bid + ask) / 2, ask - bid
        except Exception:
            return 0, 999

    async def daily_reset(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.state.trade_date:
            if self.state.daily_trades > 0:
                self.db.save_daily(
                    self.state.trade_date, self.state.daily_trades,
                    self.state.daily_wins, self.state.daily_losses, self.state.daily_pnl
                )
                await self.tg.daily_report()
            self.state.trade_date = today
            self.state.daily_pnl = 0.0
            self.state.daily_trades = 0
            self.state.daily_wins = 0
            self.state.daily_losses = 0

    async def update_analysis(self, price: float):
        """Update all multi-timeframe analysis."""
        # Daily EMA Golden Cross
        if len(self.state.candles_daily) >= 200:
            self.state.ema50 = self.az.ema(self.state.candles_daily, 50)
            self.state.ema200 = self.az.ema(self.state.candles_daily, 200)
            if self.state.ema50 > self.state.ema200:
                self.state.macro_trend = Direction.LONG
            elif self.state.ema50 < self.state.ema200:
                self.state.macro_trend = Direction.SHORT
            else:
                self.state.macro_trend = None

        # 4H Structure (BOS/CHoCH)
        if len(self.state.candles_4h) >= 30:
            atr_4h = self.az.atr(self.state.candles_4h)
            self.state.structure_bias = self.az.detect_structure(self.state.candles_4h, atr_4h)
            self.state.premium_discount = self.az.premium_discount(self.state.candles_4h)

        # 1H Indicators
        if len(self.state.candles_1h) >= 20:
            self.state.rsi = self.az.rsi(self.state.candles_1h, self.cfg.RSI_PERIOD)
            self.state.adx = self.az.adx(self.state.candles_1h)
            self.state.atr_1h = self.az.atr(self.state.candles_1h)

            _, self.state.bb_upper, self.state.bb_lower = self.az.bollinger(
                self.state.candles_1h, self.cfg.BB_PERIOD, self.cfg.BB_STD
            )
            self.state.bb_squeeze = self.az.detect_squeeze(self.state.candles_1h, self.state.atr_1h)

            # Determine regime
            if self.state.bb_squeeze:
                self.state.regime = MarketRegime.SQUEEZE
            elif self.state.adx >= self.cfg.ADX_THRESHOLD:
                if self.state.macro_trend == Direction.LONG:
                    self.state.regime = MarketRegime.TRENDING_UP
                elif self.state.macro_trend == Direction.SHORT:
                    self.state.regime = MarketRegime.TRENDING_DOWN
                else:
                    self.state.regime = MarketRegime.RANGING
            else:
                self.state.regime = MarketRegime.RANGING

    async def cycle(self):
        self.state.heartbeat = time.time()

        await self.daily_reset()
        await self.fetch_data()

        price, spread = await self.get_price_and_spread()
        if price <= 0:
            return

        # Update balance
        try:
            info = await self.conn.get_account_information()
            self.state.balance = info.get("balance", self.state.balance)
            self.state.equity = info.get("equity", self.state.equity)
        except Exception:
            pass

        # Update session
        hour = datetime.now(timezone.utc).hour
        self.state.session = self.sm.get(hour)

        # Multi-timeframe analysis
        await self.update_analysis(price)

        # Sync positions
        await self.pos.sync_positions()

        # Manage partials + trailing
        await self.pos.manage_partials(price)
        await self.pos.manage_trailing(price)

        # Heartbeat
        open_count = len(self.state.active_trades)
        await self.tg.heartbeat(price, spread, open_count)

        # Status log every 30 cycles (~30 min)
        if not hasattr(self, '_cycle_count'):
            self._cycle_count = 0
        self._cycle_count += 1
        if self._cycle_count % 30 == 0:
            trend = self.state.macro_trend.value if self.state.macro_trend else "N/A"
            struct = self.state.structure_bias.value if self.state.structure_bias else "N/A"
            log.info(
                f"STATUS: ${price:.2f} | Spread: ${spread:.2f} | "
                f"Trend: {trend} | Structure: {struct} | "
                f"Regime: {self.state.regime.value} | Zone: {self.state.premium_discount} | "
                f"RSI: {self.state.rsi:.0f} | ADX: {self.state.adx:.0f} | "
                f"Squeeze: {self.state.bb_squeeze} | "
                f"Trades: {self.state.daily_trades}/{self.cfg.MAX_DAILY_TRADES}"
            )

        # Check for signal
        signal = self.sig.evaluate(price, spread)
        if signal:
            direction, sl, tp1, tp, confluence, reason = signal
            await self.pos.open_swing(direction, price, sl, tp1, tp, confluence, reason)

    async def run(self):
        log.info(f"Swing loop started (cycle: {self.cfg.MAIN_LOOP_SECONDS}s)")
        consecutive_errors = 0

        while self.state.running:
            try:
                await self.cycle()
                consecutive_errors = 0
            except asyncio.CancelledError:
                consecutive_errors += 1
                if consecutive_errors >= 5:
                    success = await self._reconnect()
                    if success:
                        consecutive_errors = 0
                else:
                    await asyncio.sleep(10)
            except Exception as e:
                consecutive_errors += 1
                err = str(e).lower()
                if any(x in err for x in ["not connected", "not synchronized", "socket", "timed out"]):
                    if consecutive_errors >= 3:
                        success = await self._reconnect()
                        if success:
                            consecutive_errors = 0
                    else:
                        await asyncio.sleep(15)
                else:
                    log.error(f"Cycle error: {e}", exc_info=True)
                    await self.tg.send(f"⚠️ Error: {str(e)[:100]}")
                    if consecutive_errors >= 10:
                        await self._reconnect()
                        consecutive_errors = 0
            await asyncio.sleep(self.cfg.MAIN_LOOP_SECONDS)

    async def start(self):
        try:
            await self.connect()
            # Watchdog
            async def watchdog():
                while self.state.running:
                    await asyncio.sleep(60)
                    if time.time() - self.state.heartbeat > self.cfg.WATCHDOG_TIMEOUT:
                        log.critical("Watchdog timeout — restarting")
                        os._exit(1)
            asyncio.create_task(watchdog())
            await self.run()
        except KeyboardInterrupt:
            log.info("Shutting down...")
        except Exception as e:
            log.error(f"Fatal: {e}", exc_info=True)
            await self.tg.send(f"❌ <b>FATAL:</b> {str(e)[:200]}")
        finally:
            self.state.running = False
            self.db.close()
            log.info("Bot stopped.")


def main():
    def handle_signal(sig, frame):
        log.info(f"Signal {sig} received")
        sys.exit(0)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    bot = GoldSwingBot()
    asyncio.run(bot.start())


if __name__ == "__main__":
    main()
