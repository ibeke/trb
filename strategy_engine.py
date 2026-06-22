from __future__ import annotations

"""
StrategyEngine
==============
Két stratégia implementációja:
  - mode 0 — Mean Reversion (Bollinger Bands)
  - mode 1 — Trend Following (EMA crossover + ADX)

Mindkét stratégia csak dict-et és listát használ bemenetként,
így MockFeed-del is tesztelhető WebSocket-kapcsolat nélkül.
"""

import math
from typing import Optional


# ── Segédfüggvények ────────────────────────────────────────────────────────────

def _closes(candles: list[dict]) -> list[float]:
    return [c["close"] for c in candles]


def _highs(candles: list[dict]) -> list[float]:
    return [c["high"] for c in candles]


def _lows(candles: list[dict]) -> list[float]:
    return [c["low"] for c in candles]


def _sma(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _std(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    window = values[-period:]
    mean = sum(window) / period
    variance = sum((x - mean) ** 2 for x in window) / period
    return math.sqrt(variance)


def _ema(values: list[float], period: int) -> Optional[float]:
    """Exponenciális mozgóátlag az összes értékre, utolsó értéket adja vissza."""
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema


def _ema_series(values: list[float], period: int) -> list[Optional[float]]:
    """EMA sorozat — minden indexre visszaadja az értéket (None ha nincs elég adat)."""
    result: list[Optional[float]] = [None] * len(values)
    if len(values) < period:
        return result
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    result[period - 1] = ema
    for i in range(period, len(values)):
        ema = values[i] * k + ema * (1 - k)
        result[i] = ema
    return result


def _atr(candles: list[dict], period: int) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def _adx(candles: list[dict], period: int) -> Optional[float]:
    """
    Egyszerűsített ADX számítás (Wilder-simítással).
    Minimum: 2 * period + 1 gyertya szükséges.
    """
    n = len(candles)
    if n < 2 * period + 1:
        return None

    plus_dm_list: list[float] = []
    minus_dm_list: list[float] = []
    tr_list: list[float] = []

    for i in range(1, n):
        high_diff = candles[i]["high"] - candles[i - 1]["high"]
        low_diff = candles[i - 1]["low"] - candles[i]["low"]

        plus_dm = high_diff if (high_diff > low_diff and high_diff > 0) else 0.0
        minus_dm = low_diff if (low_diff > high_diff and low_diff > 0) else 0.0

        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))

        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)
        tr_list.append(tr)

    def _wilder(lst: list[float], p: int) -> list[float]:
        if len(lst) < p:
            return []
        smoothed = [sum(lst[:p])]
        for v in lst[p:]:
            smoothed.append(smoothed[-1] - smoothed[-1] / p + v)
        return smoothed

    smooth_tr = _wilder(tr_list, period)
    smooth_plus = _wilder(plus_dm_list, period)
    smooth_minus = _wilder(minus_dm_list, period)

    if not smooth_tr:
        return None

    dx_list: list[float] = []
    for atr_v, plus_v, minus_v in zip(smooth_tr, smooth_plus, smooth_minus):
        if atr_v == 0:
            continue
        di_plus = 100 * plus_v / atr_v
        di_minus = 100 * minus_v / atr_v
        denom = di_plus + di_minus
        if denom == 0:
            continue
        dx_list.append(100 * abs(di_plus - di_minus) / denom)

    if len(dx_list) < period:
        return None
    return sum(dx_list[-period:]) / period


# ── Stratégiák ─────────────────────────────────────────────────────────────────

class MeanReversionStrategy:
    """
    Mode 0 — Bollinger Bands mean reversion.
    Belépési jel: close < lower_band
    Limit ár: lower_band * (1 - limit_offset_pct)
    """

    def evaluate(self, cfg: dict, candles: list[dict]) -> Optional[dict]:
        period: int = cfg.get("bb_period", 20)
        std_mult: float = cfg.get("bb_std", 2.0)
        offset: float = cfg.get("limit_offset_pct", 0.001)

        if len(candles) < period:
            return None

        closes = _closes(candles)
        sma = _sma(closes, period)
        std = _std(closes, period)
        if sma is None or std is None or std == 0:
            return None

        lower_band = sma - std_mult * std
        upper_band = sma + std_mult * std
        current_close = closes[-1]

        if current_close >= lower_band:
            return None  # Nincs jel

        atr = _atr(candles, cfg.get("atr_period", 14))
        limit_price = round(lower_band * (1 - offset), 4)

        return {
            "limit_price": limit_price,
            "indicators": {
                "bb_sma": round(sma, 4),
                "bb_lower": round(lower_band, 4),
                "bb_upper": round(upper_band, 4),
                "bb_std": round(std, 4),
                "close": current_close,
                "atr": round(atr, 4) if atr else None,
            },
        }


class TrendFollowingStrategy:
    """
    Mode 1 — EMA crossover + ADX szűrő.
    Belépési jel: EMA(fast) keresztezi felfelé EMA(slow)-t ÉS ADX > threshold
    Limit ár: EMA(slow) szintje (spec szerint)
    """

    def evaluate(self, cfg: dict, candles: list[dict]) -> Optional[dict]:
        fast: int = cfg.get("ema_fast", 20)
        slow: int = cfg.get("ema_slow", 50)
        adx_period: int = cfg.get("adx_period", 14)
        adx_threshold: float = cfg.get("adx_threshold", 25.0)
        offset: float = cfg.get("limit_offset_pct", 0.001)

        min_candles = max(slow + 1, 2 * adx_period + 1)
        if len(candles) < min_candles:
            return None

        closes = _closes(candles)
        ema_fast_series = _ema_series(closes, fast)
        ema_slow_series = _ema_series(closes, slow)

        # Utolsó két érvényes érték kereszteződés vizsgálathoz
        idx = len(closes) - 1
        if ema_fast_series[idx] is None or ema_slow_series[idx] is None:
            return None
        if ema_fast_series[idx - 1] is None or ema_slow_series[idx - 1] is None:
            return None

        ef_now: float = ema_fast_series[idx]       # type: ignore[assignment]
        es_now: float = ema_slow_series[idx]        # type: ignore[assignment]
        ef_prev: float = ema_fast_series[idx - 1]   # type: ignore[assignment]
        es_prev: float = ema_slow_series[idx - 1]   # type: ignore[assignment]

        # EMA crossover: fast átlép slow fölé
        crossover = ef_prev <= es_prev and ef_now > es_now
        if not crossover:
            return None

        adx = _adx(candles, adx_period)
        if adx is None or adx <= adx_threshold:
            return None

        limit_price = round(es_now * (1 - offset), 4)

        return {
            "limit_price": limit_price,
            "indicators": {
                "ema_fast": round(ef_now, 4),
                "ema_slow": round(es_now, 4),
                "adx": round(adx, 2),
                "close": closes[-1],
            },
        }


class BreakoutStrategy:
    """
    Mode 2 — Volume-confirmed breakout.

    Belépési jel: a close áttöri a legutóbbi N periódus maximumát (resistance)
    ÉS az aktuális gyertya volumene meghaladja az N periódos volumen-mozgóátlag
    `volume_factor`-szorosát.

    Ez az ún. "high-of-range breakout": ha az árfolyam kitör a megfigyelt
    tartomány teteje fölé, és ezt erős forgalom kíséri, trenddel megegyező
    irányú belépési jelként értékeljük.

    Limit ár: a kitörési szint (breakout_high) + limit_offset_pct, hogy az
    order a kitörési pont felett teljesüljön (pullback elkerülése).
    """

    def evaluate(self, cfg: dict, candles: list[dict]) -> Optional[dict]:
        period: int = cfg.get("breakout_period", 20)
        vol_period: int = cfg.get("volume_ma_period", 20)
        vol_factor: float = cfg.get("volume_factor", 1.5)
        offset: float = cfg.get("limit_offset_pct", 0.001)

        # Minimum gyertyaszám: az előző N gyertya high-ja + 1 aktuális
        # + elegendő adat a volumen-MA-hoz
        min_candles = max(period, vol_period) + 1
        if len(candles) < min_candles:
            return None

        # Kitörési szint: az utolsó `period` gyertya (aktuálist NEM beleértve) maximuma
        lookback = list(candles)[-(period + 1):-1]
        if len(lookback) < period:
            return None

        breakout_high = max(c["high"] for c in lookback)
        current = candles[-1]
        current_close = current["close"]

        # Feltétel 1: az aktuális close áttöri a resistance-t
        if current_close <= breakout_high:
            return None

        # Feltétel 2: volume-konfirmáció
        # Csak azok a gyertyák számítanak a volumen-MA-ba, amelyeknek van
        # "volume" kulcsa; ha nincs (pl. warmup során kihagyják), kihagyjuk.
        vol_window = [
            c["volume"] for c in list(candles)[-(vol_period + 1):-1]
            if "volume" in c and c["volume"] is not None
        ]
        if len(vol_window) < vol_period:
            # Nincs elég volumen-adat → nem erősítjük meg, nincs jel
            return None

        volume_ma = sum(vol_window[-vol_period:]) / vol_period
        current_volume = current.get("volume")
        if current_volume is None or volume_ma <= 0:
            return None
        if current_volume < vol_factor * volume_ma:
            return None  # Gyenge forgalom, nincs konfirmáció

        # ATR a stop-loss referencia-értékéhez (logginghoz hasznos)
        atr = _atr(candles, cfg.get("atr_period", 14))

        # Limit ár: kitörési szint fölé, hogy csak valódi áttörés esetén
        # teljesüljön (ne kösse le a tőkét hamis kitörésnél)
        limit_price = round(breakout_high * (1 + offset), 4)

        return {
            "limit_price": limit_price,
            "indicators": {
                "breakout_high": round(breakout_high, 4),
                "close": current_close,
                "volume": current_volume,
                "volume_ma": round(volume_ma, 2),
                "volume_ratio": round(current_volume / volume_ma, 3) if volume_ma else None,
                "atr": round(atr, 4) if atr else None,
            },
        }


# ── Koordinátor ────────────────────────────────────────────────────────────────

class StrategyEngine:
    """Elosztja a gyertyákat a megfelelő stratégiához."""

    def __init__(self):
        self._mean_rev = MeanReversionStrategy()
        self._trend = TrendFollowingStrategy()
        self._breakout = BreakoutStrategy()

    def evaluate(self, cfg: dict, candles: list[dict]) -> Optional[dict]:
        mode = cfg.get("mode", 0)
        if mode == 0:
            return self._mean_rev.evaluate(cfg, candles)
        elif mode == 1:
            return self._trend.evaluate(cfg, candles)
        elif mode == 2:
            return self._breakout.evaluate(cfg, candles)
        return None
