from __future__ import annotations

"""
backtester.py — Historikus teljesítmény-riport
=================================================

Szimbólumonkénti és stratégia/mode szerinti visszatesztelés,
amely kizárólag a már implementált stratégiaosztályokat
(MeanReversionStrategy, TrendFollowingStrategy, BreakoutStrategy)
használja — nem kell külső adat-könyvtár.

Indítás önállóan:
    python -m modules.backtester --config config/stcks.json

Vagy importálva:
    from modules.backtester import Backtester
    bt = Backtester(cfg)
    results = bt.run(candles_by_symbol)   # dict[sym, list[dict]]
    bt.print_report(results)
    bt.save_csv(results, "data/backtest.csv")

Gyertyaformátum (ugyanaz, mint a main.py pufferében):
    {"open": float, "high": float, "low": float, "close": float,
     "volume": float|int, "ts": datetime|str}

Bracket-szimulációs modell:
    - Belépés: limit_price-on feltételezünk teljesülést, ha a
      következő gyertya low ≤ limit_price (konzervatív feltételezés).
    - Take-profit: ha a belépés utáni bármelyik gyertya high ≥ tp_price.
    - Stop-loss: ha a belépés utáni bármelyik gyertya low ≤ sl_price.
    - Mindkettő ugyanazon a gyertyán is teljesülhet; ha igen, a sl_price-t
      vesszük (pesszimista sorrend: gap-down eset kezelése).
    - Nyitott trade a sorozat végén: nincs PnL (nem számol be részleges
      eredménnyel), csak számolja mint "open_at_end".
"""

import csv
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from modules.strategy_engine import (
    BreakoutStrategy,
    MeanReversionStrategy,
    TrendFollowingStrategy,
)


# ── Belső adatszerkezetek ──────────────────────────────────────────────────────

@dataclass
class TradeResult:
    symbol:      str
    mode:        int
    strategy:    str
    entry_idx:   int          # gyertyaindex a candles listában
    entry_price: float
    tp_price:    float
    sl_price:    float
    exit_price:  Optional[float]
    exit_type:   str          # "tp" | "sl" | "open_at_end"
    pnl_pct:     Optional[float]   # (exit - entry) / entry
    bars_held:   Optional[int]     # hány gyertya volt nyitva


@dataclass
class SymbolReport:
    symbol:       str
    mode:         int
    strategy:     str
    total_trades: int = 0
    tp_hits:      int = 0
    sl_hits:      int = 0
    open_at_end:  int = 0
    total_pnl_pct: float = 0.0
    max_win_pct:  float = 0.0
    max_loss_pct: float = 0.0
    trades:       list[TradeResult] = field(default_factory=list)

    @property
    def win_rate(self) -> Optional[float]:
        closed = self.tp_hits + self.sl_hits
        return self.tp_hits / closed if closed > 0 else None

    @property
    def avg_pnl_pct(self) -> Optional[float]:
        closed = self.tp_hits + self.sl_hits
        if closed == 0:
            return None
        total = sum(
            t.pnl_pct for t in self.trades
            if t.pnl_pct is not None
        )
        return total / closed

    @property
    def profit_factor(self) -> Optional[float]:
        gross_win  = sum(t.pnl_pct for t in self.trades if (t.pnl_pct or 0) > 0)
        gross_loss = sum(abs(t.pnl_pct) for t in self.trades if (t.pnl_pct or 0) < 0)
        if gross_loss == 0:
            return None
        return gross_win / gross_loss

    @property
    def avg_bars_held(self) -> Optional[float]:
        held = [t.bars_held for t in self.trades if t.bars_held is not None]
        return sum(held) / len(held) if held else None

    @property
    def sharpe_approx(self) -> Optional[float]:
        """Egyszerűsített Sharpe ráta a trade-enkénti PnL%-ból.

        Nem évesített, de összehasonlításra alkalmas relatív mutató.
        Null ha kevesebb mint 2 zárt trade van.
        """
        pnls = [t.pnl_pct for t in self.trades if t.pnl_pct is not None]
        if len(pnls) < 2:
            return None
        mean = sum(pnls) / len(pnls)
        variance = sum((p - mean) ** 2 for p in pnls) / len(pnls)
        std = math.sqrt(variance)
        if std == 0:
            return None
        return mean / std


# ── Backtester ─────────────────────────────────────────────────────────────────

class Backtester:
    """
    Walk-forward szimuláció: gyertyáról-gyertyára haladva meghívja
    a stratégiát, majd szimulálja a bracket order teljesülését.

    Paraméterek:
        cfg         — stcks.json-ból betöltött konfiguráció dict
                      (minden szimbólumhoz mode, strategy, stop_loss_pct,
                       take_profit_pct stb.)
        commission  — egységnyi kereskedési költség (arány), default 0.0
                      (Alpaca free tier: 0 bizományosi díj)
    """

    def __init__(self, cfg: dict, commission: float = 0.0):
        self.cfg        = cfg
        self.commission = commission
        self._strats = {
            0: MeanReversionStrategy(),
            1: TrendFollowingStrategy(),
            2: BreakoutStrategy(),
        }

    # ── Fő belépési pont ───────────────────────────────────────────────────────

    def run(self, candles_by_symbol: dict[str, list[dict]]) -> dict[str, SymbolReport]:
        """Visszatesztelés futtatása minden szimbólumra.

        Args:
            candles_by_symbol: {szimbólum: gyertya-lista}

        Returns:
            {szimbólum: SymbolReport}
        """
        reports: dict[str, SymbolReport] = {}
        for sym, cfg in self.cfg.items():
            candles = candles_by_symbol.get(sym, [])
            if not candles:
                continue
            reports[sym] = self._backtest_symbol(sym, cfg, candles)
        return reports

    # ── Szimbólum szintű szimuláció ────────────────────────────────────────────

    def _backtest_symbol(
        self, sym: str, cfg: dict, candles: list[dict]
    ) -> SymbolReport:
        mode     = cfg.get("mode", 0)
        strategy = cfg.get("strategy", "unknown")
        sl_pct   = cfg.get("stop_loss_pct", 0.02)
        tp_pct   = cfg.get("take_profit_pct", 0.04)
        strategy_obj = self._strats.get(mode)

        report = SymbolReport(symbol=sym, mode=mode, strategy=strategy)

        i = 0
        while i < len(candles):
            # Stratégia értékelése az i-edik gyertyáig (inclusive)
            window = candles[:i + 1]
            signal = strategy_obj.evaluate(cfg, window) if strategy_obj else None

            if signal is None:
                i += 1
                continue

            limit_price = signal["limit_price"]
            tp_price    = round(limit_price * (1 + tp_pct), 4)
            sl_price    = round(limit_price * (1 - sl_pct), 4)

            # Próbálunk belépni a következő gyertyán
            if i + 1 >= len(candles):
                # Nincs következő gyertya → szimulációs végpont, nincs trade
                break

            entry_bar = candles[i + 1]
            # Belépési feltétel: a következő gyertya low-ja eléri a limit árat
            if entry_bar["low"] > limit_price:
                i += 2   # a gyertya nem érte el a limitet, ugrunk
                continue

            # Belépés sikerült
            entry_idx = i + 1
            trade = self._simulate_trade(
                sym=sym,
                mode=mode,
                strategy=strategy,
                candles=candles,
                entry_idx=entry_idx,
                entry_price=limit_price,
                tp_price=tp_price,
                sl_price=sl_price,
            )
            report.trades.append(trade)
            report.total_trades += 1

            if trade.exit_type == "tp":
                report.tp_hits += 1
            elif trade.exit_type == "sl":
                report.sl_hits += 1
            else:
                report.open_at_end += 1

            if trade.pnl_pct is not None:
                report.total_pnl_pct += trade.pnl_pct
                report.max_win_pct   = max(report.max_win_pct, trade.pnl_pct)
                report.max_loss_pct  = min(report.max_loss_pct, trade.pnl_pct)

            # Következő szignálkeresés a trade zárása után
            if trade.bars_held is not None:
                i = entry_idx + trade.bars_held + 1
            else:
                i = len(candles)   # open_at_end → szimuláció vége

        return report

    # ── Trade szimuláció ───────────────────────────────────────────────────────

    def _simulate_trade(
        self,
        sym:         str,
        mode:        int,
        strategy:    str,
        candles:     list[dict],
        entry_idx:   int,
        entry_price: float,
        tp_price:    float,
        sl_price:    float,
    ) -> TradeResult:
        """Bracket order szimulációja az entry_idx-től a TP/SL eléréséig."""
        for j in range(entry_idx, len(candles)):
            bar_high = candles[j]["high"]
            bar_low  = candles[j]["low"]
            bars_held = j - entry_idx

            # Pesszimista sorrend: ha same-bar SL és TP is elérhető, SL nyer
            # (gap-down / wick-through szituáció)
            sl_hit = bar_low <= sl_price
            tp_hit = bar_high >= tp_price

            if sl_hit:
                exit_p   = sl_price
                pnl      = ((sl_price / entry_price) - 1) - self.commission
                return TradeResult(
                    symbol=sym, mode=mode, strategy=strategy,
                    entry_idx=entry_idx, entry_price=entry_price,
                    tp_price=tp_price, sl_price=sl_price,
                    exit_price=exit_p, exit_type="sl",
                    pnl_pct=round(pnl, 6), bars_held=bars_held,
                )
            if tp_hit:
                exit_p   = tp_price
                pnl      = ((tp_price / entry_price) - 1) - self.commission
                return TradeResult(
                    symbol=sym, mode=mode, strategy=strategy,
                    entry_idx=entry_idx, entry_price=entry_price,
                    tp_price=tp_price, sl_price=sl_price,
                    exit_price=exit_p, exit_type="tp",
                    pnl_pct=round(pnl, 6), bars_held=bars_held,
                )

        # Trade a szimuláció végéig nyitva maradt
        return TradeResult(
            symbol=sym, mode=mode, strategy=strategy,
            entry_idx=entry_idx, entry_price=entry_price,
            tp_price=tp_price, sl_price=sl_price,
            exit_price=None, exit_type="open_at_end",
            pnl_pct=None, bars_held=None,
        )

    # ── Aggregált (mode/stratégia szintű) riport ──────────────────────────────

    @staticmethod
    def aggregate_by_mode(
        reports: dict[str, SymbolReport]
    ) -> dict[str, dict]:
        """Stratégia/mode szerinti összesítő statisztika.

        Returns:
            {"mean_reversion (mode 0)": {...}, "trend_following (mode 1)": {...}, ...}
        """
        mode_map: dict[str, dict] = {}
        for r in reports.values():
            key = f"{r.strategy} (mode {r.mode})"
            if key not in mode_map:
                mode_map[key] = {
                    "symbols": [],
                    "total_trades": 0,
                    "tp_hits": 0,
                    "sl_hits": 0,
                    "open_at_end": 0,
                    "total_pnl_pct": 0.0,
                    "all_pnl": [],
                }
            agg = mode_map[key]
            agg["symbols"].append(r.symbol)
            agg["total_trades"]  += r.total_trades
            agg["tp_hits"]       += r.tp_hits
            agg["sl_hits"]       += r.sl_hits
            agg["open_at_end"]   += r.open_at_end
            agg["total_pnl_pct"] += r.total_pnl_pct
            agg["all_pnl"].extend(
                t.pnl_pct for t in r.trades if t.pnl_pct is not None
            )

        # Számított mezők hozzáadása
        for agg in mode_map.values():
            closed = agg["tp_hits"] + agg["sl_hits"]
            all_p  = agg["all_pnl"]
            agg["win_rate"]      = agg["tp_hits"] / closed if closed > 0 else None
            agg["avg_pnl_pct"]   = sum(all_p) / len(all_p) if all_p else None
            gross_win  = sum(p for p in all_p if p > 0)
            gross_loss = sum(abs(p) for p in all_p if p < 0)
            agg["profit_factor"] = (gross_win / gross_loss) if gross_loss > 0 else None
        return mode_map

    # ── Konzol riport ─────────────────────────────────────────────────────────

    @staticmethod
    def print_report(reports: dict[str, SymbolReport]) -> None:
        """Formázott szöveges riport a konzolra."""
        SEP  = "─" * 78
        SEP2 = "═" * 78

        def _pct(v: Optional[float]) -> str:
            return f"{v * 100:+.2f}%" if v is not None else "     n/a"

        def _f(v: Optional[float], fmt: str = ".3f") -> str:
            return format(v, fmt) if v is not None else "n/a"

        print(f"\n{SEP2}")
        print("  BACKTEST EREDMÉNY — Szimbólum szintű riport")
        print(SEP2)

        for sym, r in sorted(reports.items()):
            print(f"\n  {sym}  │  mode={r.mode}  strategy={r.strategy}")
            print(f"  {SEP}")
            print(f"  Összes trade   : {r.total_trades:>6}")
            print(f"  TP találat     : {r.tp_hits:>6}  "
                  f"({_pct(r.win_rate)} win rate)")
            print(f"  SL találat     : {r.sl_hits:>6}")
            print(f"  Nyitva maradt  : {r.open_at_end:>6}")
            print(f"  Össz. PnL      : {_pct(r.total_pnl_pct)}")
            print(f"  Átlag PnL/trade: {_pct(r.avg_pnl_pct)}")
            print(f"  Max nyereség   : {_pct(r.max_win_pct)}")
            print(f"  Max veszteség  : {_pct(r.max_loss_pct)}")
            print(f"  Profit faktor  : {_f(r.profit_factor)}")
            print(f"  Sharpe (approx): {_f(r.sharpe_approx)}")
            print(f"  Átlag tartás   : {_f(r.avg_bars_held, '.1f')} gyertya")

        print(f"\n{SEP2}")
        print("  BACKTEST EREDMÉNY — Stratégia/mode összesítő")
        print(SEP2)

        agg_by_mode = Backtester.aggregate_by_mode(reports)
        for label, agg in sorted(agg_by_mode.items()):
            closed = agg["tp_hits"] + agg["sl_hits"]
            print(f"\n  {label}")
            print(f"  {SEP}")
            print(f"  Szimbólumok    : {', '.join(agg['symbols'])}")
            print(f"  Összes trade   : {agg['total_trades']}")
            print(f"  TP / SL / nyitva: {agg['tp_hits']} / "
                  f"{agg['sl_hits']} / {agg['open_at_end']}")
            wr = agg["win_rate"]
            print(f"  Win rate       : {_pct(wr)}")
            print(f"  Átlag PnL      : {_pct(agg['avg_pnl_pct'])}")
            print(f"  Profit faktor  : {_f(agg['profit_factor'])}")
        print(f"\n{SEP2}\n")

    # ── CSV mentés ────────────────────────────────────────────────────────────

    @staticmethod
    def save_csv(reports: dict[str, SymbolReport], path: str) -> None:
        """Trade-szintű eredmény CSV-be mentése."""
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        rows: list[dict] = []
        for r in reports.values():
            for t in r.trades:
                rows.append({
                    "symbol":       t.symbol,
                    "mode":         t.mode,
                    "strategy":     t.strategy,
                    "entry_idx":    t.entry_idx,
                    "entry_price":  t.entry_price,
                    "tp_price":     t.tp_price,
                    "sl_price":     t.sl_price,
                    "exit_price":   t.exit_price if t.exit_price is not None else "",
                    "exit_type":    t.exit_type,
                    "pnl_pct":      f"{t.pnl_pct:.6f}" if t.pnl_pct is not None else "",
                    "bars_held":    t.bars_held if t.bars_held is not None else "",
                })
        if not rows:
            print("Nincs mentendő trade adat.")
            return
        with out.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"Backtest CSV mentve: {out}")

    @staticmethod
    def save_summary_csv(reports: dict[str, SymbolReport], path: str) -> None:
        """Szimbólum-szintű összesítő CSV."""
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        for r in reports.values():
            rows.append({
                "symbol":         r.symbol,
                "mode":           r.mode,
                "strategy":       r.strategy,
                "total_trades":   r.total_trades,
                "tp_hits":        r.tp_hits,
                "sl_hits":        r.sl_hits,
                "open_at_end":    r.open_at_end,
                "win_rate":       f"{r.win_rate:.4f}" if r.win_rate is not None else "",
                "avg_pnl_pct":    f"{r.avg_pnl_pct:.6f}" if r.avg_pnl_pct is not None else "",
                "total_pnl_pct":  f"{r.total_pnl_pct:.6f}",
                "max_win_pct":    f"{r.max_win_pct:.6f}",
                "max_loss_pct":   f"{r.max_loss_pct:.6f}",
                "profit_factor":  f"{r.profit_factor:.4f}" if r.profit_factor is not None else "",
                "sharpe_approx":  f"{r.sharpe_approx:.4f}" if r.sharpe_approx is not None else "",
                "avg_bars_held":  f"{r.avg_bars_held:.1f}" if r.avg_bars_held is not None else "",
            })
        if not rows:
            return
        with out.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"Összesítő CSV mentve: {out}")


# ── Önálló CLI futtatás ────────────────────────────────────────────────────────

def _load_candles_from_alpaca(
    cfg: dict,
    api_key: str,
    secret_key: str,
    days: int = 90,
) -> dict[str, list[dict]]:
    """Historikus gyertyák letöltése Alpaca Historical API-ról.

    Csak akkor hívható, ha az alpaca-py telepítve van és API kulcsok elérhetők.
    A main.py warmup logikájával azonos formátumra konvertálja az adatot.
    """
    from datetime import timezone
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame

    client = StockHistoricalDataClient(api_key, secret_key)
    result: dict[str, list[dict]] = {}

    end   = datetime.now(timezone.utc)
    start = end - __import__("datetime").timedelta(days=days)

    for sym, sym_cfg in cfg.items():
        tf = TimeFrame.Hour if sym_cfg.get("mode") == 1 else TimeFrame.Minute
        try:
            req  = StockBarsRequest(
                symbol_or_symbols=sym, timeframe=tf,
                start=start, end=end, limit=10_000,
            )
            bars = client.get_stock_bars(req).data.get(sym, [])
            result[sym] = [
                {
                    "open":   bar.open,
                    "high":   bar.high,
                    "low":    bar.low,
                    "close":  bar.close,
                    "volume": getattr(bar, "volume", 0) or 0,
                    "ts":     bar.timestamp,
                }
                for bar in bars
            ]
            print(f"  {sym}: {len(result[sym])} gyertya betöltve ({tf})")
        except Exception as exc:
            print(f"  {sym}: HIBA — {exc}")
            result[sym] = []

    return result


if __name__ == "__main__":
    import argparse, os, sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))

    from modules.config_loader import load_config

    parser = argparse.ArgumentParser(
        description="Trading bot backtest futtatása historikus adatokon."
    )
    parser.add_argument("--config", default="config/stcks.json",
                        help="Konfiguráció útvonala (default: config/stcks.json)")
    parser.add_argument("--days", type=int, default=90,
                        help="Visszatesztelési időszak napokban (default: 90)")
    parser.add_argument("--out-trades", default="data/backtest_trades.csv",
                        help="Trade-szintű CSV kimenet")
    parser.add_argument("--out-summary", default="data/backtest_summary.csv",
                        help="Összesítő CSV kimenet")
    args = parser.parse_args()

    api_key = os.getenv("ALPACA_API_KEY", "")
    secret  = os.getenv("ALPACA_SECRET_KEY", "")
    if not api_key or not secret:
        print("HIBA: ALPACA_API_KEY és ALPACA_SECRET_KEY környezeti változók szükségesek.")
        sys.exit(1)

    cfg = load_config(args.config)
    print(f"\nKonfiguráció betöltve: {args.config}")
    print(f"Szimbólumok: {', '.join(cfg.keys())}")
    print(f"Adatletöltés: utolsó {args.days} nap\n")

    candles_by_symbol = _load_candles_from_alpaca(cfg, api_key, secret, args.days)

    bt = Backtester(cfg)
    reports = bt.run(candles_by_symbol)

    bt.print_report(reports)
    bt.save_csv(reports, args.out_trades)
    bt.save_summary_csv(reports, args.out_summary)
