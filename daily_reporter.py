from __future__ import annotations

"""
daily_reporter.py — Bővített napi CSV riport
=============================================

[#3] Az eredeti háromoszlopos riport (dátum, nyitó equity, záró equity)
kiegészítve a következő mezőkkel:

  daily_pnl          — napi PnL dollárban (záró - nyitó equity)
  daily_pnl_pct      — napi PnL százalékban
  trade_count        — aznap benyújtott entry orderek száma
  win_count          — TP-en zárt trade-ek száma
  loss_count         — SL-en zárt trade-ek száma
  net_cash_flow      — napi nettó pénzmozgás (sell_fills − buy_fills);
                       nyitott pozíciók entry fill-je is benne van
  realized_pnl       — csak lezárt round-trip alapján számolt PnL ($)
  unrealized_pnl     — nyitott pozíciók nem-realizált PnL-je a záráskor ($)
  max_drawdown_pct   — nap közbeni maximális drawdown az induló equity-hez
                       képest, százalékban (intrabar szinten nem mérhető,
                       de equity_low-ból közelíthető ha megadják)
  circuit_breaker    — 1 ha a nap folyamán circuit breaker aktiválódott
  rejected_orders    — elutasított orderek száma
  canceled_orders    — törölt orderek száma
  open_positions     — nyitott pozíciók száma a nap végén

Visszafelé-kompatibilitás:
  A CSV fejléce változott, ezért az első futáskor a meglévő
  daily_summary.csv-t archiválja (.bak kiterjesztéssel), és új,
  kibővített fejlécű fájlt nyit. Ez megakadályozza az oszlopcsúszást.
"""

import csv
import shutil
from datetime import date, datetime, timezone
from pathlib import Path


class DailyReporter:
    """Napi összesítő CSV riporter — bővített mezőkkel."""

    FIELDNAMES = [
        "datum",
        "nyito_equity",
        "zaro_equity",
        "daily_pnl",
        "daily_pnl_pct",
        "trade_count",
        "win_count",
        "loss_count",
        "net_cash_flow",
        "realized_pnl",
        "unrealized_pnl",
        "max_drawdown_pct",
        "circuit_breaker",
        "rejected_orders",
        "canceled_orders",
        "open_positions",
    ]

    def __init__(self, report_dir: str = "data", filename: str = "daily_summary.csv"):
        self._path = Path(report_dir) / filename
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self) -> None:
        """Fejléc ellenőrzés — ha a meglévő fájl régi formátumú, archiválja."""
        if not self._path.exists():
            self._write_header()
            return

        # Ellenőrizzük, hogy a jelenlegi fejléc egyezik-e az elvárttal
        with self._path.open("r", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter=";")
            try:
                existing_header = next(reader)
            except StopIteration:
                existing_header = []

        if existing_header != self.FIELDNAMES:
            # Régi formátumú fájl → archiválás
            bak_path = self._path.with_suffix(".bak")
            shutil.copy2(self._path, bak_path)
            self._write_header()

    def _write_header(self) -> None:
        with self._path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, delimiter=";")
            writer.writerow(self.FIELDNAMES)

    def write_day(
        self,
        *,
        trade_date: date | str | None = None,
        open_equity: float = 0.0,
        close_equity: float = 0.0,
        net_cash_flow: float = 0.0,
        realized_pnl: float = 0.0,
        unrealized_pnl: float = 0.0,
        trade_count: int = 0,
        win_count: int = 0,
        loss_count: int = 0,
        max_drawdown_pct: float = 0.0,
        circuit_breaker: bool = False,
        rejected_orders: int = 0,
        canceled_orders: int = 0,
        open_positions: int = 0,
    ) -> None:
        """Egyetlen napi sort ír a CSV-be.

        Minden paraméternek van alapértéke, így a hívó csak a ténylegesen
        ismert értékeket kell átadja — a többi nullként kerül a riportba.
        """
        if trade_date is None:
            trade_date = datetime.now(timezone.utc).date()
        date_str = str(trade_date)

        daily_pnl = close_equity - open_equity
        daily_pnl_pct = (
            round((daily_pnl / open_equity) * 100, 4)
            if open_equity and open_equity != 0
            else 0.0
        )

        with self._path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, delimiter=";")
            writer.writerow([
                date_str,
                f"{open_equity:.2f}",
                f"{close_equity:.2f}",
                f"{daily_pnl:.2f}",
                f"{daily_pnl_pct:.4f}",
                trade_count,
                win_count,
                loss_count,
                f"{net_cash_flow:.4f}",
                f"{realized_pnl:.4f}",
                f"{unrealized_pnl:.4f}",
                f"{max_drawdown_pct:.4f}",
                1 if circuit_breaker else 0,
                rejected_orders,
                canceled_orders,
                open_positions,
            ])

    @property
    def path(self) -> Path:
        return self._path
