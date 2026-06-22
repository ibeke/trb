from __future__ import annotations

import json
from pathlib import Path


def load_config(path: str = "config/stcks.json") -> dict:
    """stcks.json betöltése; alapértelmezett értékek kitöltése."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Konfiguráció nem található: {path}")
    with p.open(encoding="utf-8") as fh:
        cfg: dict = json.load(fh)

    # Alapértelmezések feltöltése, ha hiányoznának
    defaults = {
        "mode": 0,
        "strategy": "mean_reversion",
        "bb_period": 20,
        "bb_std": 2.0,
        "atr_period": 14,
        "stop_loss_pct": 0.02,
        "take_profit_pct": 0.04,
        "max_risk_per_trade": 0.01,
        "limit_offset_pct": 0.001,
        "allocation_usd": 5000,
        # mode 1 — trend following
        "ema_fast": 20,
        "ema_slow": 50,
        "adx_period": 14,
        "adx_threshold": 25,
        # mode 2 — breakout
        "breakout_period": 20,
        "volume_ma_period": 20,
        "volume_factor": 1.5,
    }
    for sym, sym_cfg in cfg.items():
        for k, v in defaults.items():
            sym_cfg.setdefault(k, v)
    return cfg


def get_symbols(cfg: dict) -> list[str]:
    return list(cfg.keys())
