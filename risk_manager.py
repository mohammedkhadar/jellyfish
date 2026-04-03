"""
Crypto Risk Manager
===================
Crypto-specific risk controls:
  - Volatility-scaled position sizing (crypto ATR is 3-5x equities)
  - BTC correlation cap (most alts are 0.7+ correlated to BTC)
  - Trailing stops (crypto trends can run far)
  - 24/7 gap risk handling (no session boundaries)
  - Circuit breaker with higher threshold (8-10% for crypto)
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime
from config import Config

logger = logging.getLogger("risk")


class CryptoRiskManager:
    def __init__(self, config: Config):
        self.config = config
        self._daily_pnl = 0.0
        self._daily_reset_hour = datetime.utcnow().hour
        self._circuit_breaker_active = False
        self._open_positions: Dict[str, Dict] = {}

    def filter_signals(self, signals: List[Dict]) -> List[Dict]:
        if self._circuit_breaker_active:
            logger.warning("CIRCUIT BREAKER ACTIVE")
            return []

        approved = []
        for signal in signals:
            sized = self._apply_risk_rules(signal)
            if sized:
                approved.append(sized)
        return approved

    def _apply_risk_rules(self, signal: Dict) -> Optional[Dict]:
        account = self._get_account_info()
        portfolio_value = account["total_equity"]
        token = signal["token"]
        pair = signal["pair"]

        # ─── Portfolio exposure check ──────────────────────────
        current_exposure = sum(
            abs(p.get("notional", 0)) for p in self._open_positions.values()
        )
        max_exposure = portfolio_value * self.config.MAX_PORTFOLIO_EXPOSURE_PCT
        if current_exposure >= max_exposure:
            logger.warning(f"Portfolio exposure limit reached")
            return None

        # ─── Single token exposure check ───────────────────────
        token_exposure = abs(
            self._open_positions.get(token, {}).get("notional", 0)
        )
        max_token = portfolio_value * self.config.MAX_SINGLE_TOKEN_PCT
        if token_exposure >= max_token:
            logger.warning(f"Token exposure limit for {token} reached")
            return None

        # ─── BTC correlation check ─────────────────────────────
        # Most altcoins are highly correlated to BTC
        # Limit total BTC-correlated exposure
        btc_correlated = self._btc_correlated_exposure()
        max_btc_corr = portfolio_value * self.config.MAX_BTC_CORRELATED_PCT
        if btc_correlated >= max_btc_corr and token != "BTC":
            logger.warning(
                f"BTC-correlated exposure limit "
                f"({btc_correlated:.0f} >= {max_btc_corr:.0f})"
            )
            return None

        # ─── Position sizing ───────────────────────────────────
        price = self._get_price(token)
        atr = self._get_atr(token)

        if price <= 0 or atr <= 0:
            logger.error(f"Bad price/ATR for {token}")
            return None

        risk_amount = portfolio_value * self.config.MAX_RISK_PER_TRADE_PCT
        stop_distance = atr * self.config.ATR_STOP_MULTIPLIER

        # For crypto, size in base currency (BTC, ETH, etc.)
        position_size = risk_amount / stop_distance
        notional_value = position_size * price

        # Cap to remaining room
        remaining = min(
            max_exposure - current_exposure,
            max_token - token_exposure,
        )
        if notional_value > remaining:
            position_size = remaining / price
            notional_value = position_size * price

        if position_size * price < 10:  # Minimum $10 order
            logger.warning(f"Position too small for {token}")
            return None

        # ─── Stop loss & take profit ───────────────────────────
        if signal["direction"] == "long":
            stop_loss = price - stop_distance
            take_profit_1 = price + (stop_distance * 2.0)  # 2:1 R:R
            take_profit_2 = price + (stop_distance * 3.5)  # 3.5:1 R:R
            trailing_stop = price - (
                atr * self.config.TRAILING_STOP_ATR_MULT
            )
        else:
            stop_loss = price + stop_distance
            take_profit_1 = price - (stop_distance * 2.0)
            take_profit_2 = price - (stop_distance * 3.5)
            trailing_stop = price + (
                atr * self.config.TRAILING_STOP_ATR_MULT
            )

        signal.update({
            "position_size": round(position_size, 6),
            "notional_value": round(notional_value, 2),
            "entry_price": price,
            "stop_loss": round(stop_loss, 2),
            "take_profit_1": round(take_profit_1, 2),
            "take_profit_2": round(take_profit_2, 2),
            "trailing_stop": round(trailing_stop, 2),
            "atr": atr,
            "stop_distance": round(stop_distance, 2),
            "risk_amount": round(risk_amount, 2),
        })

        logger.info(
            f"  Risk OK: {token} {signal['direction']} "
            f"{position_size:.4f} @ {price:.2f}, "
            f"SL={stop_loss:.2f}, TP1={take_profit_1:.2f}, "
            f"notional=${notional_value:.0f}"
        )
        return signal

    def _btc_correlated_exposure(self) -> float:
        """
        Estimate total BTC-correlated exposure.

        Most alts have 0.6-0.9 correlation to BTC.
        This is a simplified model — production would use
        rolling correlation calculations.
        """
        BTC_CORRELATION = {
            "BTC": 1.0,
            "ETH": 0.85,
            "SOL": 0.80,
            "AVAX": 0.75,
            "LINK": 0.70,
            "DOGE": 0.65,
        }

        total = 0.0
        for token, pos in self._open_positions.items():
            corr = BTC_CORRELATION.get(token, 0.7)
            total += abs(pos.get("notional", 0)) * corr
        return total

    def update_pnl(self, pnl_change: float):
        self._daily_pnl += pnl_change
        account = self._get_account_info()
        max_dd = account["total_equity"] * self.config.MAX_DAILY_DRAWDOWN_PCT

        if self._daily_pnl < -max_dd:
            self._circuit_breaker_active = True
            logger.critical(
                f"CIRCUIT BREAKER: Daily P&L {self._daily_pnl:.2f} "
                f"exceeded max {-max_dd:.2f}"
            )

    def _get_account_info(self) -> Dict:
        """Placeholder — replace with exchange API call."""
        return {"total_equity": 50_000.0, "available_balance": 50_000.0}

    def _get_price(self, token: str) -> float:
        """Placeholder prices."""
        prices = {"BTC": 95000.0, "ETH": 3500.0, "SOL": 180.0}
        return prices.get(token, 100.0)

    def _get_atr(self, token: str) -> float:
        """Placeholder ATR values (daily)."""
        atrs = {"BTC": 2800.0, "ETH": 120.0, "SOL": 8.0}
        return atrs.get(token, 5.0)
