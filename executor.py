"""
Crypto Order Executor
=====================
Executes trades via ccxt (supports 100+ exchanges).

Features:
  - TWAP order splitting (break large orders into slices)
  - Limit orders with configurable offset
  - Testnet/paper trading support
  - Trailing stop management
  - Multi-exchange support (Binance, Bybit, OKX)
"""

import logging
import time
from typing import Dict, Optional, List
from datetime import datetime
from config import Config

logger = logging.getLogger("executor")


class CryptoExecutor:
    def __init__(self, config: Config):
        self.config = config
        self.exchange = None

        if config.mode != "backtest":
            self._init_exchange()

    def _init_exchange(self):
        """Initialize exchange connection via ccxt."""
        try:
            import ccxt

            exchange_class = getattr(ccxt, self.config.EXCHANGE_ID)
            params = {
                "apiKey": self.config.EXCHANGE_API_KEY,
                "secret": self.config.EXCHANGE_SECRET,
                "options": {"defaultType": "spot"},
            }

            # Enable testnet for paper trading
            if self.config.EXCHANGE_TESTNET:
                params["options"]["testnet"] = True
                if self.config.EXCHANGE_ID == "binance":
                    params["options"]["sandboxMode"] = True

            self.exchange = exchange_class(params)

            # Verify connection
            balance = self.exchange.fetch_balance()
            usdt = balance.get("USDT", {}).get("free", 0)
            logger.info(
                f"Exchange connected: {self.config.EXCHANGE_ID} "
                f"({'testnet' if self.config.EXCHANGE_TESTNET else 'live'}) "
                f"USDT balance: {usdt:.2f}"
            )

        except ImportError:
            logger.warning("ccxt not installed — dry-run mode")
        except Exception as e:
            logger.error(f"Exchange connection failed: {e}")

    def execute(self, signal: Dict) -> Optional[Dict]:
        """Execute a trading signal, optionally with TWAP."""
        token = signal["token"]
        pair = signal["pair"]
        direction = signal["direction"]
        size = signal["position_size"]

        logger.info(
            f"Preparing: {direction.upper()} {size:.6f} {pair}"
        )

        if self.exchange is None:
            return self._dry_run(signal)

        # Use TWAP for larger orders
        if self.config.USE_TWAP and size * signal["entry_price"] > 5000:
            return self._execute_twap(signal)

        return self._execute_single(signal)

    def _execute_single(self, signal: Dict) -> Optional[Dict]:
        """Place a single order."""
        pair = signal["pair"]
        side = "buy" if signal["direction"] == "long" else "sell"
        size = signal["position_size"]

        try:
            if self.config.ORDER_TYPE == "limit":
                # Fetch current price for limit offset
                ticker = self.exchange.fetch_ticker(pair)
                mid_price = (ticker["bid"] + ticker["ask"]) / 2
                offset = mid_price * self.config.LIMIT_OFFSET_PCT

                if side == "buy":
                    limit_price = mid_price + offset
                else:
                    limit_price = mid_price - offset

                order = self.exchange.create_order(
                    symbol=pair,
                    type="limit",
                    side=side,
                    amount=size,
                    price=limit_price,
                )
            else:
                order = self.exchange.create_order(
                    symbol=pair,
                    type="market",
                    side=side,
                    amount=size,
                )

            # Set stop loss as a separate order
            try:
                sl_side = "sell" if side == "buy" else "buy"
                self.exchange.create_order(
                    symbol=pair,
                    type="stop_market",
                    side=sl_side,
                    amount=size,
                    params={"stopPrice": signal["stop_loss"]},
                )
                logger.info(f"  Stop loss set at {signal['stop_loss']}")
            except Exception as e:
                logger.warning(f"  Stop loss order failed: {e}")

            # Set take profit
            try:
                tp_side = "sell" if side == "buy" else "buy"
                # Take partial profit at TP1 (60% of position)
                tp1_size = round(size * 0.6, 6)
                self.exchange.create_order(
                    symbol=pair,
                    type="limit",
                    side=tp_side,
                    amount=tp1_size,
                    price=signal["take_profit_1"],
                    params={"reduceOnly": True},
                )
                logger.info(
                    f"  TP1 set: {tp1_size} @ {signal['take_profit_1']}"
                )
            except Exception as e:
                logger.warning(f"  Take profit order failed: {e}")

            order_info = {
                "order_id": order.get("id"),
                "pair": pair,
                "side": side,
                "amount": size,
                "price": order.get("price") or order.get("average"),
                "status": order.get("status"),
                "stop_loss": signal["stop_loss"],
                "take_profit_1": signal["take_profit_1"],
                "submitted_at": datetime.utcnow().isoformat(),
            }

            logger.info(f"  Order placed: {order_info['order_id']}")
            return order_info

        except Exception as e:
            logger.error(f"  Order failed: {e}")
            return None

    def _execute_twap(self, signal: Dict) -> Optional[Dict]:
        """
        Time-Weighted Average Price execution.

        Splits a large order into smaller slices executed
        at intervals to reduce market impact.
        """
        pair = signal["pair"]
        side = "buy" if signal["direction"] == "long" else "sell"
        total_size = signal["position_size"]
        slices = self.config.TWAP_SLICES
        interval = self.config.TWAP_INTERVAL_SECONDS
        slice_size = round(total_size / slices, 6)

        logger.info(
            f"  TWAP: {slices} slices of {slice_size} "
            f"every {interval}s"
        )

        filled_orders = []
        total_filled = 0.0

        for i in range(slices):
            try:
                remaining = round(total_size - total_filled, 6)
                current_slice = min(slice_size, remaining)

                if current_slice <= 0:
                    break

                ticker = self.exchange.fetch_ticker(pair)
                mid = (ticker["bid"] + ticker["ask"]) / 2
                offset = mid * self.config.LIMIT_OFFSET_PCT
                price = mid + offset if side == "buy" else mid - offset

                order = self.exchange.create_order(
                    symbol=pair,
                    type="limit",
                    side=side,
                    amount=current_slice,
                    price=price,
                )

                filled_orders.append(order)
                total_filled += current_slice

                logger.info(
                    f"    Slice {i+1}/{slices}: "
                    f"{current_slice} @ {price:.2f}"
                )

                if i < slices - 1:
                    time.sleep(interval)

            except Exception as e:
                logger.error(f"    Slice {i+1} failed: {e}")

        if not filled_orders:
            return None

        # Calculate average fill
        avg_price = sum(
            (o.get("price") or o.get("average", 0)) or 0
            for o in filled_orders
        ) / len(filled_orders)

        return {
            "order_type": "TWAP",
            "pair": pair,
            "side": side,
            "total_amount": total_filled,
            "slices_filled": len(filled_orders),
            "avg_price": round(avg_price, 2),
            "stop_loss": signal["stop_loss"],
            "take_profit_1": signal["take_profit_1"],
            "submitted_at": datetime.utcnow().isoformat(),
        }

    def _dry_run(self, signal: Dict) -> Dict:
        """Simulate order without exchange."""
        order = {
            "order_id": f"DRY-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            "pair": signal["pair"],
            "side": "buy" if signal["direction"] == "long" else "sell",
            "amount": signal["position_size"],
            "entry_price": signal["entry_price"],
            "stop_loss": signal["stop_loss"],
            "take_profit_1": signal["take_profit_1"],
            "trailing_stop": signal.get("trailing_stop"),
            "notional": signal["notional_value"],
            "status": "DRY_RUN",
            "submitted_at": datetime.utcnow().isoformat(),
        }

        logger.info(
            f"  [DRY RUN] {order['side'].upper()} "
            f"{order['amount']:.6f} {order['pair']} "
            f"@ {order['entry_price']:.2f} | "
            f"SL={order['stop_loss']:.2f} | "
            f"TP1={order['take_profit_1']:.2f} | "
            f"${order['notional']:.0f}"
        )
        return order

    def get_open_orders(self, pair: str = None) -> List:
        """Fetch open orders from exchange."""
        if self.exchange is None:
            return []
        try:
            return self.exchange.fetch_open_orders(pair)
        except Exception as e:
            logger.error(f"Failed to fetch open orders: {e}")
            return []

    def cancel_all_orders(self, pair: str) -> bool:
        """Cancel all open orders for a pair."""
        if self.exchange is None:
            return True
        try:
            self.exchange.cancel_all_orders(pair)
            logger.info(f"Cancelled all orders for {pair}")
            return True
        except Exception as e:
            logger.error(f"Cancel failed for {pair}: {e}")
            return False
