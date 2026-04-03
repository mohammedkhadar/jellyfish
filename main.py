"""
Crypto Sentiment Trading Bot — Starter Pipeline
=================================================
Focuses on BTC, ETH, SOL with configurable altcoin expansion.

Key differences from commodity trading:
  - 24/7 markets (no session boundaries)
  - Social media is the PRIMARY sentiment driver
  - On-chain data (whale flows, funding rates) as unique signals
  - Higher volatility → wider stops, smaller positions
  - Exchange APIs via ccxt (not broker APIs)

Architecture:
  1. Data ingestion (social, on-chain, news, exchange metrics)
  2. Sentiment scoring (FinBERT + crypto lexicon + fear/greed)
  3. Feature engineering (social velocity, whale delta, funding skew)
  4. Signal generation (multi-factor + regime filter)
  5. Risk management (volatility-scaled, BTC correlation cap)
  6. Execution (ccxt → Binance/Bybit/OKX)

Usage:
  pip install -r requirements.txt
  python main.py --mode paper --once
  python main.py --mode paper        # runs every 5 min
"""

import argparse
import logging
from datetime import datetime

from config import Config
from data_ingestion import CryptoDataPipeline
from sentiment_engine import CryptoSentimentEngine
from feature_store import CryptoFeatureStore
from signal_generator import CryptoSignalGenerator
from risk_manager import CryptoRiskManager
from executor import CryptoExecutor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("crypto_bot.log"),
    ],
)
logger = logging.getLogger("main")


def run_pipeline(config: Config):
    """Single pipeline iteration."""

    logger.info("=" * 60)
    logger.info(f"Pipeline run at {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

    # 1. Ingest fresh data
    pipeline = CryptoDataPipeline(config)
    raw_data = pipeline.fetch_all()
    logger.info(
        f"Ingested {len(raw_data['social'])} social items, "
        f"{len(raw_data['news'])} news items, "
        f"{len(raw_data['onchain'])} on-chain data points, "
        f"{len(raw_data['exchange'])} exchange metrics"
    )

    all_text_items = raw_data["social"] + raw_data["news"]
    if not all_text_items and not raw_data["onchain"]:
        logger.info("No new data — skipping this cycle")
        return

    # 2. Score sentiment
    engine = CryptoSentimentEngine(config)
    scored_text = engine.score_batch(all_text_items) if all_text_items else []
    scored_onchain = engine.score_onchain(raw_data["onchain"])
    scored_exchange = engine.score_exchange(raw_data["exchange"])
    logger.info(
        f"Scored {len(scored_text)} text items, "
        f"{len(scored_onchain)} on-chain signals, "
        f"{len(scored_exchange)} exchange signals"
    )

    # 3. Build features
    store = CryptoFeatureStore(config)
    store.update(scored_text, scored_onchain, scored_exchange)
    features = store.get_latest_features()

    for token, feat in features.items():
        logger.info(
            f"  {token}: composite={feat['composite_score']:.3f}, "
            f"social_velocity={feat['social_velocity']:.3f}, "
            f"whale_delta={feat['whale_delta']:.3f}, "
            f"z_score={feat['z_score']:.3f}"
        )

    # 4. Generate signals
    signal_gen = CryptoSignalGenerator(config)
    signals = signal_gen.evaluate(features)

    for sig in signals:
        logger.info(
            f"  SIGNAL: {sig['token']} → {sig['direction']} "
            f"(confidence={sig['confidence']:.2f})"
        )

    if not signals:
        logger.info("No actionable signals this cycle")
        return

    # 5. Risk check
    risk_mgr = CryptoRiskManager(config)
    approved = risk_mgr.filter_signals(signals)

    if not approved:
        logger.warning("All signals rejected by risk manager")
        return

    # 6. Execute
    executor = CryptoExecutor(config)
    for signal in approved:
        order = executor.execute(signal)
        if order:
            logger.info(f"  ORDER: {order}")


def main():
    parser = argparse.ArgumentParser(description="Crypto Sentiment Trading Bot")
    parser.add_argument(
        "--mode", choices=["paper", "live", "backtest"],
        default="paper", help="Trading mode",
    )
    parser.add_argument(
        "--once", action="store_true", help="Run once instead of on schedule",
    )
    args = parser.parse_args()

    config = Config(mode=args.mode)
    logger.info(f"Starting crypto bot in {args.mode} mode")
    logger.info(f"Tokens: {config.TOKENS}")

    if args.once:
        run_pipeline(config)
    else:
        import schedule
        import time

        schedule.every(config.PIPELINE_INTERVAL_MINUTES).minutes.do(
            run_pipeline, config
        )
        logger.info(f"Scheduled every {config.PIPELINE_INTERVAL_MINUTES} min")
        run_pipeline(config)

        while True:
            schedule.run_pending()
            time.sleep(15)


if __name__ == "__main__":
    main()
