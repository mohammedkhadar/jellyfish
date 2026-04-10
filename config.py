"""
Configuration for the crypto sentiment trading bot.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Config:
    mode: str = "paper"

    # ─── Tokens to track ───────────────────────────────────────
    TOKENS: List[str] = field(
        default_factory=lambda: ["BTC", "ETH", "SOL"]
    )

    TOKEN_CONFIG: Dict[str, dict] = field(
        default_factory=lambda: {
            "BTC": {
                "pair": "BTC/USDT",
                "coingecko_id": "bitcoin",
                "keywords": [
                    "bitcoin", "BTC", "satoshi", "lightning network",
                    "halving", "hash rate", "mining", "digital gold",
                ],
                "min_market_cap_rank": 1,
            },
            "ETH": {
                "pair": "ETH/USDT",
                "coingecko_id": "ethereum",
                "keywords": [
                    "ethereum", "ETH", "vitalik", "gas fees",
                    "layer 2", "staking", "EIP", "merge",
                    "defi", "smart contract",
                ],
                "min_market_cap_rank": 2,
            },
            "SOL": {
                "pair": "SOL/USDT",
                "coingecko_id": "solana",
                "keywords": [
                    "solana", "SOL", "phantom", "jupiter",
                    "solana defi", "solana nft", "firedancer",
                ],
                "min_market_cap_rank": 5,
            },
        }
    )

    # ─── Data Sources ──────────────────────────────────────────
    # CoinGecko (https://www.coingecko.com/en/api — free tier, no key needed)
    COINGECKO_API_URL: str = "https://api.coingecko.com/api/v3"

    # RSS news feeds (free, no key needed)
    RSS_FEEDS: List[str] = field(
        default_factory=lambda: [
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "https://cointelegraph.com/rss",
            "https://decrypt.co/feed",
        ]
    )

    # Alternative.me Fear & Greed (free, no key needed)
    FEAR_GREED_URL: str = "https://api.alternative.me/fng/"

    # Blockchain explorers / on-chain
    # Whale Alert API (https://whale-alert.io — free tier: 10 req/min)
    WHALE_ALERT_KEY: str = os.getenv("WHALE_ALERT_KEY", "")
    WHALE_ALERT_URL: str = "https://api.whale-alert.io/v1/transactions"
    WHALE_MIN_USD: int = 1_000_000  # Track txns > $1M

    # Glassnode or CryptoQuant for on-chain metrics (paid)
    # For starter, we use free CoinGecko data + Whale Alert
    GLASSNODE_KEY: str = os.getenv("GLASSNODE_KEY", "")

    # News
    NEWS_API_KEY: str = os.getenv("NEWS_API_KEY", "")
    NEWS_API_URL: str = "https://newsapi.org/v2/everything"

    # ─── Sentiment Engine ──────────────────────────────────────
    FINBERT_MODEL: str = "ProsusAI/finbert"
    SENTIMENT_BATCH_SIZE: int = 32

    # Crypto-specific lexicon
    # (score_adjustment, applies_to_token_or_"all")
    CRYPTO_LEXICON: Dict[str, tuple] = field(
        default_factory=lambda: {
            # Bullish signals
            "moon": (0.4, "all"),
            "bullish": (0.5, "all"),
            "pump": (0.3, "all"),
            "breakout": (0.4, "all"),
            "accumulation": (0.5, "all"),
            "institutional adoption": (0.6, "all"),
            "ETF approval": (0.7, "all"),
            "ETF inflow": (0.6, "all"),
            "halving": (0.4, "BTC"),
            "hash rate ATH": (0.3, "BTC"),
            "staking rewards": (0.3, "ETH"),
            "TVL increase": (0.4, "all"),
            "airdrop": (0.3, "all"),
            "partnership": (0.3, "all"),
            "upgrade": (0.4, "all"),

            # Bearish signals
            "bearish": (-0.5, "all"),
            "dump": (-0.4, "all"),
            "crash": (-0.5, "all"),
            "rug pull": (-0.8, "all"),
            "exploit": (-0.7, "all"),
            "hack": (-0.8, "all"),
            "SEC lawsuit": (-0.6, "all"),
            "regulation": (-0.3, "all"),
            "ban": (-0.6, "all"),
            "FUD": (-0.2, "all"),        # Meta: people calling it FUD
            "liquidation cascade": (-0.7, "all"),
            "death cross": (-0.4, "all"),
            "ETF outflow": (-0.5, "all"),
            "depeg": (-0.8, "all"),
            "insolvency": (-0.9, "all"),
            "ponzi": (-0.7, "all"),

            # Neutral but important (modify confidence, not direction)
            "whale": (0.0, "all"),       # Tracked separately via on-chain
            "transfer": (0.0, "all"),
            "fork": (0.0, "all"),
        }
    )

    # ─── Feature Engineering ───────────────────────────────────
    ROLLING_WINDOW_HOURS: int = 12      # Crypto moves faster
    MOMENTUM_WINDOW_HOURS: int = 2
    Z_SCORE_LOOKBACK_DAYS: int = 30
    SENTIMENT_DECAY_HALF_LIFE_HOURS: float = 3.0  # Faster decay for crypto

    # Source weights
    SOURCE_WEIGHTS: Dict[str, float] = field(
        default_factory=lambda: {
            "coingecko": 0.7,           # Community sentiment + market data
            "coingecko_trending": 0.5,  # Trending signal
            "rss_news": 0.6,            # RSS news headlines sentiment
            "news": 0.8,
            "whale_alert": 1.0,         # On-chain data is high signal
            "exchange_data": 0.9,
            "fear_greed": 0.7,
        }
    )

    # ─── On-chain thresholds ───────────────────────────────────
    # Exchange inflow/outflow scoring
    EXCHANGE_FLOW_BULLISH_THRESHOLD: float = -0.05  # Net outflow > 5% = bullish
    EXCHANGE_FLOW_BEARISH_THRESHOLD: float = 0.05   # Net inflow > 5% = bearish

    # Funding rate thresholds
    FUNDING_RATE_EXTREME_LONG: float = 0.01   # 1% = overleveraged longs
    FUNDING_RATE_EXTREME_SHORT: float = -0.01  # -1% = overleveraged shorts

    # ─── Signal Generation ─────────────────────────────────────
    Z_SCORE_BUY_THRESHOLD: float = 1.5
    Z_SCORE_SELL_THRESHOLD: float = -1.5
    MIN_CONFIDENCE: float = 0.55   # Slightly lower — crypto is noisier

    # Fear & Greed regime filter
    # Avoid buying when greed is extreme, avoid shorting when fear is extreme
    FEAR_GREED_EXTREME_GREED: int = 80
    FEAR_GREED_EXTREME_FEAR: int = 20

    # ─── Risk Management ───────────────────────────────────────
    MAX_RISK_PER_TRADE_PCT: float = 0.015  # 1.5% — crypto is more volatile
    MAX_PORTFOLIO_EXPOSURE_PCT: float = 0.6
    MAX_SINGLE_TOKEN_PCT: float = 0.25     # Max 25% in one token
    MAX_BTC_CORRELATED_PCT: float = 0.5    # Max 50% in BTC-correlated positions
    MAX_DAILY_DRAWDOWN_PCT: float = 0.08   # 8% daily drawdown → circuit breaker
    ATR_STOP_MULTIPLIER: float = 2.5       # Wider stops for crypto volatility
    ATR_PERIOD: int = 14
    USE_TRAILING_STOPS: bool = True
    TRAILING_STOP_ATR_MULT: float = 3.0

    # ─── Execution ─────────────────────────────────────────────
    # Exchange via ccxt
    EXCHANGE_ID: str = "binance"           # binance, bybit, okx
    EXCHANGE_API_KEY: str = os.getenv("EXCHANGE_API_KEY", "")
    EXCHANGE_SECRET: str = os.getenv("EXCHANGE_SECRET", "")
    EXCHANGE_TESTNET: bool = True          # Use testnet for paper trading

    ORDER_TYPE: str = "limit"              # limit or market
    LIMIT_OFFSET_PCT: float = 0.001        # 0.1% from mid
    USE_TWAP: bool = True                  # Split large orders over time
    TWAP_SLICES: int = 3
    TWAP_INTERVAL_SECONDS: int = 30

    # ─── Scheduling ────────────────────────────────────────────
    PIPELINE_INTERVAL_MINUTES: int = 5     # Faster than commodities

    # ─── Storage ───────────────────────────────────────────────
    DB_PATH: str = "crypto_sentiment.db"
