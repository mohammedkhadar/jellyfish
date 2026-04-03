# Crypto Sentiment Trading Bot

A sentiment analysis trading bot for **BTC, ETH, and SOL** using social media NLP, on-chain whale tracking, exchange metrics (funding rates), and Fear & Greed regime filtering.

## How it differs from the commodity bot

| Aspect | Commodities | Crypto |
|--------|------------|--------|
| Primary sentiment source | News + gov reports | Social media + on-chain |
| Market hours | Session-based | 24/7 |
| Volatility | Moderate | High (3-5x wider stops) |
| Unique signals | EIA inventories, weather | Whale flows, funding rates, Fear & Greed |
| Execution | Broker API (Alpaca/IBKR) | Exchange API via ccxt |
| Pipeline frequency | Every 15 min | Every 5 min |
| Correlation risk | Sector-based | BTC beta cap |

## Architecture

```
Social (Reddit, X) ──┐
On-chain (Whale Alert) ─┤
News (CoinDesk, NewsAPI) ┼→ Stream → Sentiment Engine → Features → Signals → Risk → Exchange
Exchange (Funding, OI) ──┤                ↑                 ↑
Fear & Greed ────────────┘           FinBERT +         Social velocity
                                   Crypto lexicon     Whale delta
                                   Sarcasm filter     Funding skew
                                                      Regime filter
```

## Modules

| Module | Purpose |
|--------|---------|
| `main.py` | Pipeline orchestrator |
| `config.py` | All config, API keys, thresholds, crypto lexicon |
| `data_ingestion.py` | Reddit, news, Whale Alert, exchange metrics, Fear & Greed |
| `sentiment_engine.py` | FinBERT + crypto lexicon + sarcasm discount + on-chain scoring |
| `feature_store.py` | Social velocity, whale delta, funding skew, z-scores |
| `signal_generator.py` | Multi-factor signals with Fear & Greed regime filter |
| `risk_manager.py` | Volatility-scaled sizing, BTC correlation cap, circuit breaker |
| `executor.py` | ccxt execution with TWAP splitting, stop/take-profit management |

## Quick Start

```bash
# 1. Install
pip install -r requirements.txt

# 2. Set API keys
export NEWS_API_KEY="..."              # newsapi.org (free)
export WHALE_ALERT_KEY="..."           # whale-alert.io (free tier)
export EXCHANGE_API_KEY="..."          # Binance testnet
export EXCHANGE_SECRET="..."

# 3. Paper trade (single pass)
python main.py --mode paper --once

# 4. Scheduled (every 5 min)
python main.py --mode paper
```

## Crypto-Specific Features

### Sarcasm Discount
Crypto communities are heavily sarcastic. The engine detects markers like "this time is different", "trust me bro", "hopium", "copium" and discounts sentiment by 25-50% when detected.

### Fear & Greed Regime Filter
Prevents buying when the market is in extreme greed (>80) and shorting in extreme fear (<20). This is a powerful contrarian filter — most retail traders do the opposite.

### Whale Flow Tracking
Large exchange inflows (deposits for selling) → bearish signal. Large outflows (withdrawals for holding) → bullish. Scored by transaction size ($1M+ tracked).

### Funding Rate Contrarian
When perpetual futures funding rates are extremely positive (crowded longs), the bot treats long signals with skepticism and boosts short signal confidence. Vice versa for negative funding.

### BTC Correlation Cap
Most altcoins are 0.7-0.9 correlated with BTC. The risk manager limits total BTC-correlated exposure to prevent hidden concentration risk.

### TWAP Execution
Orders above $5,000 notional are split into 3 slices executed 30 seconds apart to reduce market impact.

## What to Build Next

1. **Twitter/X integration** — Crypto Twitter is the #1 sentiment source. Add via X API v2 with filtered stream for real-time data.

2. **Telegram group monitoring** — Many alpha calls happen in private Telegram groups. Add Telethon for group message ingestion.

3. **DEX activity monitoring** — Large swaps on Uniswap/Jupiter signal smart money moves. Use Dune Analytics or direct RPC calls.

4. **GitHub commit tracking** — Developer activity is a leading indicator for L1/L2 tokens. Track commit frequency via GitHub API.

5. **Narrative clustering** — Use topic modeling (BERTopic) to detect emerging narratives (RWA, AI tokens, restaking) before they peak.

6. **Backtesting** — Historical Reddit data from Pushshift + historical prices from CoinGecko for proper walk-forward testing.

7. **Multi-exchange execution** — Route orders to the exchange with the best liquidity/spread using ccxt's multi-exchange support.

## Disclaimer

This is educational code. Crypto markets are extremely volatile and operate 24/7. You can lose your entire investment. Always paper trade extensively before using real funds.
