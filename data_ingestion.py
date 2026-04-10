"""
Crypto Data Ingestion Pipeline
==============================
Fetches data from:
  - CoinGecko (trending, community stats, market data — free, no key)
  - RSS feeds (CoinDesk, CoinTelegraph, Decrypt — free, no key)
  - Crypto news (NewsAPI)
  - On-chain (Whale Alert)
  - Exchange metrics (funding rates, open interest via ccxt)
  - Fear & Greed Index (Alternative.me)
"""

import logging
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict
from config import Config

logger = logging.getLogger("ingestion")


class CryptoDataPipeline:
    def __init__(self, config: Config):
        self.config = config

    def fetch_all(self) -> Dict[str, List[Dict]]:
        """Fetch from all sources, organized by type."""
        result = {
            "social": [],
            "news": [],
            "onchain": [],
            "exchange": [],
        }

        # Social / community data from CoinGecko + RSS news feeds
        for token in self.config.TOKENS:
            try:
                cg_data = self._fetch_coingecko_community(token)
                result["social"].extend(cg_data)
            except Exception as e:
                logger.error(f"CoinGecko community error for {token}: {e}")

        try:
            rss_data = self._fetch_rss_news()
            result["social"].extend(rss_data)
        except Exception as e:
            logger.error(f"RSS news error: {e}")

        try:
            trending = self._fetch_coingecko_trending()
            result["social"].extend(trending)
        except Exception as e:
            logger.error(f"CoinGecko trending error: {e}")

        # News
        try:
            news = self._fetch_crypto_news()
            result["news"].extend(news)
        except Exception as e:
            logger.error(f"News error: {e}")

        # On-chain
        try:
            whale_data = self._fetch_whale_alerts()
            result["onchain"].extend(whale_data)
        except Exception as e:
            logger.error(f"Whale alert error: {e}")

        # Exchange metrics
        for token in self.config.TOKENS:
            try:
                exchange_data = self._fetch_exchange_metrics(token)
                result["exchange"].extend(exchange_data)
            except Exception as e:
                logger.error(f"Exchange data error for {token}: {e}")

        # Fear & Greed
        try:
            fng = self._fetch_fear_greed()
            if fng:
                result["exchange"].append(fng)
        except Exception as e:
            logger.error(f"Fear & Greed error: {e}")

        return result

    def _fetch_coingecko_community(self, token: str) -> List[Dict]:
        """
        Fetch community/social stats from CoinGecko for a token.

        Free API, no key needed. Returns:
          - Community score, developer score, public interest score
          - Social media stats (Twitter followers, Telegram members)
          - Sentiment votes (up/down percentage)
        """
        token_config = self.config.TOKEN_CONFIG[token]
        cg_id = token_config.get("coingecko_id", "")
        if not cg_id:
            return []

        url = f"{self.config.COINGECKO_API_URL}/coins/{cg_id}"
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "true",
            "developer_data": "false",
            "sparkline": "false",
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 429:
            logger.warning("CoinGecko rate-limited, waiting 30s")
            time.sleep(30)
            resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        articles = []
        now = datetime.utcnow().isoformat()

        # Community data as a social signal
        community = data.get("community_data", {})
        sentiment_up = data.get("sentiment_votes_up_percentage", 50)
        sentiment_down = data.get("sentiment_votes_down_percentage", 50)
        community_score = data.get("community_score", 0) or 0

        # Normalize sentiment: 0-100 → -1 to +1
        sentiment_norm = (sentiment_up - sentiment_down) / 100.0

        description = data.get("description", {}).get("en", "")[:500]
        name = data.get("name", token)

        articles.append({
            "type": "social",
            "source": "coingecko",
            "token": token,
            "title": f"{name} community sentiment",
            "text": (
                f"{name} sentiment: {sentiment_up:.0f}% bullish, "
                f"{sentiment_down:.0f}% bearish. "
                f"Community score: {community_score}. "
                f"Twitter followers: {community.get('twitter_followers', 0)}. "
                f"Telegram members: {community.get('telegram_channel_user_count', 0)}."
            ),
            "score": int(sentiment_up - sentiment_down),
            "engagement": community_score,
            "sentiment_up_pct": sentiment_up,
            "sentiment_down_pct": sentiment_down,
            "sentiment_normalized": sentiment_norm,
            "published_at": now,
            "fetched_at": now,
        })

        # Market data for price context
        market = data.get("market_data", {})
        price_change_24h = market.get("price_change_percentage_24h", 0) or 0
        price_change_7d = market.get("price_change_percentage_7d", 0) or 0

        articles.append({
            "type": "social",
            "source": "coingecko",
            "token": token,
            "title": f"{name} market overview",
            "text": (
                f"{name} price change: {price_change_24h:+.1f}% (24h), "
                f"{price_change_7d:+.1f}% (7d). "
                f"Market cap rank: #{market.get('market_cap_rank', 'N/A')}."
            ),
            "score": int(price_change_24h * 10),
            "engagement": 0,
            "published_at": now,
            "fetched_at": now,
        })

        logger.info(f"  CoinGecko {token}: community + market data")
        time.sleep(2)  # Respect CoinGecko rate limits (10-30 req/min)
        return articles

    def _fetch_coingecko_trending(self) -> List[Dict]:
        """
        Fetch trending coins from CoinGecko.

        High trending score for tracked tokens = social momentum signal.
        """
        url = f"{self.config.COINGECKO_API_URL}/search/trending"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        articles = []
        now = datetime.utcnow().isoformat()
        tracked_ids = {
            self.config.TOKEN_CONFIG[t]["coingecko_id"]: t
            for t in self.config.TOKENS
        }

        for coin_entry in data.get("coins", []):
            item = coin_entry.get("item", {})
            cg_id = item.get("id", "")
            if cg_id not in tracked_ids:
                continue

            token = tracked_ids[cg_id]
            rank = item.get("score", 0)
            name = item.get("name", token)
            price_change = item.get("data", {}).get(
                "price_change_percentage_24h", {}
            ).get("usd", 0) or 0

            articles.append({
                "type": "social",
                "source": "coingecko_trending",
                "token": token,
                "title": f"{name} is trending on CoinGecko",
                "text": (
                    f"{name} is trending (rank #{rank + 1}). "
                    f"24h price change: {price_change:+.1f}%."
                ),
                "score": max(100 - rank * 10, 10),
                "engagement": max(100 - rank * 10, 10),
                "trending_rank": rank,
                "published_at": now,
                "fetched_at": now,
            })

        logger.info(f"  CoinGecko trending: {len(articles)} tracked coins")
        time.sleep(2)
        return articles

    def _fetch_rss_news(self) -> List[Dict]:
        """
        Fetch crypto news from RSS feeds (CoinDesk, CoinTelegraph, Decrypt).

        Free, no auth, no rate limits. Headlines are fed into FinBERT
        for sentiment scoring, replacing the discontinued CryptoPanic API.
        """
        import feedparser

        articles = []
        now = datetime.utcnow().isoformat()

        for feed_url in self.config.RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
                source_name = feed.feed.get("title", feed_url)

                for entry in feed.entries[:20]:
                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    link = entry.get("link", "")
                    published = entry.get("published", now)

                    # Match to tracked tokens via keywords
                    text = f"{title} {summary}".lower()
                    matched_tokens = []
                    for token in self.config.TOKENS:
                        keywords = self.config.TOKEN_CONFIG[token].get("keywords", [])
                        if any(kw.lower() in text for kw in keywords):
                            matched_tokens.append(token)

                    if not matched_tokens:
                        continue

                    for token in matched_tokens:
                        articles.append({
                            "type": "social",
                            "source": "rss_news",
                            "token": token,
                            "title": title,
                            "text": f"{title} (via {source_name})",
                            "url": link,
                            "score": 0,
                            "engagement": 0,
                            "published_at": published,
                            "fetched_at": now,
                        })
            except Exception as e:
                logger.warning(f"RSS feed error ({feed_url}): {e}")

        logger.info(f"  RSS news: {len(articles)} items")
        return articles

    def _fetch_crypto_news(self) -> List[Dict]:
        """Fetch crypto news from NewsAPI."""
        if not self.config.NEWS_API_KEY:
            return []

        # Build query for all tracked tokens
        all_keywords = []
        for token in self.config.TOKENS:
            kw = self.config.TOKEN_CONFIG[token]["keywords"][:3]
            all_keywords.extend(kw)

        query = " OR ".join(f'"{kw}"' for kw in all_keywords[:8])

        params = {
            "q": query,
            "apiKey": self.config.NEWS_API_KEY,
            "language": "en",
            "sortBy": "publishedAt",
            "from": (datetime.utcnow() - timedelta(hours=12)).isoformat(),
            "pageSize": 50,
        }

        resp = requests.get(self.config.NEWS_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        articles = []
        for article in data.get("articles", []):
            # Determine which token(s) this article is about
            text = (
                (article.get("title") or "") + " " +
                (article.get("description") or "")
            ).lower()

            matched_tokens = []
            for token in self.config.TOKENS:
                keywords = self.config.TOKEN_CONFIG[token]["keywords"]
                if any(kw.lower() in text for kw in keywords):
                    matched_tokens.append(token)

            if not matched_tokens:
                matched_tokens = ["BTC"]  # Default: general crypto → BTC

            for token in matched_tokens:
                articles.append({
                    "type": "news",
                    "source": "news",
                    "token": token,
                    "title": article.get("title", ""),
                    "text": (
                        (article.get("title") or "") + ". " +
                        (article.get("description") or "")
                    ),
                    "url": article.get("url", ""),
                    "published_at": article.get("publishedAt", ""),
                    "fetched_at": datetime.utcnow().isoformat(),
                })

        logger.info(f"  News: {len(articles)} articles")
        return articles

    def _fetch_whale_alerts(self) -> List[Dict]:
        """
        Fetch large crypto transactions from Whale Alert API.

        Large exchange inflows → bearish (selling pressure)
        Large exchange outflows → bullish (accumulation)
        """
        if not self.config.WHALE_ALERT_KEY:
            return []

        params = {
            "api_key": self.config.WHALE_ALERT_KEY,
            "min_value": self.config.WHALE_MIN_USD,
            "start": int(
                (datetime.utcnow() - timedelta(hours=1)).timestamp()
            ),
            "cursor": "",
        }

        resp = requests.get(
            self.config.WHALE_ALERT_URL, params=params, timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

        alerts = []
        exchange_names = {
            "binance", "coinbase", "kraken", "okx", "bybit",
            "bitfinex", "huobi", "kucoin", "gemini",
        }

        for txn in data.get("transactions", []):
            symbol = txn.get("symbol", "").upper()
            if symbol not in self.config.TOKENS:
                continue

            amount_usd = txn.get("amount_usd", 0)
            from_owner = (txn.get("from", {}).get("owner", "") or "").lower()
            to_owner = (txn.get("to", {}).get("owner", "") or "").lower()

            # Classify flow direction
            from_exchange = from_owner in exchange_names
            to_exchange = to_owner in exchange_names

            if to_exchange and not from_exchange:
                flow_type = "exchange_inflow"   # Bearish
                flow_score = -0.5
            elif from_exchange and not to_exchange:
                flow_type = "exchange_outflow"  # Bullish
                flow_score = 0.5
            elif from_exchange and to_exchange:
                flow_type = "exchange_transfer"
                flow_score = 0.0
            else:
                flow_type = "wallet_transfer"
                flow_score = 0.1  # Slight bullish (accumulation)

            alerts.append({
                "type": "onchain",
                "source": "whale_alert",
                "token": symbol,
                "flow_type": flow_type,
                "amount_usd": amount_usd,
                "flow_score": flow_score,
                "hash": txn.get("hash", ""),
                "timestamp": datetime.utcfromtimestamp(
                    txn.get("timestamp", 0)
                ).isoformat(),
                "fetched_at": datetime.utcnow().isoformat(),
            })

        logger.info(f"  Whale alerts: {len(alerts)} transactions")
        return alerts

    def _fetch_exchange_metrics(self, token: str) -> List[Dict]:
        """
        Fetch exchange-level metrics: funding rate, open interest.

        Uses ccxt for exchange data.
        """
        try:
            import ccxt
        except ImportError:
            logger.warning("ccxt not installed — skipping exchange metrics")
            return []

        pair = self.config.TOKEN_CONFIG[token]["pair"]
        metrics = []

        try:
            exchange = ccxt.binance({
                "apiKey": self.config.EXCHANGE_API_KEY,
                "secret": self.config.EXCHANGE_SECRET,
                "options": {"defaultType": "future"},
            })

            # Funding rate
            try:
                funding = exchange.fetch_funding_rate(pair)
                funding_rate = funding.get("fundingRate", 0)

                # Score: extreme positive funding = bearish (crowded longs)
                # extreme negative funding = bullish (crowded shorts)
                if funding_rate > self.config.FUNDING_RATE_EXTREME_LONG:
                    score = -0.5
                elif funding_rate < self.config.FUNDING_RATE_EXTREME_SHORT:
                    score = 0.5
                else:
                    score = -funding_rate * 30  # Linear scaling

                metrics.append({
                    "type": "exchange",
                    "source": "exchange_data",
                    "token": token,
                    "metric": "funding_rate",
                    "value": funding_rate,
                    "score": score,
                    "fetched_at": datetime.utcnow().isoformat(),
                })
            except Exception as e:
                logger.debug(f"Funding rate error for {token}: {e}")

            # Open interest
            try:
                oi_data = exchange.fetch_open_interest(pair)
                metrics.append({
                    "type": "exchange",
                    "source": "exchange_data",
                    "token": token,
                    "metric": "open_interest",
                    "value": oi_data.get("openInterestAmount", 0),
                    "score": 0.0,  # OI alone isn't directional
                    "fetched_at": datetime.utcnow().isoformat(),
                })
            except Exception as e:
                logger.debug(f"OI error for {token}: {e}")

        except Exception as e:
            logger.error(f"Exchange metrics error for {token}: {e}")

        return metrics

    def _fetch_fear_greed(self) -> Dict:
        """
        Fetch Crypto Fear & Greed Index from Alternative.me.

        Scale: 0 = Extreme Fear → 100 = Extreme Greed
        Used as a market regime indicator, not a directional signal.
        """
        resp = requests.get(self.config.FEAR_GREED_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        fng_data = data.get("data", [{}])[0]
        value = int(fng_data.get("value", 50))
        classification = fng_data.get("value_classification", "Neutral")

        return {
            "type": "exchange",
            "source": "fear_greed",
            "token": "MARKET",  # Applies to all tokens
            "metric": "fear_greed_index",
            "value": value,
            "classification": classification,
            "score": 0.0,  # Used as filter, not signal
            "fetched_at": datetime.utcnow().isoformat(),
        }
