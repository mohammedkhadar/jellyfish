"""
Crypto Data Ingestion Pipeline
==============================
Fetches data from:
  - Reddit (r/bitcoin, r/ethereum, r/solana, r/CryptoCurrency)
  - Crypto news (NewsAPI, CoinDesk RSS)
  - On-chain (Whale Alert, CoinGecko exchange flows)
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
        self._reddit_token: str = ""
        self._reddit_token_expiry: float = 0.0

    def _get_reddit_token(self) -> str:
        """Fetch OAuth token using client credentials."""
        if time.time() < self._reddit_token_expiry:
            return self._reddit_token
        resp = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(self.config.REDDIT_CLIENT_ID, self.config.REDDIT_CLIENT_SECRET),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": self.config.REDDIT_USER_AGENT},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        self._reddit_token = data["access_token"]
        self._reddit_token_expiry = time.time() + data.get("expires_in", 3600) - 60
        return self._reddit_token

    def fetch_all(self) -> Dict[str, List[Dict]]:
        """Fetch from all sources, organized by type."""
        result = {
            "social": [],
            "news": [],
            "onchain": [],
            "exchange": [],
        }

        # Social media
        for token in self.config.TOKENS:
            try:
                reddit_data = self._fetch_reddit(token)
                result["social"].extend(reddit_data)
            except Exception as e:
                logger.error(f"Reddit error for {token}: {e}")

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

    def _fetch_reddit(self, token: str) -> List[Dict]:
        """
        Fetch recent posts and top comments from crypto subreddits.

        Uses Reddit JSON endpoint (no auth needed for basic access).
        For production, use PRAW with OAuth.
        """
        token_config = self.config.TOKEN_CONFIG[token]
        subreddits = token_config.get("subreddits", [])
        articles = []

        for sub in subreddits:
            try:
                if self.config.REDDIT_CLIENT_ID and self.config.REDDIT_CLIENT_SECRET:
                    url = f"https://oauth.reddit.com/r/{sub}/hot.json"
                    headers = {
                        "User-Agent": self.config.REDDIT_USER_AGENT,
                        "Authorization": f"bearer {self._get_reddit_token()}",
                    }
                else:
                    url = f"https://www.reddit.com/r/{sub}/hot.json"
                    headers = {"User-Agent": self.config.REDDIT_USER_AGENT}
                resp = requests.get(url, headers=headers, timeout=10)

                if resp.status_code == 429:
                    logger.warning(f"Reddit rate limited on r/{sub}")
                    time.sleep(2)
                    continue

                resp.raise_for_status()
                data = resp.json()

                for post in data.get("data", {}).get("children", [])[:25]:
                    post_data = post.get("data", {})
                    created = datetime.utcfromtimestamp(
                        post_data.get("created_utc", 0)
                    )

                    # Only recent posts (last 24h)
                    if datetime.utcnow() - created > timedelta(hours=24):
                        continue

                    title = post_data.get("title", "")
                    selftext = post_data.get("selftext", "")[:500]
                    score = post_data.get("score", 0)
                    num_comments = post_data.get("num_comments", 0)

                    articles.append({
                        "type": "social",
                        "source": "reddit_post",
                        "token": token,
                        "subreddit": sub,
                        "title": title,
                        "text": f"{title}. {selftext}".strip(),
                        "score": score,            # Upvotes
                        "num_comments": num_comments,
                        "engagement": score + num_comments * 2,
                        "published_at": created.isoformat(),
                        "fetched_at": datetime.utcnow().isoformat(),
                    })

                logger.info(f"  Reddit r/{sub}: {len(articles)} posts")
                time.sleep(1)  # Respect rate limits

            except Exception as e:
                logger.error(f"Reddit r/{sub}: {e}")

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
