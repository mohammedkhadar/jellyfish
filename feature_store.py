"""
Crypto Feature Store
====================
Maintains rolling features per token:
  - Composite sentiment (text + chain + exchange)
  - Social velocity (rate of mention growth)
  - Whale delta (net exchange flow direction)
  - Funding skew (funding rate deviation)
  - Narrative detection (trending topics)
  - Z-score vs historical
"""

import logging
import sqlite3
import math
from datetime import datetime, timedelta
from typing import List, Dict
from collections import defaultdict

import numpy as np

from config import Config

logger = logging.getLogger("features")


class CryptoFeatureStore:
    def __init__(self, config: Config):
        self.config = config
        self.db = sqlite3.connect(config.DB_PATH)
        self._init_db()

    def _init_db(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS scored_text (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT, source TEXT, weighted_score REAL,
                engagement REAL, scored_at TEXT, title TEXT
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS scored_onchain (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT, flow_type TEXT, amount_usd REAL,
                weighted_score REAL, timestamp TEXT
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS scored_exchange (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT, metric TEXT, value REAL,
                weighted_score REAL, fetched_at TEXT
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS features (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT, timestamp TEXT,
                composite_score REAL, social_velocity REAL,
                whale_delta REAL, funding_skew REAL,
                z_score REAL, mention_count INTEGER,
                fear_greed INTEGER
            )
        """)
        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_text_token_time
            ON scored_text (token, scored_at)
        """)
        self.db.commit()

    def update(
        self,
        scored_text: List[Dict],
        scored_onchain: List[Dict],
        scored_exchange: List[Dict],
    ):
        """Store all scored items."""
        for item in scored_text:
            self.db.execute(
                """INSERT INTO scored_text
                   (token, source, weighted_score, engagement, scored_at, title)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    item.get("token"),
                    item.get("source"),
                    item.get("weighted_score", 0),
                    item.get("engagement", 0),
                    item.get("scored_at"),
                    (item.get("title") or "")[:200],
                ),
            )

        for item in scored_onchain:
            self.db.execute(
                """INSERT INTO scored_onchain
                   (token, flow_type, amount_usd, weighted_score, timestamp)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    item.get("token"),
                    item.get("flow_type"),
                    item.get("amount_usd", 0),
                    item.get("weighted_score", 0),
                    item.get("timestamp"),
                ),
            )

        for item in scored_exchange:
            self.db.execute(
                """INSERT INTO scored_exchange
                   (token, metric, value, weighted_score, fetched_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    item.get("token"),
                    item.get("metric"),
                    item.get("value", 0),
                    item.get("weighted_score", 0),
                    item.get("fetched_at"),
                ),
            )

        self.db.commit()

    def get_latest_features(self) -> Dict[str, Dict]:
        """Compute features for all tokens."""
        features = {}
        now = datetime.utcnow()

        # Get market-wide fear/greed
        fear_greed = self._get_fear_greed()

        for token in self.config.TOKENS:
            features[token] = self._compute_features(token, now, fear_greed)

        return features

    def _compute_features(
        self, token: str, now: datetime, fear_greed: int
    ) -> Dict:
        window = self.config.ROLLING_WINDOW_HOURS
        cutoff = (now - timedelta(hours=window)).isoformat()

        # ─── Text sentiment ────────────────────────────────────
        text_rows = self.db.execute(
            """SELECT weighted_score, scored_at, source
               FROM scored_text
               WHERE token = ? AND scored_at > ?""",
            (token, cutoff),
        ).fetchall()

        text_scores = [r[0] for r in text_rows]
        text_timestamps = [r[1] for r in text_rows]
        text_sources = [r[2] for r in text_rows]

        text_sentiment = self._time_decayed_average(
            text_scores, text_timestamps, now
        ) if text_scores else 0.0

        # ─── On-chain sentiment (whale delta) ──────────────────
        chain_rows = self.db.execute(
            """SELECT weighted_score, amount_usd, flow_type
               FROM scored_onchain
               WHERE token = ? AND timestamp > ?""",
            (token, cutoff),
        ).fetchall()

        whale_delta = 0.0
        if chain_rows:
            # Net flow: positive = more outflows (bullish)
            for score, amount, flow_type in chain_rows:
                whale_delta += score * (amount / 1_000_000)  # Scale by $M
            # Normalize
            whale_delta = math.tanh(whale_delta / 10)

        # ─── Exchange sentiment (funding rate) ─────────────────
        funding_rows = self.db.execute(
            """SELECT value, weighted_score FROM scored_exchange
               WHERE token = ? AND metric = 'funding_rate'
               AND fetched_at > ?
               ORDER BY fetched_at DESC LIMIT 1""",
            (token, cutoff),
        ).fetchall()

        funding_skew = 0.0
        if funding_rows:
            funding_rate = funding_rows[0][0]
            funding_skew = funding_rows[0][1]

        # ─── Composite score ───────────────────────────────────
        # Weighted combination: text 40%, on-chain 35%, exchange 25%
        composite = (
            text_sentiment * 0.40 +
            whale_delta * 0.35 +
            funding_skew * 0.25
        )

        # ─── Social velocity ───────────────────────────────────
        social_velocity = self._compute_social_velocity(token, now)

        # ─── Z-score ───────────────────────────────────────────
        z_score = self._compute_z_score(token, composite, now)

        # ─── Store features ────────────────────────────────────
        feat = {
            "composite_score": composite,
            "text_sentiment": text_sentiment,
            "social_velocity": social_velocity,
            "whale_delta": whale_delta,
            "funding_skew": funding_skew,
            "z_score": z_score,
            "mention_count": len(text_rows),
            "fear_greed": fear_greed,
            "timestamp": now.isoformat(),
        }

        self.db.execute(
            """INSERT INTO features
               (token, timestamp, composite_score, social_velocity,
                whale_delta, funding_skew, z_score, mention_count,
                fear_greed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (token, now.isoformat(), composite, social_velocity,
             whale_delta, funding_skew, z_score, len(text_rows),
             fear_greed),
        )
        self.db.commit()

        return feat

    def _time_decayed_average(
        self, scores: List[float], timestamps: List[str], now: datetime
    ) -> float:
        half_life = self.config.SENTIMENT_DECAY_HALF_LIFE_HOURS
        decay_rate = math.log(2) / half_life

        weighted_sum = 0.0
        weight_sum = 0.0

        for score, ts_str in zip(scores, timestamps):
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                hours_ago = (now - ts.replace(tzinfo=None)).total_seconds() / 3600
                weight = math.exp(-decay_rate * max(0, hours_ago))
                weighted_sum += score * weight
                weight_sum += weight
            except (ValueError, TypeError):
                weighted_sum += score
                weight_sum += 1.0

        return weighted_sum / weight_sum if weight_sum > 0 else 0.0

    def _compute_social_velocity(self, token: str, now: datetime) -> float:
        """
        Social velocity: rate of change in mention volume.

        A sudden spike in mentions (regardless of sentiment direction)
        often precedes big moves. This is the crypto-specific equivalent
        of the "volume spike" feature in the commodity bot.
        """
        recent_window = self.config.MOMENTUM_WINDOW_HOURS
        recent_cutoff = (now - timedelta(hours=recent_window)).isoformat()
        older_cutoff = (
            now - timedelta(hours=recent_window * 2)
        ).isoformat()

        recent_count = self.db.execute(
            """SELECT COUNT(*) FROM scored_text
               WHERE token = ? AND scored_at > ?""",
            (token, recent_cutoff),
        ).fetchone()[0]

        older_count = self.db.execute(
            """SELECT COUNT(*) FROM scored_text
               WHERE token = ? AND scored_at > ? AND scored_at <= ?""",
            (token, older_cutoff, recent_cutoff),
        ).fetchone()[0]

        if older_count == 0:
            return 1.0 if recent_count > 5 else 0.0

        return (recent_count - older_count) / max(older_count, 1)

    def _compute_z_score(
        self, token: str, current: float, now: datetime
    ) -> float:
        lookback = self.config.Z_SCORE_LOOKBACK_DAYS
        cutoff = (now - timedelta(days=lookback)).isoformat()

        rows = self.db.execute(
            """SELECT composite_score FROM features
               WHERE token = ? AND timestamp > ?""",
            (token, cutoff),
        ).fetchall()

        if len(rows) < 10:
            return 0.0

        historical = [r[0] for r in rows]
        mean = np.mean(historical)
        std = np.std(historical)

        if std < 0.001:
            return 0.0

        return (current - mean) / std

    def _get_fear_greed(self) -> int:
        """Get most recent fear & greed index value."""
        row = self.db.execute(
            """SELECT value FROM scored_exchange
               WHERE metric = 'fear_greed_index'
               ORDER BY fetched_at DESC LIMIT 1"""
        ).fetchone()
        return int(row[0]) if row else 50
