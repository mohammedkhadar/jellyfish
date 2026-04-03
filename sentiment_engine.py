"""
Crypto Sentiment Engine
=======================
Three scoring pipelines:
  1. Text scoring: FinBERT + crypto lexicon (social + news)
  2. On-chain scoring: whale flow direction + magnitude
  3. Exchange scoring: funding rate extremes, OI changes

Key crypto-specific adaptations:
  - Crypto slang handling (moon, FUD, rug pull, etc.)
  - Engagement-weighted scoring (viral posts > low-engagement)
  - Sarcasm discount (crypto communities are heavily sarcastic)
"""

import logging
import math
from typing import List, Dict
from datetime import datetime
from config import Config

logger = logging.getLogger("sentiment")

_model = None
_tokenizer = None


def _load_finbert(model_name: str):
    global _model, _tokenizer
    if _model is None:
        logger.info(f"Loading FinBERT: {model_name}")
        from transformers import AutoTokenizer, AutoModelForSequenceClassification
        import torch
        _tokenizer = AutoTokenizer.from_pretrained(model_name)
        _model = AutoModelForSequenceClassification.from_pretrained(model_name)
        _model.eval()
        logger.info("FinBERT loaded")
    return _model, _tokenizer


class CryptoSentimentEngine:
    def __init__(self, config: Config):
        self.config = config
        self.lexicon = config.CRYPTO_LEXICON
        self.source_weights = config.SOURCE_WEIGHTS

    # ─── Text Scoring (Social + News) ──────────────────────────
    def score_batch(self, items: List[Dict]) -> List[Dict]:
        """Score text items (social media posts, news articles)."""
        if not items:
            return []

        texts = [item.get("text", "") for item in items]
        finbert_scores = self._finbert_score(texts)

        scored = []
        for item, base_score in zip(items, finbert_scores):
            token = item.get("token", "BTC")

            # Lexicon adjustment
            lexicon_adj = self._lexicon_adjustment(item["text"], token)

            # Engagement multiplier (for social media)
            engagement_mult = self._engagement_multiplier(item)

            # Sarcasm discount
            sarcasm_discount = self._sarcasm_discount(item["text"])

            # Source weight
            source = item.get("source", "unknown")
            source_weight = self.source_weights.get(source, 0.5)

            # Composite
            raw_score = (base_score + lexicon_adj) * sarcasm_discount
            weighted_score = raw_score * source_weight * engagement_mult

            scored.append({
                **item,
                "finbert_score": base_score,
                "lexicon_adjustment": lexicon_adj,
                "engagement_multiplier": engagement_mult,
                "sarcasm_discount": sarcasm_discount,
                "raw_score": raw_score,
                "weighted_score": weighted_score,
                "source_weight": source_weight,
                "scored_at": datetime.utcnow().isoformat(),
            })

        return scored

    def _finbert_score(self, texts: List[str]) -> List[float]:
        """Score using FinBERT. Returns [-1, +1]."""
        try:
            import torch
            model, tokenizer = _load_finbert(self.config.FINBERT_MODEL)
        except ImportError:
            logger.warning("transformers not installed — lexicon-only mode")
            return [0.0] * len(texts)

        scores = []
        batch_size = self.config.SENTIMENT_BATCH_SIZE

        for i in range(0, len(texts), batch_size):
            batch = [t[:1500] for t in texts[i:i + batch_size]]
            inputs = tokenizer(
                batch, padding=True, truncation=True,
                max_length=512, return_tensors="pt",
            )
            with torch.no_grad():
                outputs = model(**inputs)
                probs = torch.softmax(outputs.logits, dim=-1)

            for prob in probs:
                scores.append(prob[0].item() - prob[1].item())

        return scores

    def _lexicon_adjustment(self, text: str, token: str) -> float:
        """Apply crypto-specific lexicon adjustments."""
        text_lower = text.lower()
        adjustment = 0.0
        matches = 0

        for term, (score, applies_to) in self.lexicon.items():
            if term.lower() in text_lower:
                if applies_to == "all" or applies_to == token:
                    adjustment += score
                    matches += 1

        if matches > 3:
            adjustment = adjustment / (matches * 0.5)

        return max(-0.8, min(0.8, adjustment))

    def _engagement_multiplier(self, item: Dict) -> float:
        """
        Weight by engagement for social media posts.

        High-engagement posts (viral tweets, top Reddit posts)
        carry more signal than low-engagement ones.
        """
        engagement = item.get("engagement", 0)
        source = item.get("source", "")

        if source in ("reddit_post", "reddit_comment"):
            # Reddit: score is upvotes, amplify high-score posts
            if engagement > 500:
                return 1.5
            elif engagement > 100:
                return 1.2
            elif engagement > 20:
                return 1.0
            else:
                return 0.7  # Low engagement = less signal

        if source == "twitter":
            if engagement > 1000:
                return 1.5
            elif engagement > 200:
                return 1.2
            else:
                return 1.0

        return 1.0  # News and other sources: neutral weight

    def _sarcasm_discount(self, text: str) -> float:
        """
        Discount sentiment for likely sarcastic posts.

        Crypto communities use heavy sarcasm. Posts with
        extreme positive language + certain markers are
        often ironic.

        Returns multiplier: 1.0 = no discount, 0.5 = heavy discount.
        """
        text_lower = text.lower()

        sarcasm_markers = [
            "definitely going to",
            "this time is different",
            "can't go tits up",
            "free money",
            "guaranteed",
            "100x",
            "trust me bro",
            "financial advice",
            "not financial advice",
            "/s",
            "copium",
            "hopium",
            "ngmi",         # Not gonna make it (often sarcastic)
            "few understand",
        ]

        sarcasm_hits = sum(
            1 for marker in sarcasm_markers if marker in text_lower
        )

        if sarcasm_hits >= 2:
            return 0.5   # Heavy discount
        elif sarcasm_hits == 1:
            return 0.75  # Moderate discount
        return 1.0       # No discount

    # ─── On-Chain Scoring ──────────────────────────────────────
    def score_onchain(self, items: List[Dict]) -> List[Dict]:
        """
        Score on-chain data (whale movements, exchange flows).

        Logic:
          - Exchange inflow (deposit to sell) → bearish
          - Exchange outflow (withdraw to hold) → bullish
          - Large wallet-to-wallet → slight bullish (accumulation)
        """
        scored = []
        for item in items:
            flow_score = item.get("flow_score", 0.0)
            amount_usd = item.get("amount_usd", 0)

            # Scale by transaction size
            if amount_usd > 50_000_000:
                magnitude = 1.5    # $50M+ = very significant
            elif amount_usd > 10_000_000:
                magnitude = 1.2
            elif amount_usd > 1_000_000:
                magnitude = 1.0
            else:
                magnitude = 0.7

            weighted_score = flow_score * magnitude

            scored.append({
                **item,
                "magnitude": magnitude,
                "weighted_score": weighted_score,
                "scored_at": datetime.utcnow().isoformat(),
            })

        return scored

    # ─── Exchange Metric Scoring ───────────────────────────────
    def score_exchange(self, items: List[Dict]) -> List[Dict]:
        """
        Score exchange-level metrics.

        Funding rate:
          - High positive → crowded longs → contrarian bearish
          - High negative → crowded shorts → contrarian bullish
        """
        scored = []
        for item in items:
            scored.append({
                **item,
                "weighted_score": item.get("score", 0.0),
                "scored_at": datetime.utcnow().isoformat(),
            })
        return scored
