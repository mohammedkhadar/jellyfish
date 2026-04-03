"""
Crypto Signal Generator
=======================
Multi-factor signal generation with crypto-specific features:
  1. Z-score threshold (primary)
  2. Social velocity confirmation
  3. Whale flow alignment
  4. Funding rate contrarian signal
  5. Fear & Greed regime filter

Key difference from commodities:
  - Fear & Greed regime filter prevents buying into euphoria
    or shorting into panic
  - Social velocity acts as a volume indicator
  - Funding rate provides contrarian edge
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime
from config import Config

logger = logging.getLogger("signals")


class CryptoSignalGenerator:
    def __init__(self, config: Config):
        self.config = config

    def evaluate(self, features: Dict[str, Dict]) -> List[Dict]:
        signals = []
        for token, feat in features.items():
            signal = self._evaluate_token(token, feat)
            if signal:
                signals.append(signal)
        return signals

    def _evaluate_token(self, token: str, feat: Dict) -> Optional[Dict]:
        z_score = feat["z_score"]
        social_vel = feat["social_velocity"]
        whale_delta = feat["whale_delta"]
        funding_skew = feat["funding_skew"]
        fear_greed = feat["fear_greed"]
        mention_count = feat["mention_count"]

        # ─── Primary: Z-score threshold ────────────────────────
        if abs(z_score) < abs(self.config.Z_SCORE_BUY_THRESHOLD):
            return None

        direction = "long" if z_score > 0 else "short"

        # ─── Regime filter (Fear & Greed) ──────────────────────
        # Don't buy when market is in extreme greed
        # Don't short when market is in extreme fear
        if (direction == "long" and
                fear_greed > self.config.FEAR_GREED_EXTREME_GREED):
            logger.info(
                f"  {token}: Long rejected — extreme greed "
                f"(F&G={fear_greed})"
            )
            return None

        if (direction == "short" and
                fear_greed < self.config.FEAR_GREED_EXTREME_FEAR):
            logger.info(
                f"  {token}: Short rejected — extreme fear "
                f"(F&G={fear_greed})"
            )
            return None

        # ─── Confidence scoring ────────────────────────────────
        confidence = 0.0
        reasons = []

        # Z-score magnitude
        z_mag = abs(z_score)
        if z_mag > 3.0:
            confidence += 0.35
            reasons.append(f"Extreme sentiment (z={z_score:.2f})")
        elif z_mag > 2.0:
            confidence += 0.25
            reasons.append(f"Strong sentiment (z={z_score:.2f})")
        else:
            confidence += 0.15
            reasons.append(f"Elevated sentiment (z={z_score:.2f})")

        # Social velocity confirmation
        if abs(social_vel) > 1.0:
            confidence += 0.15
            reasons.append(f"Social velocity spike ({social_vel:.1f}x)")
        elif abs(social_vel) > 0.5:
            confidence += 0.08

        # Whale flow alignment
        if (whale_delta > 0.2 and direction == "long") or \
           (whale_delta < -0.2 and direction == "short"):
            confidence += 0.2
            reasons.append(
                f"Whale flow confirms ({whale_delta:+.2f})"
            )
        elif (whale_delta > 0.2 and direction == "short") or \
             (whale_delta < -0.2 and direction == "long"):
            confidence -= 0.15
            reasons.append(
                f"Whale flow opposes ({whale_delta:+.2f})"
            )

        # Funding rate contrarian
        # High funding + long signal = less confidence (crowded)
        # High funding + short signal = more confidence (contrarian)
        if funding_skew != 0:
            if (funding_skew < 0 and direction == "long"):
                # Contrarian: funding bearish but we're going long
                confidence += 0.1
                reasons.append("Contrarian to funding rate")
            elif (funding_skew > 0 and direction == "short"):
                confidence += 0.1
                reasons.append("Contrarian to funding rate")
            elif (funding_skew > 0 and direction == "long"):
                confidence -= 0.1
                reasons.append("Aligned with crowded longs")

        # Data volume check
        if mention_count < 5:
            confidence -= 0.15
            reasons.append(f"Low data ({mention_count} mentions)")
        elif mention_count > 50:
            confidence += 0.05
            reasons.append(f"High coverage ({mention_count} mentions)")

        # Fear & Greed context bonus
        # Buying in moderate fear = contrarian (good)
        # Shorting in moderate greed = contrarian (good)
        if direction == "long" and 20 < fear_greed < 40:
            confidence += 0.1
            reasons.append(f"Contrarian buy in fear (F&G={fear_greed})")
        elif direction == "short" and 60 < fear_greed < 80:
            confidence += 0.1
            reasons.append(f"Contrarian short in greed (F&G={fear_greed})")

        confidence = max(0.0, min(1.0, confidence))

        if confidence < self.config.MIN_CONFIDENCE:
            logger.info(
                f"  {token}: Rejected (confidence {confidence:.2f})"
            )
            return None

        return {
            "token": token,
            "pair": self.config.TOKEN_CONFIG[token]["pair"],
            "direction": direction,
            "confidence": confidence,
            "z_score": z_score,
            "social_velocity": social_vel,
            "whale_delta": whale_delta,
            "funding_skew": funding_skew,
            "fear_greed": fear_greed,
            "reasons": reasons,
            "generated_at": datetime.utcnow().isoformat(),
        }
