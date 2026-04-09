"""
config.py — Load environment variables and expose a typed Config dataclass.

All modules import `cfg` from here rather than reading os.environ directly.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    """Read a required env var, raising if absent."""
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. Check your .env file."
        )
    return val


def _opt(key: str, default: str = "") -> str:
    """Read an optional env var with a fallback default."""
    return os.getenv(key, default)


@dataclass
class Config:
    """Typed configuration object for the arb daemon."""

    # ── Polymarket (EVM / Polygon) ─────────────────────────────────────────
    wallet_private_key: str = field(default_factory=lambda: _opt("WALLET_PRIVATE_KEY"))
    """EVM private key for Polymarket order placement on Polygon."""

    wallet_address: str = field(default_factory=lambda: _opt("WALLET_ADDRESS"))
    """EVM wallet address (0x…)."""

    poly_api_key: str = field(default_factory=lambda: _opt("POLY_API_KEY"))
    poly_api_secret: str = field(default_factory=lambda: _opt("POLY_API_SECRET"))
    poly_api_passphrase: str = field(default_factory=lambda: _opt("POLY_API_PASSPHRASE"))

    # ── Kalshi ─────────────────────────────────────────────────────────────
    kalshi_key_id: str = field(default_factory=lambda: _opt("KALSHI_KEY_ID"))
    """Kalshi API Key ID from account settings."""

    kalshi_private_key: str = field(default_factory=lambda: _opt("KALSHI_PRIVATE_KEY"))
    """
    RSA private key PEM string for signing Kalshi requests.
    Store the full PEM including -----BEGIN/END----- lines.
    In .env, replace newlines with \\n:
      KALSHI_PRIVATE_KEY=-----BEGIN RSA PRIVATE KEY-----\\nMIIE...\\n-----END RSA PRIVATE KEY-----
    """

    kalshi_private_key_path: str = field(
        default_factory=lambda: _opt("KALSHI_PRIVATE_KEY_PATH", "")
    )
    """Path to RSA private key .pem file. Takes precedence over KALSHI_PRIVATE_KEY if set."""

    kalshi_base_url: str = field(
        default_factory=lambda: _opt(
            "KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"
        )
    )
    """Despite the elections subdomain, serves ALL Kalshi markets. Demo: https://demo-api.kalshi.co/trade-api/v2"""

    # ── OpenAI (embeddings) ────────────────────────────────────────────────
    openai_api_key: str = field(default_factory=lambda: _opt("OPENAI_API_KEY"))

    # ── Scheduler timings ──────────────────────────────────────────────────
    fetch_interval_hours: float = field(
        default_factory=lambda: float(_opt("FETCH_INTERVAL_HOURS", "4"))
    )
    monitor_interval_seconds: float = field(
        default_factory=lambda: float(_opt("MONITOR_INTERVAL_SECONDS", "15"))
    )

    # ── Trading thresholds ─────────────────────────────────────────────────
    min_profit_threshold: float = field(
        default_factory=lambda: float(_opt("MIN_PROFIT_THRESHOLD", "0.02"))
    )
    """Minimum gross profit fraction (e.g. 0.02 = 2%) to execute a trade."""

    max_bet_size_usd: float = field(
        default_factory=lambda: float(_opt("MAX_BET_SIZE_USD", "50"))
    )
    """Hard cap in USD per leg."""

    min_profit_abs: float = field(
        default_factory=lambda: float(_opt("MIN_PROFIT_ABS", "1.00"))
    )
    """Minimum absolute profit per trade in USD."""

    # ── Embedding model ────────────────────────────────────────────────────
    embedding_model: str = field(
        default_factory=lambda: _opt("EMBEDDING_MODEL", "openai")
    )
    """'openai' or 'local'."""

    # ── Database ───────────────────────────────────────────────────────────
    db_path: str = field(default_factory=lambda: _opt("DB_PATH", "arb.db"))

    # ── Logging ────────────────────────────────────────────────────────────
    log_level: str = field(default_factory=lambda: _opt("LOG_LEVEL", "INFO"))

    # ── Dry-run ────────────────────────────────────────────────────────────
    dry_run: bool = field(
        default_factory=lambda: _opt("DRY_RUN", "false").lower() == "true"
    )
    """When True, no real orders are placed. Log [DRY_RUN] actions instead."""

    # ── Matching thresholds (tune here, not in .env) ───────────────────────
    match_confirm_threshold: float = 0.90
    match_candidate_threshold: float = 0.70
    end_date_tolerance_days: int = 3
    orderbook_vol_fraction: float = 0.20


# Singleton config instance imported by all modules.
cfg = Config()