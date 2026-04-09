"""
kalshi_fetcher.py — Fetches active markets from Kalshi using raw HTTP + RSA-PSS auth.

No external Kalshi SDK required. Authentication is handled manually:
  - Every request is signed with your RSA private key
  - Three headers are added: KALSHI-ACCESS-KEY, KALSHI-ACCESS-TIMESTAMP, KALSHI-ACCESS-SIGNATURE
  - The signature covers: timestamp_ms + METHOD + path (no query string)

Base URL: https://api.elections.kalshi.com/trade-api/v2
Despite the "elections" subdomain, this serves ALL Kalshi markets.
"""

import asyncio
import base64
import datetime
from typing import Optional
from collections import defaultdict

import aiohttp
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from config import cfg
from db import (
    _get_conn,
    get_seen_kalshi_ids
)
from logger import get_logger

log = get_logger(__name__)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
PAGE_SIZE = 100


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _load_private_key():
    """
    Load the RSA private key from a .pem file or inline env var.

    Prefers KALSHI_PRIVATE_KEY_PATH (file) over KALSHI_PRIVATE_KEY (string).

    Returns:
        RSA private key object from the cryptography library.
    """
    if cfg.kalshi_private_key_path:
        with open(cfg.kalshi_private_key_path, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)
    pem = cfg.kalshi_private_key.replace("\\n", "\n")
    return serialization.load_pem_private_key(pem.encode("utf-8"), password=None)


def build_auth_headers(method: str, path: str) -> dict:
    """
    Build the three Kalshi authentication headers for a request.

    Signs the string: {timestamp_ms}{METHOD}{path_without_query}
    using RSA-PSS with SHA-256.

    Args:
        method: HTTP method in uppercase e.g. "GET", "POST".
        path: URL path WITHOUT query string e.g. "/trade-api/v2/markets"

    Returns:
        Dict containing KALSHI-ACCESS-KEY, KALSHI-ACCESS-TIMESTAMP,
        KALSHI-ACCESS-SIGNATURE, and Content-Type.
    """
    ts_ms = str(int(datetime.datetime.now().timestamp() * 1000))
    clean_path = path.split("?")[0]
    msg = (ts_ms + method.upper() + clean_path).encode("utf-8")

    private_key = _load_private_key()
    signature = private_key.sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )

    return {
        "KALSHI-ACCESS-KEY": cfg.kalshi_key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "Content-Type": "application/json",
    }



# ---------------------------------------------------------------------------
# Sports game-winner filter
# ---------------------------------------------------------------------------

SERIES_TICKER_NOT_USING= {
    "nba":"KXNBAGAME",       # NBA game winner
    "mlb":"KXMLBGAME",       # MLB game winner
    "nhl":"KXNHLGAME",       # NHL game winner
    "laliga":"KXLALIGAGAME",    # La Liga game winner
    "epl":"KXEPLGAME",       # English Premier League game winner
    "ucl":"KXUCLGAME",       # UEFA Champions League game winner
    "serieA":"KXSERIEA",       # Serie A game winner
    "ligue1":"KXLIGUE1GAME",    # Ligue 1 game winner
}

SERIES_TICKER = {
    "ucl": "KXUCLGAME",       # UEFA Champions League game winner
}

# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

async def _get_json(
    session: aiohttp.ClientSession,
    path: str,
    params: Optional[dict] = None,
) -> dict:
    """
    Perform a signed GET to the Kalshi API and return parsed JSON.

    Args:
        session: Active aiohttp ClientSession.
        path: API path e.g. "/trade-api/v2/markets"
        params: Optional query parameters (not included in signature).

    Returns:
        Parsed JSON response as dict.

    Raises:
        aiohttp.ClientResponseError: On non-2xx response.
    """
    headers = build_auth_headers("GET", path)
    url = f"https://api.elections.kalshi.com{path}"

    async with session.get(url, params=params, headers=headers) as resp:
        resp.raise_for_status()
        return await resp.json()


# ---------------------------------------------------------------------------
# Market fetching
# ---------------------------------------------------------------------------

async def fetch_all_kalshi_markets(session: aiohttp.ClientSession) -> list[dict]:
    categorized_markets = defaultdict(list)
    
    for sport, tag in SERIES_TICKER.items():
        log.debug(f"Fetching Kalshi markets for {sport} (series_ticker={tag})…")
        cursor = None
        page_num = 0
        while True:
            page_num += 1
            params = {"limit": PAGE_SIZE, "status": "open", "mve_filter": "exclude", "series_ticker": tag}

            if cursor:
                params["cursor"] = cursor

            try:
                data = await _get_json(session, "/trade-api/v2/markets", params)
            except Exception as exc:
                log.error(f"Kalshi /markets error: {exc}")
                break

            page = data.get("markets", [])
            new_cursor = data.get("cursor")

            log.debug(f"Kalshi {sport} page {page_num} fetched with {len(page)} markets.")

            if not page:
                log.debug(f"Kalshi {sport} page {page_num} is empty.")
                break

            real = [m for m in page if m.get("is_provisional") != True]
            categorized_markets[sport].extend(real)

            if not new_cursor:
                log.debug(f"Kalshi fetching done for {sport}.")
                break

            cursor = new_cursor

    log.info(f"Kalshi: {sum(len(markets) for markets in categorized_markets.values())} total open markets fetched.")
    return categorized_markets


# ---------------------------------------------------------------------------
# Market normalisation
# ---------------------------------------------------------------------------

def _normalise_market(raw, category) -> Optional[dict]:
    """
    Args:
        raw: Raw market dict from the Kalshi REST API.

    Returns:
        Dict conforming to the kalshi_markets table schema, or None if
        the market has no ticker.
    """
    ticker = raw.get("ticker")

    if not ticker:
        return None

    return {
        "market_id": ticker,
        "question": raw.get("title", "")+" "+raw.get("subtitle", "")+" - Yes token: "+raw.get("yes_sub_title"),
        "category": category,
        "end_date": str(raw.get("close_time")),
        "status": str(raw.get("status")).lower(),
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def fetch_new_kalshi_markets() -> list[dict]:
    """
    Poll Kalshi for open markets and persist new ones to arb.db.

    Steps:
      1. Load already-seen Kalshi tickers from arb.db.
      2. Paginate through GET /markets?status=open with signed headers.
      3. Normalise each market — skip if already seen or not open.
      4. Persist all new markets to kalshi_markets + kalshi_unmatched.
      5. Return list of new market dicts.

    Returns:
        List of newly discovered Kalshi market dicts (may be empty).
    """
    log.info("Fetching Kalshi markets…")

    if not cfg.kalshi_key_id or (not cfg.kalshi_private_key and not cfg.kalshi_private_key_path):
        log.error("Kalshi credentials not set — check KALSHI_KEY_ID and key path")
        return []

    async with aiohttp.ClientSession() as session:
        seen_kalshi_ids = await get_seen_kalshi_ids(cfg.db_path)
        raw_categorized_markets = await fetch_all_kalshi_markets(session)

    if sum(len(markets) for markets in raw_categorized_markets.values()) == 0:
        log.warning("Kalshi returned no open markets")
        return []

    candidates = []
    for category, markets in raw_categorized_markets.items():
        for raw in markets:
            market = _normalise_market(raw, category)
            if market is None:
                continue
            candidates.append(market)

    if not candidates:
        log.info("Kalshi: 0 new markets found")
        return []

    log.info(f"Adding {len(candidates)} new Kalshi markets…")

    async with _get_conn(cfg.db_path) as db:
        for market in candidates:
            await db.execute("""
                INSERT INTO kalshi_markets
                    (market_id, question, category, end_date, status)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    status=excluded.status,
                    end_date=excluded.end_date
            """, (market["market_id"], market["question"], market["category"], market["end_date"], market["status"]))
            if market["market_id"] not in seen_kalshi_ids:
                await db.execute(
                    "INSERT OR IGNORE INTO kalshi_unmatched (market_id) VALUES (?)",
                    (market["market_id"],)
                )
        await db.commit()

    log.info(f"Kalshi: {len(candidates)} new markets added.")
    return list(candidates)