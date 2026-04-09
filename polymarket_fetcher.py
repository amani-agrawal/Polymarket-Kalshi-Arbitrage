"""
polymarket_fetcher.py — Fetches active Polymarket markets using the Polymarket API only.

The Polymarket API (https://gamma-api.polymarket.com) is the primary discovery API
and provides everything needed for market ingestion:
  - market_id (conditionId)
  - question / title
  - category / tags
  - endDate
  - clobTokenIds (YES and NO token IDs for later orderbook/trading use)
  - outcomePrices (ballpark prices — precise prices fetched live by arb_monitor)
  - liquidity / volume

The CLOB API is NOT used here. It is only needed by:
  - arb_monitor.py  → live orderbook prices via /book?token_id=...
  - trade_executor.py → order placement
"""

import asyncio
from curses import raw
import json
from typing import Optional
from collections import defaultdict

import aiohttp

from config import cfg
from db import (
    _get_conn,
    get_seen_poly_ids,
    upsert_poly_market,
    add_poly_unmatched,
)
from logger import get_logger

log = get_logger(__name__)

Polymarket_BASE = "https://gamma-api.polymarket.com"
PAGE_SIZE = 100



# ---------------------------------------------------------------------------
# Sports game-winner filter
# ---------------------------------------------------------------------------

POLY_SPORT_TAGS_NOT_USING = {
    "nba":  "745",     # NBA
    "mlb":  "100381",  # MLB
    "nhl":  "899",     # NHL
    "laliga":  "780",     # La Liga
    "epl":  "82",      # EPL
    "ucl":  "100977",  # UCL / Champions League
    "serieA":  "101962",  # Serie A
    "ligue1":  "102070",  # Ligue 1
}

POLY_SPORT_TAGS = {
    "ucl":  "100977",  # UCL / Champions League
}

# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

async def _get_json(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[dict] = None,
) -> dict | list:
    """
    Perform an async GET and return parsed JSON.

    Args:
        session: Active aiohttp ClientSession.
        url: Full request URL.
        params: Optional query parameters.

    Returns:
        Parsed JSON as dict or list.

    Raises:
        aiohttp.ClientResponseError: On non-2xx response.
    """
    async with session.get(url, params=params) as resp:
        resp.raise_for_status()
        return await resp.json()


# ---------------------------------------------------------------------------
# Polymarket market fetching
# ---------------------------------------------------------------------------

async def fetch_poly_markets(session: aiohttp.ClientSession) -> list[dict]:
    """
    Paginate through GET /markets on the Polymarket API and return all active markets.

    Uses limit/offset pagination. Stops when a page returns fewer items than
    the page size.

    Args:
        session: Active aiohttp session.

    Returns:
        Flat list of raw Polymarket market dicts.
    """
    categorized_markets = defaultdict(list)
    markets_count = 0

    for sport, curr_tag in POLY_SPORT_TAGS.items():
        log.debug(f"Fetching Polymarket markets for sport={sport} (tag_id={curr_tag})…")
        offset = 0

        while True:
            params = {"closed": "false", "limit": PAGE_SIZE, "offset": offset, "tag_id": curr_tag}
            params["sports_market_types"] = "moneyline"

            try:
                page = await _get_json(session, f"{Polymarket_BASE}/markets", params)
            except Exception as exc:
                log.error(f"Polymarket /markets error (offset={offset}): {exc}")
                break

            if not page:
                break

            categorized_markets[sport].extend(page)
            markets_count+= len(page)
            log.debug(f"Polymarket: fetched {markets_count} markets so far (offset={offset})")

            if len(page) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

    log.info(f"Polymarket: {markets_count} total active markets fetched")
    return categorized_markets


# ---------------------------------------------------------------------------
# Market normalisation
# ---------------------------------------------------------------------------

def _normalise_market(raw, category) -> Optional[dict]:
    """
    Convert a raw Polymarket market dict into our internal schema.

    Extracts YES/NO token IDs from clobTokenIds (index 0 = YES, index 1 = NO).
    Extracts ballpark prices from outcomePrices (same ordering).

    Args:
        raw: Raw market dict from the Polymarket API.

    Returns:
        Dict conforming to the poly_markets table schema, or None if the
        market is missing a conditionId (unusable).
    """
    market_id = raw.get("conditionId")

    if not market_id:
        return None
    
    outcomes = json.loads(raw.get("outcomes", "[]"))
    clob_ids = json.loads(raw.get("clobTokenIds", "[]"))

    if outcomes and outcomes[0].lower() == "yes":
        yes_token_id = clob_ids[0] if clob_ids else None
        no_token_id  = clob_ids[1] if len(clob_ids) > 1 else None
    else:
        yes_token_id = clob_ids[1] if len(clob_ids) > 1 else None
        no_token_id  = clob_ids[0] if clob_ids else None
    return {
        "market_id": market_id,
        "question": raw.get("question", ""),
        "category": category,
        "end_date": raw.get("endDate", ""),
        "active": bool(raw.get("active", not raw.get("closed", False))),
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def fetch_new_poly_markets() -> list[dict]:
    """
    Poll the Polymarket API for active Polymarket markets and persist new ones to arb.db.

    Steps:
      1. Load already-seen market IDs from arb.db.
      2. Paginate through Polymarket /markets (active=true, closed=false).
      3. Normalise each market — skip if already seen or missing conditionId.
      4. Persist all new markets to poly_markets + poly_unmatched concurrently.
      5. Return the list of new market dicts.

    Orderbook prices are NOT fetched here — arb_monitor fetches live prices
    from the CLOB for matched pairs only, every MONITOR_INTERVAL_SECONDS.

    Returns:
        List of newly discovered market dicts (may be empty if nothing new).
    """
    log.info("Fetching Polymarket markets via Polymarket API…")

    async with aiohttp.ClientSession() as session:
        seen_poly_ids = await get_seen_poly_ids(cfg.db_path)
        raw_categorized_markets = await fetch_poly_markets(session)

    if sum(len(markets) for markets in raw_categorized_markets.values()) == 0:
        log.warning("Polymarket API returned no markets")
        return []

    candidates = []
    for category, markets in raw_categorized_markets.items():
        for raw in markets:
            market = _normalise_market(raw, category)
            if market is None:
                continue
            candidates.append(market)
    
    if not candidates:
        log.info("Polymarket: 0 new markets found")
        return []

    log.info(f"Adding {len(candidates)} new Polymarket markets…")

    # Bulk insert — one connection, one transaction, one commit.
    async with _get_conn(cfg.db_path) as db:
        for market in candidates:
            await db.execute("""
                INSERT INTO poly_markets
                    (market_id, question, category, end_date, active, yes_token_id, no_token_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    active=excluded.active
            """, (
                market["market_id"],
                market.get("question", ""),
                market.get("category", ""),
                market.get("end_date", ""),
                int(market.get("active", True)),
                market.get("yes_token_id"),
                market.get("no_token_id")
            ))
            if market["market_id"] not in seen_poly_ids:
                await db.execute(
                    "INSERT OR IGNORE INTO poly_unmatched (market_id) VALUES (?)",
                    (market["market_id"],)
                )
        await db.commit()

    log.info(f"Polymarket: {len(candidates)} new markets found")
    return candidates