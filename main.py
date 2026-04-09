"""
main.py — Daemon entry point.

Startup sequence:
  1. Load config, configure logging.
  2. Initialise arb.db.
  3. Start two concurrent long-running tasks:
       a. fetch_and_match_loop — runs immediately then every 24 hours
       b. run_ws_monitor      — persistent WebSocket connections to both platforms
  5. Block until SIGTERM/SIGINT.
"""

import asyncio
import signal
import sys
from datetime import datetime

import aiohttp

from config import cfg
from db import (
    init_db,
    get_all_poly_markets,
    get_all_kalshi_markets,
    deactivate_poly_market,
    deactivate_kalshi_market,
    deactivate_matched_pair,
    get_active_matched_pairs,
)
from logger import setup_logging, get_logger
from polymarket_fetcher import fetch_new_poly_markets
from kalshi_fetcher import fetch_new_kalshi_markets, build_auth_headers
from market_matcher import run_market_matcher
from ws_monitor import run_ws_monitor, refresh_pairs

log = get_logger(__name__)

_shutdown_event = asyncio.Event()

GAMMA_BASE = "https://gamma-api.polymarket.com"


# ---------------------------------------------------------------------------
# Cleanup routine
# ---------------------------------------------------------------------------

async def run_cleanup() -> None:
    """
    Scan all markets and deactivate stale or closed ones.

    Checks end_date expiry and live API status for both platforms.
    Soft-deletes only (active=0 / status='closed') to preserve PnL history.
    Also deactivates matched pairs where either leg is now inactive.
    """
    log.info("Running cleanup cycle...")
    now = datetime.utcnow()
    poly_deactivated = 0
    kalshi_deactivated = 0
    pairs_deactivated = 0

    async with aiohttp.ClientSession() as session:

        # Polymarket cleanup
        for market in await get_all_poly_markets(cfg.db_path):
            deactivate = False

            if market.get("end_date"):
                try:
                    end_dt = datetime.fromisoformat(
                        market["end_date"].replace("Z", "").split("+")[0]
                    )
                    if end_dt < now:
                        deactivate = True
                except ValueError:
                    pass

            if not deactivate:
                try:
                    url = f"{GAMMA_BASE}/markets/{market['market_id']}"
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if not data.get("active", True) or data.get("closed", False):
                                deactivate = True
                        elif resp.status == 404:
                            deactivate = True
                except Exception:
                    pass

            if deactivate:
                await deactivate_poly_market(cfg.db_path, market["market_id"])
                poly_deactivated += 1

        # Kalshi cleanup
        for market in await get_all_kalshi_markets(cfg.db_path):
            deactivate = False

            if market.get("end_date"):
                try:
                    end_dt = datetime.fromisoformat(
                        market["end_date"].replace("Z", "").split("+")[0]
                    )
                    if end_dt < now:
                        deactivate = True
                except ValueError:
                    pass

            if not deactivate:
                try:
                    path = f"/trade-api/v2/markets/{market['market_id']}"
                    headers = build_auth_headers("GET", path)
                    url = f"https://api.elections.kalshi.com{path}"
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            m = data.get("market", data)
                            if str(m.get("status", "open")).lower() not in ("open", "active"):
                                deactivate = True
                        elif resp.status == 404:
                            deactivate = True
                except Exception:
                    pass

            if deactivate:
                await deactivate_kalshi_market(cfg.db_path, market["market_id"])
                kalshi_deactivated += 1

        # Matched pairs cleanup
        for pair in await get_active_matched_pairs(cfg.db_path):
            poly_dead = pair.get("active") == 0
            kalshi_dead = str(pair.get("status", "open")).lower() not in ("open", "active")
            if poly_dead or kalshi_dead:
                await deactivate_matched_pair(cfg.db_path, pair["id"])
                pairs_deactivated += 1

    log.info(
        f"Cleanup done: {poly_deactivated} poly, {kalshi_deactivated} kalshi, "
        f"{pairs_deactivated} pairs deactivated"
    )


# ---------------------------------------------------------------------------
# Fetch & match loop (runs every 24 hours)
# ---------------------------------------------------------------------------

async def fetch_and_match_loop() -> None:
    """
    Runs one fetch+match+cleanup cycle immediately on startup,
    then repeats every 24 hours forever.

    After each match cycle, calls refresh_pairs() so the WS monitor
    subscribes to any newly matched markets without restarting.
    """
    try:
        log.info("Starting fetch & match cycle...")

        poly_new, kalshi_new = await asyncio.gather(
            fetch_new_poly_markets(),
            fetch_new_kalshi_markets(),
        )
        log.info(f"Fetched {len(poly_new)} new poly markets, {len(kalshi_new)} new kalshi markets")

        if poly_new or kalshi_new:
            summary = await run_market_matcher()
            log.info(f"Match summary: {summary}")
        else:
            log.info("No new markets — skipping matcher")

        await run_cleanup()

        # Update WS monitor lookup maps with any newly matched pairs
        await refresh_pairs()

        log.info("Fetch & match cycle complete.")

    except Exception as exc:
        log.exception(f"Fetch & match cycle error: {exc}")


# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------

def _handle_signal(sig: int) -> None:
    log.info(f"Signal {sig} received — shutting down...")
    _shutdown_event.set()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    """Initialise and run the arbitrage daemon."""
    setup_logging(cfg.log_level)

    log.info("=" * 60)
    log.info("  Polymarket x Kalshi Arbitrage Daemon starting up")
    log.info("=" * 60)
    log.info(f"  Poly wallet:     {cfg.wallet_address}")
    log.info(f"  Kalshi key ID:   {cfg.kalshi_key_id}")
    log.info(f"  DB:              {cfg.db_path}")
    log.info(f"  Min profit:      {cfg.min_profit_threshold * 100:.1f}% / ${cfg.min_profit_abs}")
    log.info(f"  Max bet size:    ${cfg.max_bet_size_usd}")
    log.info(f"  DRY RUN:         {'YES' if cfg.dry_run else 'NO (LIVE)'}")
    log.info("=" * 60)

    if cfg.dry_run:
        log.warning("DRY_RUN=true — no real orders will be placed")

    await init_db(cfg.db_path)

    # Register signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: _handle_signal(s))

    # Run both tasks concurrently forever:
    #   - fetch_and_match_loop: daily market discovery + matching
    #   - run_ws_monitor: real-time orderbook + arb execution via WebSockets
    try:
        await fetch_and_match_loop()
        await run_ws_monitor()
    except asyncio.CancelledError:
        pass

    log.info("Daemon shut down cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — daemon stopped")
        sys.exit(0)