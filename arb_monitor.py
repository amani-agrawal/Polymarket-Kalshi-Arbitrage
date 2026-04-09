"""
arb_monitor.py — Monitors matched pairs and executes arbitrage immediately.

Critical path (no DB writes):
  1. Fetch both orderbooks concurrently
  2. Detect spread
  3. If actionable → fire both trade legs concurrently
  4. DB writes happen AFTER execution (non-blocking on the arb itself)

Kalshi orderbook note:
  Returns yes/no BIDS only. Asks are derived:
    YES ask = 1.0 - best_no_bid
    NO  ask = 1.0 - best_yes_bid
  Arrays are sorted lowest→highest, so [-1] is the best (highest) bid.

Opportunity detection checks both directions:
  A: BUY YES on Polymarket + BUY NO on Kalshi
  B: BUY NO on Polymarket + BUY YES on Kalshi
"""

import asyncio
from typing import Optional

import aiohttp

from config import cfg
from db import (
    get_active_matched_pairs,
    insert_opportunity,
    insert_trade,
    mark_opportunity_executed,
    get_flag,
    set_flag,
)
from kalshi_fetcher import build_auth_headers
from logger import get_logger

log = get_logger(__name__)

CLOB_BASE = "https://clob.polymarket.com"
KALSHI_BASE = "https://api.elections.kalshi.com"


# ---------------------------------------------------------------------------
# Orderbook fetchers
# ---------------------------------------------------------------------------

async def _fetch_poly_side(
    session: aiohttp.ClientSession, token_id: str
) -> tuple[Optional[float], float]:
    """Fetch best ask price and size for one Polymarket token."""
    try:
        async with session.get(
            f"{CLOB_BASE}/book", params={"token_id": token_id}
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        asks = data.get("asks", [])
        if not asks:
            return None, 0.0
        best = asks[-1]
        return round(float(best["price"]), 4), float(best.get("size", 0))
    except Exception as exc:
        log.debug(f"Poly orderbook error {token_id[:16]}...: {exc}")
        return None, 0.0


async def fetch_poly_orderbook(
    session: aiohttp.ClientSession,
    yes_token_id: str,
    no_token_id: str,
) -> dict:
    """
    Fetch best ask prices for YES and NO tokens from Polymarket CLOB.

    Returns:
        Dict with yes_best_ask, no_best_ask, yes_ask_volume, no_ask_volume.
    """
    (yes_ask, yes_vol), (no_ask, no_vol) = await asyncio.gather(
        _fetch_poly_side(session, yes_token_id),
        _fetch_poly_side(session, no_token_id),
    )
    return {
        "yes_best_ask": yes_ask,
        "no_best_ask": no_ask,
        "yes_ask_volume": yes_vol,
        "no_ask_volume": no_vol,
    }


async def fetch_kalshi_orderbook(
    session: aiohttp.ClientSession, ticker: str
) -> dict:
    """
    Fetch Kalshi orderbook. Returns bids only — asks are derived.

    yes_ask = 1.0 - best_no_bid
    no_ask  = 1.0 - best_yes_bid

    Arrays sorted lowest to highest so [-1] is best bid.

    Returns:
        Dict with yes_best_ask, no_best_ask, yes_ask_volume, no_ask_volume.
    """
    result = {
        "yes_best_ask": None, "no_best_ask": None,
        "yes_ask_volume": 0.0, "no_ask_volume": 0.0,
    }

    path = f"/trade-api/v2/markets/{ticker}/orderbook"
    headers = build_auth_headers("GET", path)
    url = f"{KALSHI_BASE}{path}"

    try:
        async with session.get(url, headers=headers, params={"depth": 5}) as resp:
            resp.raise_for_status()
            data = await resp.json()

        ob = data.get("orderbook_fp", data.get("orderbook", data))
        yes_bids = ob.get("yes_dollars", ob.get("yes", []))
        no_bids  = ob.get("no_dollars",  ob.get("no",  []))

        def _best(bids: list) -> tuple[Optional[float], float]:
            if not bids:
                return None, 0.0
            best = bids[-1]  # highest bid is last element
            if isinstance(best, (list, tuple)):
                price, qty = float(best[0]), float(best[1])
                # yes_dollars is already in dollars (e.g. 0.42)
                # legacy yes/no arrays are in cents (e.g. 42)
                if price > 1.0:
                    price = price / 100.0
            else:
                price = float(best.get("price", 0)) / 100.0
                qty = float(best.get("count", 0))
            return round(price, 4), qty

        best_yes_bid, yes_vol = _best(yes_bids)
        best_no_bid,  no_vol  = _best(no_bids)

        if best_no_bid is not None:
            result["yes_best_ask"]   = round(1.0 - best_no_bid, 4)
            result["yes_ask_volume"] = no_vol
        if best_yes_bid is not None:
            result["no_best_ask"]   = round(1.0 - best_yes_bid, 4)
            result["no_ask_volume"] = yes_vol

    except Exception as exc:
        log.debug(f"Kalshi orderbook error {ticker}: {exc}")

    return result


# ---------------------------------------------------------------------------
# Bet sizing
# ---------------------------------------------------------------------------

def _calculate_size(
    yes_ask_volume: float,
    no_ask_volume: float,
    yes_price: float,
    no_price: float,
) -> tuple[float, float]:
    """
    Size the trade using orderbook_vol_fraction of the minimum available depth.

    Returns:
        (size_usd, expected_profit_usd) — both 0.0 if below minimums.
    """
    slippage_safe = min(yes_ask_volume * cfg.orderbook_vol_fraction, no_ask_volume  * cfg.orderbook_vol_fraction,)
    size   = round(min(slippage_safe, cfg.max_bet_size_usd), 2)
    profit = round(size * (1.0 - yes_price - no_price), 4)

    if profit < cfg.min_profit_abs:
        return 0.0, 0.0
    return size, profit


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

async def _place_poly_order(
    session: aiohttp.ClientSession,
    side: str,
    token_id: str,
    price: float,
    size: float,
) -> dict:
    """Place a BUY order on Polymarket CLOB."""
    if cfg.dry_run:
        log.info(f"[DRY_RUN] Poly {side} token={token_id[:16]}... price={price} size=${size}")
        return {"status": "dry_run", "order_id": "DRY", "tx_hash": "DRY"}

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs

        client = ClobClient(
            host=CLOB_BASE,
            key=cfg.wallet_private_key,
            chain_id=137,
            signature_type=0,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        resp = client.create_and_post_order(
            OrderArgs(token_id=token_id, price=price, size=size, side=side)
        )
        order_id = resp.get("orderID", "")
        log.info(f"Poly {side} placed: ${size} @ {price} | id={order_id}")
        return {
            "status": resp.get("status", "open"),
            "order_id": order_id,
            "tx_hash": resp.get("transactionHash", ""),
        }
    except Exception as exc:
        log.error(f"Poly order failed: {exc}")
        raise


async def _place_kalshi_order(
    session: aiohttp.ClientSession,
    side: str,
    ticker: str,
    price: float,
    size_usd: float,
) -> dict:
    """Place a BUY order on Kalshi via raw HTTP."""
    if cfg.dry_run:
        log.info(f"[DRY_RUN] Kalshi {side.upper()} ticker={ticker} price={price} size=${size_usd}")
        return {"status": "dry_run", "order_id": "DRY"}

    price_cents = round(price * 100)
    if price_cents <= 0:
        raise ValueError(f"Invalid price_cents={price_cents}")

    count = max(1, int(size_usd / (price_cents / 100)))
    path  = "/trade-api/v2/portfolio/orders"
    headers = build_auth_headers("POST", path)
    url = f"{KALSHI_BASE}{path}"

    payload = {
        "ticker":   ticker,
        "action":   "buy",
        "side":     side.lower(),
        "type":     "limit",
        "count":    count,
        f"{side.lower()}_price": price_cents,
        "client_order_id": __import__("uuid").uuid4().hex,
    }

    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()

        order  = data.get("order", data)
        status = str(order.get("status", "open")).lower()
        log.info(
            f"Kalshi {side.upper()} placed: {count} contracts @ {price_cents}c "
            f"| ticker={ticker} id={order.get('order_id', '')}"
        )
        return {"status": status, "order_id": order.get("order_id", "")}
    except Exception as exc:
        log.error(f"Kalshi order failed: {exc}")
        raise


# ---------------------------------------------------------------------------
# Unwind helpers (partial fill recovery)
# ---------------------------------------------------------------------------

async def _unwind_poly(session, side, token_id, price, size):
    unwind = "SELL" if side == "BUY" else "BUY"
    for attempt in range(1, 4):
        try:
            await _place_poly_order(session, unwind, token_id, price, size)
            log.warning(f"Poly unwind {unwind} ok (attempt {attempt})")
            return True
        except Exception as exc:
            log.error(f"Poly unwind attempt {attempt} failed: {exc}")
            if attempt < 3:
                await asyncio.sleep(1)
    return False


async def _unwind_kalshi(session, side, ticker, price, size):
    unwind = "no" if side == "yes" else "yes"
    for attempt in range(1, 4):
        try:
            await _place_kalshi_order(session, unwind, ticker, round(1.0 - price, 4), size)
            log.warning(f"Kalshi unwind {unwind.upper()} ok (attempt {attempt})")
            return True
        except Exception as exc:
            log.error(f"Kalshi unwind attempt {attempt} failed: {exc}")
            if attempt < 3:
                await asyncio.sleep(1)
    return False


# ---------------------------------------------------------------------------
# Core: detect + execute immediately, DB writes after
# ---------------------------------------------------------------------------

async def _process_pair(session: aiohttp.ClientSession, pair: dict) -> None:
    """
    For one matched pair:
      1. Fetch both orderbooks concurrently
      2. Detect arb in both directions
      3. Execute immediately if actionable
      4. Write to DB only after execution
    """
    poly_book, kalshi_book = await asyncio.gather(
        fetch_poly_orderbook(session, pair.get("yes_token_id", ""), pair.get("no_token_id", ""), ),
        fetch_kalshi_orderbook(session, pair["kalshi_market_id"]),
    )

    poly_yes   = poly_book.get("yes_best_ask")
    poly_no    = poly_book.get("no_best_ask")
    kalshi_yes = kalshi_book.get("yes_best_ask")
    kalshi_no  = kalshi_book.get("no_best_ask")

    candidates = []

    # Direction A: YES on Poly, NO on Kalshi
    if poly_yes is not None and kalshi_no is not None:
        spread = round(1.0 - poly_yes - kalshi_no, 4)
        if spread >= cfg.min_profit_threshold:
            candidates.append({
                "direction":        "A",
                "yes_price":        poly_yes,
                "no_price":         kalshi_no,
                "spread":           spread,
                "yes_platform":     "polymarket",
                "no_platform":      "kalshi",
                "yes_ask_volume":   poly_book.get("yes_ask_volume", 0),
                "no_ask_volume":    kalshi_book.get("no_ask_volume", 0),
                "yes_token_id":     pair.get("yes_token_id", ""),
                "no_token_id":      pair.get("no_token_id", ""),
                "poly_market_id":   pair["poly_market_id"],
                "kalshi_market_id": pair["kalshi_market_id"],
                "pair_id":          pair["id"],
            })

    # Direction B: NO on Poly, YES on Kalshi
    if poly_no is not None and kalshi_yes is not None:
        spread = round(1.0 - poly_no - kalshi_yes, 4)
        if spread >= cfg.min_profit_threshold:
            candidates.append({
                "direction":        "B",
                "yes_price":        kalshi_yes,
                "no_price":         poly_no,
                "spread":           spread,
                "yes_platform":     "kalshi",
                "no_platform":      "polymarket",
                "yes_ask_volume":   kalshi_book.get("yes_ask_volume", 0),
                "no_ask_volume":    poly_book.get("no_ask_volume", 0),
                "yes_token_id":     pair.get("yes_token_id", ""),
                "no_token_id":      pair.get("no_token_id", ""),
                "poly_market_id":   pair["poly_market_id"],
                "kalshi_market_id": pair["kalshi_market_id"],
                "pair_id":          pair["id"],
            })

    if not candidates:
        log.debug(f"No arbitrage for pair_id={pair['id']} | Poly YES={poly_yes} NO={poly_no} | Kalshi YES={kalshi_yes} NO={kalshi_no}")
        return

    # Take the best spread candidate
    opp = max(candidates, key=lambda x: x["spread"])

    log.info(f"Arb found | pair={opp['pair_id']} dir={opp['direction']} spread={opp['spread']:.4f} | {opp['yes_platform']} YES @ {opp['yes_price']} + {opp['no_platform']} NO @ {opp['no_price']}"
    )

    # Size the trade
    size, expected_profit = _calculate_size(
        opp["yes_ask_volume"], opp["no_ask_volume"],
        opp["yes_price"],      opp["no_price"],
    )
    if size == 0.0:
        log.info(f"Below min profit after sizing — skipping pair={opp['pair_id']}")
        return

    # Resolve legs
    direction = opp["direction"]
    if direction == "A":
        poly_side   = "BUY"
        poly_token  = opp["yes_token_id"]
        poly_price  = opp["yes_price"]
        kalshi_side = "no"
        kalshi_price = opp["no_price"]
    else:
        poly_side   = "BUY"
        poly_token  = opp["no_token_id"]
        poly_price  = opp["no_price"]
        kalshi_side = "yes"
        kalshi_price = opp["yes_price"]

    kalshi_ticker = opp["kalshi_market_id"]

    log.info(f"Executing | dir={direction} size=${size} expected_profit=${expected_profit}")

    # DRY RUN path
    if cfg.dry_run:
        log.info(
            f"[DRY_RUN] Poly {poly_side} ${size} @ {poly_price} | "
            f"Kalshi {kalshi_side.upper()} ${size} @ {kalshi_price}"
        )
        opp_id = await insert_opportunity(cfg.db_path, {
            "pair_id":         opp["pair_id"],
            "direction":       direction,
            "yes_price":       opp["yes_price"],
            "no_price":        opp["no_price"],
            "spread":          opp["spread"],
            "calculated_size": size,
            "executed":        True,
        })
        await mark_opportunity_executed(cfg.db_path, opp_id)
        return

    # LIVE EXECUTION — both legs fire simultaneously
    results = await asyncio.gather(
        _place_poly_order(session, poly_side, poly_token, poly_price, size),
        _place_kalshi_order(session, kalshi_side, kalshi_ticker, kalshi_price, size),
        return_exceptions=True,
    )
    poly_result, kalshi_result = results
    poly_ok   = not isinstance(poly_result, Exception)
    kalshi_ok = not isinstance(kalshi_result, Exception)

    if poly_ok and kalshi_ok:
        # Both legs filled — write to DB
        opp_id = await insert_opportunity(cfg.db_path, {
            "pair_id":         opp["pair_id"],
            "direction":       direction,
            "yes_price":       opp["yes_price"],
            "no_price":        opp["no_price"],
            "spread":          opp["spread"],
            "calculated_size": size,
            "executed":        True,
        })
        await asyncio.gather(
            insert_trade(cfg.db_path, {
                "opportunity_id": opp_id, "platform": "polymarket",
                "side": poly_side, "price": poly_price, "size": size,
                "tx_hash": poly_result.get("tx_hash", ""),
                "status":  poly_result.get("status", "unknown"),
            }),
            insert_trade(cfg.db_path, {
                "opportunity_id": opp_id, "platform": "kalshi",
                "side": kalshi_side.upper(), "price": kalshi_price, "size": size,
                "tx_hash": "",
                "status":  kalshi_result.get("status", "unknown"),
            }),
        )
        await mark_opportunity_executed(cfg.db_path, opp_id)
        log.info(f"Executed opp={opp_id} profit=${expected_profit:.4f}")
        return

    # Partial fill — unwind
    unwind_ok = True
    if poly_ok and not kalshi_ok:
        log.error(f"Kalshi leg FAILED ({kalshi_result}). Unwinding Poly...")
        unwind_ok = await _unwind_poly(session, poly_side, poly_token, poly_price, size)
    elif kalshi_ok and not poly_ok:
        log.error(f"Poly leg FAILED ({poly_result}). Unwinding Kalshi...")
        unwind_ok = await _unwind_kalshi(session, kalshi_side, kalshi_ticker, kalshi_price, size)
    else:
        log.error("BOTH legs failed — nothing to unwind")

    if not unwind_ok:
        log.critical("CRITICAL: Unwind FAILED after 3 retries. One-sided position may be open. TRADING HALTED. Manually inspect and clear 'trading_halted' in arb.db.")
        await set_flag(cfg.db_path, "trading_halted", "true")


# ---------------------------------------------------------------------------
# Main sweep
# ---------------------------------------------------------------------------

async def run_arb_monitor() -> None:
    """
    One monitoring sweep across all active matched pairs.

    All pairs are scanned concurrently. For each pair:
    orderbooks are fetched, arb is detected, and trades fire immediately.
    DB writes happen only after execution.
    """
    halt = await get_flag(cfg.db_path, "trading_halted")
    if halt == "true":
        log.warning("Trading HALTED. Clear 'trading_halted' in arb.db to resume.")
        return

    pairs = await get_active_matched_pairs(cfg.db_path)
    if not pairs:
        log.debug("Arb monitor: no active matched pairs")
        return

    log.debug(f"Arb monitor: scanning {len(pairs)} pairs...")

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[_process_pair(session, p) for p in pairs])