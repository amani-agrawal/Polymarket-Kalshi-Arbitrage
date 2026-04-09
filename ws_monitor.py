"""
ws_monitor.py — Real-time arbitrage monitor using WebSockets.

Replaces the polling-based arb_monitor.py with persistent WebSocket
connections to both Polymarket and Kalshi. Arb detection runs on every
orderbook update — no scheduler needed.

Architecture:
  - One Polymarket WS connection subscribing to all YES/NO token IDs
  - One Kalshi WS connection subscribing to all matched market tickers
  - Shared in-memory orderbook state dict updated by both connections
  - On every update: check spread for the affected pair, execute if actionable

Kalshi WebSocket:
  wss://api.elections.kalshi.com/trade-api/v2/ws
  Auth: RSA-PSS headers during handshake
  Receives: orderbook_snapshot (full state) then orderbook_delta (increments)
  You maintain local state and apply deltas.

Polymarket WebSocket:
  wss://ws-subscriptions-clob.polymarket.com/ws/market
  No auth needed.
  Receives: book (snapshot), price_change (deltas), best_bid_ask (summary)
  Subscribe by asset_id (token ID).
"""

import asyncio
import json
import time
from typing import Optional

import aiohttp
import websockets

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

POLY_WS_URL    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
KALSHI_BASE    = "https://api.elections.kalshi.com"
CLOB_BASE      = "https://clob.polymarket.com"

# ---------------------------------------------------------------------------
# Shared orderbook state
# ---------------------------------------------------------------------------

# poly_books: { token_id → {"best_ask": float, "best_ask_volume": float} }
poly_books = {}

# kalshi_books: { ticker → {"yes_dollars": [[price, qty],...], "no_dollars": [...]} }
# Stored as bid arrays (sorted low→high), derived asks computed on demand
kalshi_books = {}

# pair lookup maps — built from DB at startup
# token_id → pair dict
poly_token_to_pair = {}

# kalshi ticker → pair dict
kalshi_ticker_to_pair = {}

_last_executed: dict[str, float] = {}  # pair_key → timestamp
COOLDOWN_SECONDS = 30  # don't re-execute same pair for 30s

async def _init_orderbooks(pairs: list[dict], session: aiohttp.ClientSession) -> None:
    """
    Pre-populate poly_books and kalshi_books via REST before WS connects.
    This ensures we have valid prices immediately rather than waiting for
    the first WS snapshot.
    """
    log.info(f"Initialising orderbooks via REST for {len(pairs)} pairs...")

    async def _init_poly_side(token_id: str):
        try:
            async with session.get(
                f"{CLOB_BASE}/book", params={"token_id": token_id}
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
            asks = data.get("asks", [])
            if asks:
                poly_books[token_id] = {
                    "best_ask": round(float(asks[0]["price"]), 4),
                    "best_ask_volume": float(asks[0].get("size", 0)),
                }
        except Exception as exc:
            log.debug(f"REST init poly {token_id[:16]}...: {exc}")

    async def _init_kalshi(ticker: str):
        try:
            path = f"/trade-api/v2/markets/{ticker}/orderbook"
            headers = build_auth_headers("GET", path)
            async with session.get(
                f"{KALSHI_BASE}{path}", headers=headers, params={"depth": 10}
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
            ob = data.get("orderbook_fp", data.get("orderbook", {}))
            kalshi_books[ticker] = {
                "yes_dollars": sorted(
                    ob.get("yes_dollars", []), key=lambda x: float(x[0])
                ),
                "no_dollars": sorted(
                    ob.get("no_dollars", []), key=lambda x: float(x[0])
                ),
            }
        except Exception as exc:
            log.debug(f"REST init kalshi {ticker}: {exc}")

    # Fire all REST calls concurrently
    tasks = []
    for pair in pairs:
        tasks.append(_init_poly_side(pair.get("yes_token_id", "")))
        tasks.append(_init_poly_side(pair.get("no_token_id", "")))
        tasks.append(_init_kalshi(pair["kalshi_market_id"]))

    await asyncio.gather(*tasks)
    log.info(f"REST initialization done — {len(poly_books)} poly tokens, {len(kalshi_books)} kalshi markets pre-loaded")

async def refresh_pairs():
    pairs = await get_active_matched_pairs(cfg.db_path)
    for pair in pairs:
        yes_id = pair.get("yes_token_id", "")
        no_id  = pair.get("no_token_id", "")
        ticker = pair["kalshi_market_id"]

        if yes_id and yes_id not in poly_token_to_pair:
            poly_token_to_pair[yes_id] = pair   # new token added

        if no_id and no_id not in poly_token_to_pair:
            poly_token_to_pair[no_id] = pair

        if ticker not in kalshi_ticker_to_pair:
            kalshi_ticker_to_pair[ticker] = pair  # new ticker added

# ---------------------------------------------------------------------------
# Orderbook helpers
# ---------------------------------------------------------------------------

def _kalshi_best_ask(ticker: str, side: str) -> tuple[Optional[float], float]:
    """
    Derive best ask and volume for a Kalshi side from local bid state.

    yes_ask = 1.0 - best_no_bid
    no_ask  = 1.0 - best_yes_bid

    Args:
        ticker: Kalshi market ticker.
        side: "yes" or "no".

    Returns:
        (best_ask_price, volume_at_best) — (None, 0.0) if no data.
    """
    book = kalshi_books.get(ticker, {})
    if side == "yes":
        no_bids = book.get("no_dollars", [])
        if not no_bids:
            return None, 0.0
        best_no_bid = float(no_bids[-1][0])
        qty = float(no_bids[-1][1])
        return round(1.0 - best_no_bid, 4), qty
    else:
        yes_bids = book.get("yes_dollars", [])
        if not yes_bids:
            return None, 0.0
        best_yes_bid = float(yes_bids[-1][0])
        qty = float(yes_bids[-1][1])
        return round(1.0 - best_yes_bid, 4), qty


def _apply_kalshi_delta(ticker: str, side: str, price_dollars: str, delta_fp: str):
    """
    Apply an incremental orderbook delta to local Kalshi state.

    A delta_fp of "0" means remove the price level entirely.
    Maintains sorted order (lowest→highest) after each update.

    Args:
        ticker: Kalshi market ticker.
        side: "yes" or "no".
        price_dollars: Price as dollar string e.g. "0.4200".
        delta_fp: Change in contracts as fixed-point string.
    """
    if ticker not in kalshi_books:
        kalshi_books[ticker] = {"yes_dollars": [], "no_dollars": []}

    key = f"{side}_dollars"
    bids = kalshi_books[ticker][key]
    price = price_dollars
    delta = float(delta_fp)

    # Find existing level
    for i, (p, q) in enumerate(bids):
        if p == price:
            new_qty = float(q) + delta
            if new_qty <= 0:
                bids.pop(i)  # remove level
            else:
                bids[i] = [price, f"{new_qty:.2f}"]
            kalshi_books[ticker][key] = sorted(bids, key=lambda x: float(x[0]))
            return

    # New price level
    if delta > 0:
        bids.append([price, f"{delta:.2f}"])
        kalshi_books[ticker][key] = sorted(bids, key=lambda x: float(x[0]))


# ---------------------------------------------------------------------------
# Arb detection + execution (called on every orderbook update)
# ---------------------------------------------------------------------------

async def _check_and_execute(pair: dict, session: aiohttp.ClientSession):
    """
    Check spread for one pair and execute immediately if actionable.
    Called on every orderbook update for the affected pair.
    """
    pair_key = str(pair["id"])
    if time.time() - _last_executed.get(pair_key, 0) < COOLDOWN_SECONDS:
        return  # already firing for this pair
    else:
        _last_executed[pair_key] = time.time()

    yes_token = pair.get("yes_token_id", "")
    no_token  = pair.get("no_token_id", "")
    ticker    = pair["kalshi_market_id"]

    poly_yes_book = poly_books.get(yes_token, {})
    poly_no_book  = poly_books.get(no_token, {})

    poly_yes = poly_yes_book.get("best_ask")
    poly_no  = poly_no_book.get("best_ask")
    poly_yes_vol = poly_yes_book.get("best_ask_volume", 0)
    poly_no_vol  = poly_no_book.get("best_ask_volume", 0)

    kalshi_yes, kalshi_yes_vol = _kalshi_best_ask(ticker, "yes")
    kalshi_no,  kalshi_no_vol  = _kalshi_best_ask(ticker, "no")

    candidates = []

    # Direction A: YES on Poly, NO on Kalshi
    if poly_yes is not None and kalshi_no is not None:
        spread = round(1.0 - poly_yes - kalshi_no, 4)
        if spread >= cfg.min_profit_threshold:
            candidates.append({
                "direction": "A",
                "yes_price": poly_yes, "no_price": kalshi_no,
                "spread": spread,
                "yes_platform": "polymarket", "no_platform": "kalshi",
                "yes_ask_volume": poly_yes_vol, "no_ask_volume": kalshi_no_vol,
                "yes_token_id": yes_token, "no_token_id": no_token,
                "poly_market_id": pair["poly_market_id"],
                "kalshi_market_id": ticker,
                "pair_id": pair["id"],
            })

    # Direction B: NO on Poly, YES on Kalshi
    if poly_no is not None and kalshi_yes is not None:
        spread = round(1.0 - poly_no - kalshi_yes, 4)
        if spread >= cfg.min_profit_threshold:
            candidates.append({
                "direction": "B",
                "yes_price": kalshi_yes, "no_price": poly_no,
                "spread": spread,
                "yes_platform": "kalshi", "no_platform": "polymarket",
                "yes_ask_volume": kalshi_yes_vol, "no_ask_volume": poly_no_vol,
                "yes_token_id": yes_token, "no_token_id": no_token,
                "poly_market_id": pair["poly_market_id"],
                "kalshi_market_id": ticker,
                "pair_id": pair["id"],
            })

    if not candidates:
        return

    opp = max(candidates, key=lambda x: x["spread"])

    log.debug(f"Arb found | pair={opp['pair_id']} dir={opp['direction']} spread={opp['spread']:.4f} | {opp['yes_platform']} YES @ {opp['yes_price']} + {opp['no_platform']} NO @ {opp['no_price']}")

    # Size
    slippage_safe = min(
        opp["yes_ask_volume"] * cfg.orderbook_vol_fraction,
        opp["no_ask_volume"]  * cfg.orderbook_vol_fraction,
    )
    limiting_price = max(opp["yes_price"], opp["no_price"])
    max_by_budget  = int(cfg.max_bet_size_usd / limiting_price)
    contracts      = int(min(slippage_safe, max_by_budget))

    if contracts < 1:
        log.debug(f"Sizing: 0 contracts — skipping pair={opp['pair_id']}")
        return

    profit = round(contracts * (1.0 - opp["yes_price"] - opp["no_price"]), 4)
    if profit < cfg.min_profit_abs:
        log.debug(f"Sizing: profit ${profit} below min — skipping pair={opp['pair_id']} | DEBUG HELP: yes_ask_volume={opp['yes_ask_volume']} no_ask_volume={opp['no_ask_volume']} yes_price={opp['yes_price']} no_price={opp['no_price']}")
        return

    try:
        direction = opp["direction"]
        if direction == "A":
            poly_side, poly_token = "BUY", opp["yes_token_id"]
            poly_price = opp["yes_price"]
            kalshi_side, kalshi_price = "no", opp["no_price"]
        else:
            poly_side, poly_token = "BUY", opp["no_token_id"]
            poly_price = opp["no_price"]
            kalshi_side, kalshi_price = "yes", opp["yes_price"]

        log.info(f"Executing | dir={direction} contracts={contracts} expected_profit=${profit:.4f} pair={opp['pair_id']}")

        if cfg.dry_run: #For dry run
            log.info(f"[DRY_RUN] Poly {poly_side} {contracts} contracts @ {poly_price} | Kalshi {kalshi_side.upper()} {contracts} contracts @ {kalshi_price}")
            opp_id = await insert_opportunity(cfg.db_path, {
                "pair_id": opp["pair_id"], "direction": direction,
                "yes_price": opp["yes_price"], "no_price": opp["no_price"],
                "spread": opp["spread"], "calculated_size": contracts, "executed": True,
            })
            await mark_opportunity_executed(cfg.db_path, opp_id)
            return

        # Fire both legs simultaneously
        results = await asyncio.gather(
            _place_poly_order(session, poly_side, poly_token, poly_price, contracts),
            _place_kalshi_order(session, kalshi_side, opp["kalshi_market_id"], kalshi_price, contracts),
            return_exceptions=True,
        )
        poly_result, kalshi_result = results
        poly_ok   = not isinstance(poly_result, Exception)
        kalshi_ok = not isinstance(kalshi_result, Exception)

        if poly_ok and kalshi_ok:
            opp_id = await insert_opportunity(cfg.db_path, {
                "pair_id": opp["pair_id"], "direction": direction,
                "yes_price": opp["yes_price"], "no_price": opp["no_price"],
                "spread": opp["spread"], "calculated_size": contracts, "executed": True,
            })
            await asyncio.gather(
                insert_trade(cfg.db_path, {
                    "opportunity_id": opp_id, "platform": "polymarket",
                    "side": poly_side, "price": poly_price, "size": contracts,
                    "tx_hash": poly_result.get("tx_hash", ""),
                    "status": poly_result.get("status", "unknown"),
                }),
                insert_trade(cfg.db_path, {
                    "opportunity_id": opp_id, "platform": "kalshi",
                    "side": kalshi_side.upper(), "price": kalshi_price, "size": contracts,
                    "tx_hash": "", "status": kalshi_result.get("status", "unknown"),
                }),
            )
            await mark_opportunity_executed(cfg.db_path, opp_id)
            log.info(f"Executed opp={opp_id} contracts={contracts} profit=${profit:.4f} pair={opp['pair_id']}")

        else:
            # Partial fill — unwind
            if poly_ok and not kalshi_ok:
                log.error(f"Kalshi leg failed. Unwinding Poly...")
                await _place_poly_order(session, "SELL", poly_token, poly_price, contracts)
            elif kalshi_ok and not poly_ok:
                log.error(f"Poly leg failed. Unwinding Kalshi...")
                unwind_side = "yes" if kalshi_side == "no" else "no"
                await _place_kalshi_order(session, unwind_side, opp["kalshi_market_id"],
                                          round(1.0 - kalshi_price, 4), contracts)
            else:
                log.error("Both legs failed — nothing to unwind")

    except Exception as exc:
        log.error(f"Execution error for pair={pair_key}: {exc}")

# ---------------------------------------------------------------------------
# Order placement (reused from arb_monitor pattern)
# ---------------------------------------------------------------------------

async def _place_poly_order(session, side, token_id, price, contracts):
    if cfg.dry_run:
        return {"status": "dry_run", "order_id": "DRY", "tx_hash": "DRY"}
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs
        client = ClobClient(host=CLOB_BASE, key=cfg.wallet_private_key,
                            chain_id=137, signature_type=0)
        client.set_api_creds(client.create_or_derive_api_creds())
        size_usd = round(contracts * price, 4)
        resp = client.create_and_post_order(
            OrderArgs(token_id=token_id, price=price, size=size_usd, side=side)
        )
        return {"status": resp.get("status", "open"),
                "order_id": resp.get("orderID", ""),
                "tx_hash": resp.get("transactionHash", "")}
    except Exception as exc:
        log.error(f"Poly order failed: {exc}")
        raise


async def _place_kalshi_order(session, side, ticker, price, contracts):
    if cfg.dry_run:
        return {"status": "dry_run", "order_id": "DRY"}
    price_cents = round(price * 100)
    path = "/trade-api/v2/portfolio/orders"
    headers = build_auth_headers("POST", path)
    payload = {
        "ticker": ticker, "action": "buy", "side": side.lower(),
        "type": "limit", "count": contracts,
        f"{side.lower()}_price": price_cents,
        "client_order_id": __import__("uuid").uuid4().hex,
    }
    try:
        async with session.post(f"{KALSHI_BASE}{path}", json=payload, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
        order = data.get("order", data)
        return {"status": str(order.get("status", "open")).lower(),
                "order_id": order.get("order_id", "")}
    except Exception as exc:
        log.error(f"Kalshi order failed: {exc}")
        raise


# ---------------------------------------------------------------------------
# Polymarket WebSocket
# ---------------------------------------------------------------------------
async def _handle_poly_message(msg: dict, session: aiohttp.ClientSession):
    """
    Handle incoming Polymarket WS message. Update poly_books and trigger
    arb check if relevant.

    Message types:
      - book: full orderbook snapshot for one token_id
      - price_change: best bid/ask update for one token_id
      - best_bid_ask: summary update with just the best bid/ask for one token_id
    """
    event_type = msg.get("event_type")
    asset_id   = msg.get("asset_id")

    if event_type == "book":
        asks = msg.get("asks", [])
        if asks and asset_id:
            best = asks[0]
            poly_books[asset_id] = {
                "best_ask": round(float(best["price"]), 4),
                "best_ask_volume": float(best.get("size", 0)),
            }
            pair = poly_token_to_pair.get(asset_id)
            if pair:
                await _check_and_execute(pair, session)
    elif event_type == "price_change":
        for change in msg.get("price_changes", []):
            asset_id = change.get("asset_id")
            best_ask = change.get("best_ask")
            if asset_id and best_ask:
                existing = poly_books.get(asset_id, {})
                poly_books[asset_id] = {
                    "best_ask": round(float(best_ask), 4),
                    "best_ask_volume": existing.get("best_ask_volume", 0),
                }
                pair = poly_token_to_pair.get(asset_id)
                if pair:
                    await _check_and_execute(pair, session)
                
async def _run_poly_ws(pairs: list[dict], session: aiohttp.ClientSession):
    """
    Connect to Polymarket WebSocket and subscribe to all YES/NO token IDs.
    Updates poly_books on every best_bid_ask or price_change event.
    Reconnects with exponential backoff on disconnect.
    """
    # Collect all token IDs from pairs
    asset_ids = []
    for pair in pairs:
        yes_id = pair.get("yes_token_id", "")
        no_id  = pair.get("no_token_id", "")
        if yes_id:
            asset_ids.append(yes_id)
            poly_token_to_pair[yes_id] = pair
        if no_id:
            asset_ids.append(no_id)
            poly_token_to_pair[no_id] = pair

    if not asset_ids:
        log.error("No Polymarket token IDs to subscribe to")
        return

    backoff = 1
    while True:
        try:
            log.info(f"Connecting to Polymarket WS ({len(asset_ids)} tokens)...")
            async with websockets.connect(POLY_WS_URL) as ws:
                backoff = 1  # reset on successful connect

                await ws.send(json.dumps({
                    "type": "market",
                    "assets_ids": asset_ids,
                }))
                log.info("Polymarket WS subscribed")

                # Heartbeat task
                async def _ping():
                    while True:
                        await asyncio.sleep(10)
                        try:
                            await ws.send(json.dumps({}))
                        except Exception:
                            break

                asyncio.create_task(_ping())

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    if isinstance(msg, list):
                        for m in msg:
                            await _handle_poly_message(m, session)
                    else:
                        await _handle_poly_message(msg, session)

        except Exception as exc:
            log.error(f"Polymarket WS disconnected: {exc} — reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------------------
# Kalshi WebSocket
# ---------------------------------------------------------------------------

async def _run_kalshi_ws(pairs: list[dict], session: aiohttp.ClientSession):
    """
    Connect to Kalshi WebSocket with RSA-PSS auth headers.
    Subscribes to orderbook_delta for all matched Kalshi tickers.
    Applies snapshots and deltas to kalshi_books.
    Reconnects with exponential backoff on disconnect.
    """
    tickers = []
    for pair in pairs:
        t = pair["kalshi_market_id"]
        tickers.append(t)
        kalshi_ticker_to_pair[t] = pair

    if not tickers:
        log.error("No Kalshi tickers to subscribe to")
        return

    path = "/trade-api/ws/v2"
    backoff = 1

    while True:
        try:
            log.info(f"Connecting to Kalshi WS ({len(tickers)} tickers)...")
            auth_headers = build_auth_headers("GET", path)
            extra_headers = {
                "KALSHI-ACCESS-KEY":       auth_headers["KALSHI-ACCESS-KEY"],
                "KALSHI-ACCESS-TIMESTAMP": auth_headers["KALSHI-ACCESS-TIMESTAMP"],
                "KALSHI-ACCESS-SIGNATURE": auth_headers["KALSHI-ACCESS-SIGNATURE"],
            }

            async with websockets.connect(
                KALSHI_WS_URL,
                additional_headers=extra_headers,
            ) as ws:
                backoff = 1

                # Subscribe to orderbook_delta for all tickers
                await ws.send(json.dumps({
                    "id": 1,
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["orderbook_delta"],
                        "market_tickers": tickers,
                    },
                }))
                log.info(f"Kalshi WS subscribed to {len(tickers)} tickers")

                # Heartbeat task
                async def _ping():
                    while True:
                        await asyncio.sleep(10)
                        try:
                            await ws.send(json.dumps({"id": 99, "cmd": "ping"}))
                        except Exception:
                            break

                asyncio.create_task(_ping())

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    msg_type = msg.get("type")
                    data = msg.get("msg", {})

                    if msg_type == "orderbook_snapshot":
                        ticker = data.get("market_ticker")
                        if ticker:
                            kalshi_books[ticker] = {
                                "yes_dollars": sorted(data.get("yes_dollars_fp", []), key=lambda x: float(x[0])),
                                "no_dollars": sorted(data.get("no_dollars_fp", []), key=lambda x: float(x[0])),
                            }
                            pair = kalshi_ticker_to_pair.get(ticker)
                            if pair:
                                await _check_and_execute(pair, session)
                    elif msg_type == "orderbook_delta":
                        ticker      = data.get("market_ticker")
                        side        = data.get("side")
                        price_dollars = data.get("price_dollars")
                        delta_fp    = data.get("delta_fp")

                        if ticker and side and price_dollars and delta_fp:
                            _apply_kalshi_delta(ticker, side, price_dollars, delta_fp)
                            pair = kalshi_ticker_to_pair.get(ticker)
                            if pair:
                                await _check_and_execute(pair, session)

        except Exception as exc:
            log.error(f"Kalshi WS disconnected: {exc} — reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run_ws_monitor():
    """
    Start WebSocket monitor. Runs forever — call from main.py instead of
    the scheduler-based monitor_and_execute_job.

    Loads matched pairs from DB at startup, then opens two concurrent
    WebSocket connections (Polymarket + Kalshi). Arb detection and
    execution happen on every orderbook update, no polling needed.
    """
    halt = await get_flag(cfg.db_path, "trading_halted")
    if halt == "true":
        log.warning("Trading HALTED. Clear 'trading_halted' in arb.db to resume.")
        return

    pairs = await get_active_matched_pairs(cfg.db_path)
    if not pairs:
        log.warning("WS monitor: no active matched pairs — nothing to subscribe to")
        return


    async with aiohttp.ClientSession() as session:
        await _init_orderbooks(pairs, session)

        log.info(f"WS monitor starting with {len(pairs)} matched pairs")

        await asyncio.gather(
            _run_poly_ws(pairs, session),
            _run_kalshi_ws(pairs, session),
        )