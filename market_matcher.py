"""
market_matcher.py — Interactive CLI for manually matching Polymarket
markets to Kalshi markets.

Called from fetch_and_match_loop() in main.py exactly like the old
AI matcher — same function signature, same return value.

All input() calls are run in a thread executor so they don't block
the event loop while the WS monitor is running concurrently.
"""

import asyncio
import re

from config import cfg
from db import (
    get_poly_unmatched,
    get_kalshi_unmatched,
    insert_matched_pair,
    remove_poly_unmatched,
    remove_kalshi_unmatched,
)
from logger import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Async-safe input
# ---------------------------------------------------------------------------

async def _ainput(prompt: str = "") -> str:
    """Run input() in a thread so it doesn't block the event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, input, prompt)


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _truncate(text: str, width: int) -> str:
    return text if len(text) <= width else text[: width - 1] + "…"


def _print_markets(label: str, markets: list[dict]) -> None:
    print(f"\n{'─' * 72}")
    print(f"  {label}  ({len(markets)} unmatched)")
    print(f"{'─' * 72}")
    for i, m in enumerate(markets, start=1):
        question = _truncate(m.get("question", ""), 58)
        end      = (m.get("end_date", "") or "")[:10]
        cat      = (m.get("category",  "") or "").upper()
        print(f"  {i:>3}.  [{cat:8s}]  {end}  {question}")
    print(f"{'─' * 72}")


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------

def _parse_pairs(raw: str) -> list[tuple[int, int]] | None:
    """
    Parse '(1,2), (3,4)' into [(1,2), (3,4)].
    Returns None if nothing matched.
    """
    found = re.findall(r"\(\s*(\d+)\s*,\s*(\d+)\s*\)", raw)
    if not found:
        return None
    return [(int(a), int(b)) for a, b in found]


def _validate_pairs(
    pairs: list[tuple[int, int]],
    n_kalshi: int,
    n_poly: int,
) -> list[str]:
    errors = []
    for k_idx, p_idx in pairs:
        if k_idx < 1 or k_idx > n_kalshi:
            errors.append(f"Kalshi index {k_idx} is out of range (1–{n_kalshi})")
        if p_idx < 1 or p_idx > n_poly:
            errors.append(f"Poly index {p_idx} is out of range (1–{n_poly})")

    k_indices = [k for k, _ in pairs]
    p_indices = [p for _, p in pairs]
    if len(k_indices) != len(set(k_indices)):
        errors.append("Duplicate Kalshi indices — each market can only be matched once")
    if len(p_indices) != len(set(p_indices)):
        errors.append("Duplicate Poly indices — each market can only be matched once")

    return errors


# ---------------------------------------------------------------------------
# Main interactive flow
# ---------------------------------------------------------------------------

async def run_market_matcher() -> dict:
    """
    Drop-in replacement for the old AI matcher.

    Displays all unmatched Kalshi and Polymarket markets, then prompts
    for pairs in the format:  (kalshi_index, poly_index), ...

    Returns the same summary dict as the old matcher:
        {"confirmed": int, "candidates": 0, "skipped": 0}
    """
    poly_markets   = await get_poly_unmatched(cfg.db_path)
    kalshi_markets = await get_kalshi_unmatched(cfg.db_path)

    if not poly_markets or not kalshi_markets:
        log.info(
            f"Manual matcher: nothing to match "
            f"({len(poly_markets)} poly, {len(kalshi_markets)} kalshi unmatched)"
        )
        return {"confirmed": 0, "candidates": 0, "skipped": 0}

    log.info(
        f"Manual matcher: {len(poly_markets)} poly × {len(kalshi_markets)} kalshi — "
        f"prompting for manual input"
    )

    # Print both lists
    _print_markets("KALSHI MARKETS",     kalshi_markets)
    _print_markets("POLYMARKET MARKETS", poly_markets)

    print("\n  Enter pairs as (kalshi_index, poly_index).")
    print("  Example:  (1,3), (2,1), (4,4)")
    print("  Press Enter with no input to skip.\n")

    # Input loop — retry on bad input
    pairs: list[tuple[int, int]] = []
    while True:
        raw = (await _ainput("  Pairs > ")).strip()

        if not raw:
            log.info("Manual matcher: no input — skipping match cycle")
            return {"confirmed": 0, "candidates": 0, "skipped": 0}

        parsed = _parse_pairs(raw)
        if parsed is None:
            print("  ✗  Could not parse. Use format: (1,2), (3,4)")
            continue

        errors = _validate_pairs(parsed, len(kalshi_markets), len(poly_markets))
        if errors:
            for e in errors:
                print(f"  ✗  {e}")
            continue

        pairs = parsed
        break

    # Confirmation preview
    print(f"\n{'─' * 72}")
    print(f"  Confirm {len(pairs)} match(es):")
    print(f"{'─' * 72}")
    for k_idx, p_idx in pairs:
        k = kalshi_markets[k_idx - 1]
        p = poly_markets[p_idx - 1]
        print(f"  K{k_idx:>2}:  {_truncate(k['question'], 60)}")
        print(f"  P{p_idx:>2}:  {_truncate(p['question'], 60)}")
        print()

    confirm = (await _ainput("  Confirm? [y/N] > ")).strip().lower()
    if confirm != "y":
        log.info("Manual matcher: user aborted at confirmation step")
        return {"confirmed": 0, "candidates": 0, "skipped": 0}

    # Write confirmed pairs to DB
    confirmed = 0
    for k_idx, p_idx in pairs:
        k = kalshi_markets[k_idx - 1]
        p = poly_markets[p_idx - 1]

        pair = {
            "poly_market_id":   p["market_id"],
            "kalshi_market_id": k["market_id"],
            "poly_question":    p.get("question", ""),
            "kalshi_question":  k.get("question", ""),
            "similarity_score": 1.0,
            "end_date":         p.get("end_date") or k.get("end_date", ""),
            "category":         p.get("category") or k.get("category", ""),
        }

        pair_id = await insert_matched_pair(cfg.db_path, pair)
        await remove_poly_unmatched(cfg.db_path, p["market_id"])
        await remove_kalshi_unmatched(cfg.db_path, k["market_id"])

        log.info(
            f"✓ Manual match (id={pair_id}): "
            f"'{_truncate(p['question'], 40)}' ↔ '{_truncate(k['question'], 40)}'"
        )
        print(f"  ✓  Saved pair id={pair_id}")
        confirmed += 1

    print(f"\n  {confirmed} pair(s) saved.\n")
    return {"confirmed": confirmed, "candidates": 0, "skipped": 0}