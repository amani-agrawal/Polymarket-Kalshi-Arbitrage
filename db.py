"""
db.py — SQLite schema definitions and async helper functions.

All database I/O is done via aiosqlite for compatibility with the async
event loop. WAL mode + busy_timeout are set on every connection to handle
concurrent access from the fetch and monitor jobs.
"""

import aiosqlite
from contextlib import asynccontextmanager
from typing import Optional
from logger import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

CREATE_POLY_MARKETS = """
CREATE TABLE IF NOT EXISTS poly_markets (
    market_id       TEXT PRIMARY KEY,
    question        TEXT NOT NULL,
    category        TEXT,
    end_date        TEXT,
    active          INTEGER DEFAULT 1,
    yes_token_id    TEXT,
    no_token_id     TEXT,
    fetched_at      TEXT DEFAULT (datetime('now'))
);
"""

CREATE_KALSHI_MARKETS = """
CREATE TABLE IF NOT EXISTS kalshi_markets (
    market_id       TEXT PRIMARY KEY,
    question        TEXT,
    category        TEXT,
    end_date        TEXT,
    status          TEXT,
    fetched_at      TEXT DEFAULT (datetime('now'))
);
"""

CREATE_POLY_UNMATCHED = """
CREATE TABLE IF NOT EXISTS poly_unmatched (
    market_id       TEXT PRIMARY KEY,
    FOREIGN KEY (market_id) REFERENCES poly_markets(market_id)
);
"""

CREATE_KALSHI_UNMATCHED = """
CREATE TABLE IF NOT EXISTS kalshi_unmatched (
    market_id       TEXT PRIMARY KEY,
    FOREIGN KEY (market_id) REFERENCES kalshi_markets(market_id)
);
"""

CREATE_MATCHED_MARKETS = """
CREATE TABLE IF NOT EXISTS matched_markets (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    poly_market_id      TEXT NOT NULL,
    kalshi_market_id    TEXT NOT NULL,
    poly_question       TEXT,
    kalshi_question     TEXT,
    similarity_score    REAL,
    end_date            TEXT,
    category            TEXT,
    matched_at          TEXT DEFAULT (datetime('now')),
    active              INTEGER DEFAULT 1,
    UNIQUE (poly_market_id, kalshi_market_id)
);
"""

CREATE_CANDIDATE_MATCHES = """
CREATE TABLE IF NOT EXISTS candidate_matches (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    poly_market_id      TEXT NOT NULL,
    kalshi_market_id    TEXT NOT NULL,
    poly_question       TEXT,
    kalshi_question     TEXT,
    similarity_score    REAL,
    created_at          TEXT DEFAULT (datetime('now')),
    reviewed            INTEGER DEFAULT 0
);
"""

CREATE_OPPORTUNITIES = """
CREATE TABLE IF NOT EXISTS opportunities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id         INTEGER NOT NULL,
    direction       TEXT NOT NULL,
    yes_price       REAL,
    no_price        REAL,
    spread          REAL,
    calculated_size REAL,
    timestamp       TEXT DEFAULT (datetime('now')),
    executed        INTEGER DEFAULT 0,
    FOREIGN KEY (pair_id) REFERENCES matched_markets(id)
);
"""

CREATE_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id  INTEGER NOT NULL,
    platform        TEXT NOT NULL,
    side            TEXT NOT NULL,
    price           REAL,
    size            REAL,
    tx_hash         TEXT,
    status          TEXT,
    timestamp       TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (opportunity_id) REFERENCES opportunities(id)
);
"""

CREATE_SYSTEM_FLAGS = """
CREATE TABLE IF NOT EXISTS system_flags (
    key     TEXT PRIMARY KEY,
    value   TEXT,
    updated TEXT DEFAULT (datetime('now'))
);
"""

ALL_DDL = [
    CREATE_POLY_MARKETS,
    CREATE_KALSHI_MARKETS,
    CREATE_POLY_UNMATCHED,
    CREATE_KALSHI_UNMATCHED,
    CREATE_MATCHED_MARKETS,
    CREATE_CANDIDATE_MATCHES,
    CREATE_OPPORTUNITIES,
    CREATE_TRADES,
    CREATE_SYSTEM_FLAGS,
]


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _get_conn(db_path: str):
    """
    Async context manager that opens a WAL-mode SQLite connection.

    WAL mode allows concurrent reads during writes. busy_timeout tells
    SQLite to wait up to 5 seconds before raising a "database is locked"
    error, preventing collisions between the fetch and monitor jobs.

    Usage:
        async with _get_conn(db_path) as db:
            await db.execute(...)

    Args:
        db_path: Filesystem path to the SQLite database file.
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        yield db


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

async def init_db(db_path: str) -> None:
    """
    Create all tables if they do not already exist.

    Args:
        db_path: Filesystem path to the SQLite database file.
    """
    async with _get_conn(db_path) as db:
        for ddl in ALL_DDL:
            await db.execute(ddl)
        await db.commit()
    log.info(f"Database initialised at {db_path}")


# ---------------------------------------------------------------------------
# Poly markets helpers
# ---------------------------------------------------------------------------

async def upsert_poly_market(db_path: str, market: dict) -> None:
    sql = """
        INSERT INTO poly_markets
            (market_id, question, category, end_date, active, yes_token_id, no_token_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(market_id) DO UPDATE SET
            active=excluded.active,
            end_date=excluded.end_date
    """
    async with _get_conn(db_path) as db:
        await db.execute(sql, (
            market["market_id"],
            market.get("question", ""),
            market.get("category", ""),
            market.get("end_date", ""),
            int(market.get("active", True)),
            market.get("yes_token_id", ""),
            market.get("no_token_id", ""),
        ))
        await db.commit()

async def get_seen_poly_ids(db_path: str) -> set:
    """
    Return the set of all Polymarket market_ids already stored.

    Args:
        db_path: Path to arb.db.

    Returns:
        Set of market_id strings.
    """
    async with _get_conn(db_path) as db:
        async with db.execute("SELECT market_id FROM poly_markets") as cur:
            rows = await cur.fetchall()
    return {r[0] for r in rows}

async def add_poly_unmatched(db_path: str, market_id: str) -> None:
    """
    Mark a Polymarket market as unmatched (awaiting pairing).

    Args:
        db_path: Path to arb.db.
        market_id: The conditionId of the Polymarket market.
    """
    async with _get_conn(db_path) as db:
        await db.execute(
            "INSERT OR IGNORE INTO poly_unmatched (market_id) VALUES (?)",
            (market_id,),
        )
        await db.commit()

async def get_poly_unmatched(db_path: str) -> list[dict]:
    """
    Fetch all unmatched Polymarket markets with their full details.

    Args:
        db_path: Path to arb.db.

    Returns:
        List of dicts with poly_markets columns.
    """
    sql = """
        SELECT p.* FROM poly_markets p
        INNER JOIN poly_unmatched u ON p.market_id = u.market_id
        WHERE p.active = 1
    """
    async with _get_conn(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]

async def remove_poly_unmatched(db_path: str, market_id: str) -> None:
    """
    Remove a Polymarket market from the unmatched queue after pairing.

    Args:
        db_path: Path to arb.db.
        market_id: The conditionId of the matched market.
    """
    async with _get_conn(db_path) as db:
        await db.execute(
            "DELETE FROM poly_unmatched WHERE market_id = ?", (market_id,)
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Kalshi markets helpers
# ---------------------------------------------------------------------------

async def upsert_kalshi_market(db_path: str, market: dict) -> None:
    sql = """
        INSERT INTO kalshi_markets
            (market_id, question, category, end_date, status)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(market_id) DO UPDATE SET
            status=excluded.status,
            end_date=excluded.end_date
    """
    async with _get_conn(db_path) as db:
        await db.execute(sql, (
            market["market_id"],
            market.get("question", ""),
            market.get("category", ""),
            market.get("end_date", ""),
            market.get("status", "active"),
        ))
        await db.commit()

async def get_seen_kalshi_ids(db_path: str) -> set:
    """
    Return the set of all Kalshi market tickers already stored.

    Args:
        db_path: Path to arb.db.

    Returns:
        Set of market_id (ticker) strings.
    """
    async with _get_conn(db_path) as db:
        async with db.execute("SELECT market_id FROM kalshi_markets") as cur:
            rows = await cur.fetchall()
    return {r[0] for r in rows}

async def add_kalshi_unmatched(db_path: str, market_id: str) -> None:
    """
    Mark a Kalshi market as unmatched (awaiting pairing).

    Args:
        db_path: Path to arb.db.
        market_id: Kalshi market ticker.
    """
    async with _get_conn(db_path) as db:
        await db.execute(
            "INSERT OR IGNORE INTO kalshi_unmatched (market_id) VALUES (?)",
            (market_id,),
        )
        await db.commit()

async def get_kalshi_unmatched(db_path: str) -> list[dict]:
    """
    Fetch all unmatched Kalshi markets with their full details.

    Args:
        db_path: Path to arb.db.

    Returns:
        List of dicts with kalshi_markets columns.
    """
    sql = """
        SELECT k.* FROM kalshi_markets k
        INNER JOIN kalshi_unmatched u ON k.market_id = u.market_id
        WHERE k.status = 'active'
    """
    async with _get_conn(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]

async def remove_kalshi_unmatched(db_path: str, market_id: str) -> None:
    """
    Remove a Kalshi market from the unmatched queue after pairing.

    Args:
        db_path: Path to arb.db.
        market_id: Kalshi market ticker.
    """
    async with _get_conn(db_path) as db:
        await db.execute(
            "DELETE FROM kalshi_unmatched WHERE market_id = ?", (market_id,)
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Matched markets helpers
# ---------------------------------------------------------------------------

async def insert_matched_pair(db_path: str, pair: dict) -> int:
    """
    Insert a confirmed matched market pair. Returns the new row id.

    Args:
        db_path: Path to arb.db.
        pair: Dict with poly_market_id, kalshi_market_id, questions,
              similarity_score, end_date, category.

    Returns:
        Integer rowid of the newly inserted matched_markets row.
    """
    sql = """
        INSERT OR IGNORE INTO matched_markets
            (poly_market_id, kalshi_market_id, poly_question,
             kalshi_question, similarity_score, end_date, category)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    async with _get_conn(db_path) as db:
        cur = await db.execute(sql, (
            pair["poly_market_id"],
            pair["kalshi_market_id"],
            pair.get("poly_question", ""),
            pair.get("kalshi_question", ""),
            round(float(pair.get("similarity_score", 0)), 4),
            pair.get("end_date", ""),
            pair.get("category", ""),
        ))
        await db.commit()
        return cur.lastrowid

async def insert_candidate_pair(db_path: str, pair: dict) -> None:
    """
    Log a low-confidence match candidate for manual review.

    Args:
        db_path: Path to arb.db.
        pair: Dict with poly_market_id, kalshi_market_id, questions, score.
    """
    sql = """
        INSERT INTO candidate_matches
            (poly_market_id, kalshi_market_id, poly_question,
             kalshi_question, similarity_score)
        VALUES (?, ?, ?, ?, ?)
    """
    async with _get_conn(db_path) as db:
        await db.execute(sql, (
            pair["poly_market_id"],
            pair["kalshi_market_id"],
            pair.get("poly_question", ""),
            pair.get("kalshi_question", ""),
            round(float(pair.get("similarity_score", 0)), 4),
        ))
        await db.commit()

async def get_active_matched_pairs(db_path: str) -> list[dict]:
    sql = """
        SELECT m.*,
               p.yes_token_id, p.no_token_id
        FROM matched_markets m
        JOIN poly_markets p ON m.poly_market_id = p.market_id
        JOIN kalshi_markets k ON m.kalshi_market_id = k.market_id
        WHERE m.active = 1
    """
    async with _get_conn(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]

async def deactivate_matched_pair(db_path: str, pair_id: int) -> None:
    """
    Mark a matched pair as inactive.

    Args:
        db_path: Path to arb.db.
        pair_id: matched_markets.id to deactivate.
    """
    async with _get_conn(db_path) as db:
        await db.execute(
            "UPDATE matched_markets SET active = 0 WHERE id = ?", (pair_id,)
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Opportunities + Trades
# ---------------------------------------------------------------------------

async def insert_opportunity(db_path: str, opp: dict) -> int:
    """
    Log an arbitrage opportunity (executed or not).

    Args:
        db_path: Path to arb.db.
        opp: Dict with pair_id, direction, yes_price, no_price, spread,
             calculated_size, executed.

    Returns:
        Integer rowid of the new opportunity row.
    """
    sql = """
        INSERT INTO opportunities
            (pair_id, direction, yes_price, no_price, spread,
             calculated_size, executed)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    async with _get_conn(db_path) as db:
        cur = await db.execute(sql, (
            opp["pair_id"],
            opp["direction"],
            round(float(opp.get("yes_price", 0)), 4),
            round(float(opp.get("no_price", 0)), 4),
            round(float(opp.get("spread", 0)), 4),
            round(float(opp.get("calculated_size", 0)), 4),
            int(opp.get("executed", False)),
        ))
        await db.commit()
        return cur.lastrowid


async def mark_opportunity_executed(db_path: str, opp_id: int) -> None:
    """
    Mark an opportunity as executed after a successful fill.

    Args:
        db_path: Path to arb.db.
        opp_id: opportunities.id to update.
    """
    async with _get_conn(db_path) as db:
        await db.execute(
            "UPDATE opportunities SET executed = 1 WHERE id = ?", (opp_id,)
        )
        await db.commit()


async def insert_trade(db_path: str, trade: dict) -> None:
    """
    Log one leg of an executed trade.

    Args:
        db_path: Path to arb.db.
        trade: Dict with opportunity_id, platform, side, price, size,
               tx_hash, status.
    """
    sql = """
        INSERT INTO trades
            (opportunity_id, platform, side, price, size, tx_hash, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    async with _get_conn(db_path) as db:
        await db.execute(sql, (
            trade["opportunity_id"],
            trade["platform"],
            trade["side"],
            round(float(trade.get("price", 0)), 4),
            round(float(trade.get("size", 0)), 4),
            trade.get("tx_hash", ""),
            trade.get("status", "unknown"),
        ))
        await db.commit()


# ---------------------------------------------------------------------------
# System flags
# ---------------------------------------------------------------------------

async def set_flag(db_path: str, key: str, value: str) -> None:
    """
    Upsert a system flag (e.g. trading_halted=true).

    Args:
        db_path: Path to arb.db.
        key: Flag name.
        value: Flag value string.
    """
    async with _get_conn(db_path) as db:
        await db.execute(
            """INSERT INTO system_flags (key, value, updated)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(key) DO UPDATE SET value=excluded.value,
               updated=excluded.updated""",
            (key, value),
        )
        await db.commit()


async def get_flag(db_path: str, key: str) -> Optional[str]:
    """
    Read a system flag value.

    Args:
        db_path: Path to arb.db.
        key: Flag name.

    Returns:
        String value, or None if not set.
    """
    async with _get_conn(db_path) as db:
        async with db.execute("SELECT value FROM system_flags WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Cleanup helpers
# ---------------------------------------------------------------------------

async def deactivate_poly_market(db_path: str, market_id: str) -> None:
    """Mark a Polymarket market inactive (soft delete)."""
    async with _get_conn(db_path) as db:
        await db.execute(
            "UPDATE poly_markets SET active = 0 WHERE market_id = ?", (market_id,)
        )
        await db.commit()


async def deactivate_kalshi_market(db_path: str, market_id: str) -> None:
    """Mark a Kalshi market inactive (soft delete)."""
    async with _get_conn(db_path) as db:
        await db.execute(
            "UPDATE kalshi_markets SET status = 'closed' WHERE market_id = ?",
            (market_id,),
        )
        await db.commit()


async def get_all_poly_markets(db_path: str) -> list[dict]:
    """Return all active Polymarket markets (for cleanup scanning)."""
    async with _get_conn(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM poly_markets WHERE active = 1") as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def get_all_kalshi_markets(db_path: str) -> list[dict]:
    """Return all open Kalshi markets (for cleanup scanning)."""
    async with _get_conn(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM kalshi_markets WHERE status = 'open'"
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]
