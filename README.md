# poly-kalshi-arb

A real-time cross-platform arbitrage daemon that monitors [Polymarket](https://polymarket.com) and [Kalshi](https://kalshi.com) prediction markets for pricing inefficiencies in sports markets and executes two-legged trades.

## How it works

Prediction markets on Polymarket and Kalshi price the same real-world outcomes independently. When the same outcome is cheaper on one platform than the other, you can buy YES on one side and NO on the other for a total cost below $1.00 — locking in a risk-free profit regardless of the outcome.

For example:
```
Polymarket: Barcelona win YES @ $0.28  
Kalshi:     Barcelona win NO  @ $0.67  
Total cost: $0.95 → $0.05 profit per contract guaranteed
```

The daemon detects these windows in real time via WebSocket connections to both platforms and fires both legs simultaneously.

## Architecture

```
main.py
├── fetch_and_match_loop()     # Fetches new markets every 24h, prompts for manual matching
└── run_ws_monitor()           # Persistent WS connections, detects + executes arb
    ├── _run_poly_ws()         # Polymarket CLOB WebSocket (price_change events)
    ├── _run_kalshi_ws()       # Kalshi WebSocket (orderbook_delta events)
    └── _check_and_execute()   # Spread detection, sizing, order placement
```

**Market discovery** — `polymarket_fetcher.py` and `kalshi_fetcher.py` pull active sports markets from each platform's REST API on a configurable interval.

**Market matching** — `market_matcher.py` presents unmatched markets as a numbered list and prompts for manual pair input `(kalshi_idx, poly_idx)`, ensuring correct outcome alignment.

**Real-time monitoring** — `ws_monitor.py` maintains live orderbook state in memory via two concurrent WebSocket connections. On every price update, it checks if the combined cost of both legs is below $1.00 by the minimum profit threshold.

**Execution** — Both legs fire simultaneously via `asyncio.gather`. A per-pair cooldown prevents re-execution on the same market within a configurable window. DB writes happen only after both legs confirm.

## Stack

- **Python 3.12** — asyncio, aiohttp, aiosqlite
- **WebSockets** — `websockets` library for both platforms
- **Auth** — RSA-PSS signing for Kalshi, EIP-712 / CLOB credentials for Polymarket
- **Storage** — SQLite via aiosqlite (markets, matched pairs, opportunities, trades)
- **Logging** — Loguru

## Supported leagues

| League | Polymarket tag | Kalshi series |
|--------|---------------|---------------|
| UCL | `ucl` | `KXUCLGAME` |
| EPL | `epl` | `KXEPLGAME` |
| La Liga | `laliga` | `KXLALIGAGAME` |
| NBA | `nba` | `KXNBAGAME` |
| MLB | `mlb` | `KXMLBGAME` |
| NHL | `nhl` | `KXNHLGAME` |
| Serie A | `serieA` | `KXSERIEA` |
| Ligue 1 | `ligue1` | `KXLIGUE1GAME` |

## Performance

Dry-run tested across live UCL quarter-final fixtures. achieving ~2.3% return on capital per session.

## Limitations

Prediction market arbitrage carries execution risk — spreads can close before both legs fill, and partial fills require unwinding.
