# Weather Arb Bot 🌡️ — A/B Testing Mode
**Paper trading simulator — two strategies, one data spine, separate bankrolls**

Runs two competing trading strategies on Polymarket temperature markets:
- **Strategy B (Sigma):** Sigma-driven probability edge, high volume, 12-gate stack
- **Strategy A (Forecast Edge):** Forecast must clear threshold by ≥4°F/2°C, low volume, 7-gate stack

Both share the same forecast pipeline and market data. Each has its own $2,000 bankroll.

---

## What's real vs simulated

| Component | Real or Simulated |
|-----------|------------------|
| NOAA/ICON/JMA forecasts | ✅ Real — NWS API + Open-Meteo |
| GFS validator forecasts | ✅ Real — Open-Meteo |
| Polymarket prices & volume | ✅ Real — Gamma + CLOB API |
| Resolution check | ✅ Real — Polymarket bucket winners |
| Trade execution | 🟡 Simulated — paper money only |
| Bankroll | 🟡 Simulated — $2,000 per strategy |
| Polymarket fees | ✅ Modeled — 2% on winnings |

---

## Strategy Comparison

| Dimension | Strategy B (Sigma) | Strategy A (Forecast Edge) |
|-----------|-------------------|--------------------------|
| Edge source | Sigma tail probability | Forecast separation from threshold |
| Gap requirement | None | ≥4°F / ≥2°C |
| Uses consensus | Yes (directional agreement) | No |
| Uses spread gate | Yes | No |
| Gate count | 12 | 7 |
| Expected volume | 7-8 trades/day | 2-4 trades/day |
| Sigma role | Creates edge + sizes position | Sizes position only |

---

## A/B Trading Algorithm

```
Every 5 minutes:
  1. Fetch Polymarket prices (SHARED)
  2. Fetch primary forecasts — NOAA/ICON/JMA (SHARED)
  3. Fetch GFS validator forecasts (SHARED)
  4. Settle open positions for BOTH strategies
  5. For each city × threshold:
     — SHARED pre-filters: noon guard, liquidity
     — Compute forecast analytics (gap, directional agreement)
     — STRATEGY B (Sigma): Full 12-gate stack with directional consensus
     — STRATEGY A (Forecast Edge): 7-gate stack, forecast gap ≥4°F/2°C
     — Each writes trades tagged with strategy, using its own bankroll
  6. Log everything to Postgres
```

---

## API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check (shows A/B mode) |
| `GET /api/dashboard?strategy=` | Dashboard data (all / sigma / forecast_edge) |
| `GET /api/trades?strategy=&status=` | Trade history with strategy filter |
| `GET /api/stats/by-strategy` | Side-by-side strategy comparison |
| `GET /api/stats/by-city?strategy=` | Per-city breakdown |
| `GET /api/stats/by-model?strategy=` | Per-model breakdown |
| `GET /api/calibration` | Forecast vs actual |
| `POST /api/scan` | Trigger manual scan |
| `POST /api/admin/reset-bankroll?strategy=` | Reset specific strategy bankroll |

---

## Deploy / Migration

### First-time A/B setup (existing deployment)
1. Push `migrate_ab_testing.py` to repo root
2. In railway.json, temporarily set: `"startCommand": "python migrate_ab_testing.py"`
3. Deploy — migration adds new columns + Strategy A bankroll row
4. Restore railway.json to: `"startCommand": "python run.py"`
5. Push all updated files (config, database, signals, scanner, main, HTMLs)
6. Deploy — both strategies begin scanning

### Files changed for A/B
- `backend/config.py` — Added FORECAST_EDGE_CONFIG + STRATEGY_BANKROLL_ID
- `backend/models/database.py` — New Trade columns, dual bankroll init
- `backend/core/signals.py` — Fixed consensus + new evaluate_signal_forecast_edge()
- `backend/core/scanner.py` — Dual strategy execution, fixed dedup
- `backend/api/main.py` — Strategy filters on all endpoints
- `weather-arb-dashboard.html` — Strategy tabs + comparison bar
- `weather-analysis.html` — Strategy filter + gap column

---

## Evaluation Plan

**Target:** 100 settled trades per strategy

**Metrics (ranked):**
1. Net P&L per trade
2. Win rate
3. Total net P&L
4. Max drawdown
5. Return on deployed capital
6. Forecast accuracy (diagnostic)

**Decision framework:**
- A wins clearly → promote A, retire B
- B wins on volume → sigma works, keep both
- Both similar → test Strategy C (backtest from B data)
- Both losing → pause, recalibrate sigma

---

## Key URLs
- Dashboard: https://web-production-5e27c.up.railway.app/
- Analysis: https://web-production-5e27c.up.railway.app/analysis
- API docs: https://web-production-5e27c.up.railway.app/docs
- Strategy comparison: https://web-production-5e27c.up.railway.app/api/stats/by-strategy
