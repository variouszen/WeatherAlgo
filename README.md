# Weather Arb Bot — A/B/C Testing Mode
**Paper trading simulator — three strategies, one data spine, separate bankrolls**

Runs three competing trading strategies on Polymarket temperature markets:
- **Strategy B (Sigma):** Sigma-driven cumulative probability edge, 12-gate stack
- **Strategy A (Forecast Edge):** Forecast must clear threshold by 4F/2C, 7-gate stack
- **Strategy C (Spectrum):** Native per-bucket EV trading, YES-only, one best bucket per city-date

All share the same forecast pipeline, market data, and **bucket-native probability engine** with settlement-corrected rounding. Each has its own $2,000 bankroll.

---

## Architecture: Level 1 + Level 2

### Level 1 — Bucket-Native Probability Engine (shared by all strategies)

Maps forecast distributions onto Polymarket's actual native bucket structure with settlement-aware rounding correction (0.5 degrees).

- US markets: 2F interior buckets (e.g., "82-83F")
- International markets: 1C interior buckets (e.g., "13C")
- Settlement uses whole-degree precision (standard rounding)

Computes per-bucket forecast probabilities, then derives cumulative threshold probabilities for A/B.

### Level 2 — Strategy C (Spectrum)

Evaluates individual Polymarket buckets for mispricing. Both sides native: forecast bucket probability vs real bucket market price. No synthetic cumulative involved. YES-only at launch.

---

## Strategy Comparison

| Dimension | Strategy B (Sigma) | Strategy A (Edge) | Strategy C (Spectrum) |
|-----------|-------------------|-------------------|----------------------|
| Edge source | Cumulative probability | Forecast separation | Per-bucket mispricing |
| Market object | Derived cumulative | Derived cumulative | Native bucket |
| Direction | YES or NO | YES or NO | YES only |
| Win rate profile | Higher (55-60%) | Higher (55-60%) | Lower (15-25%) |
| Payout profile | 1.5-3x | 1.5-3x | 4-7x |
| Primary metric | Win rate + PnL | Win rate + PnL | PnL per trade + ROC |

---

## API endpoints

| Endpoint | Description |
|----------|-------------|
| GET /health | Health check (shows A/B/C mode) |
| GET /api/dashboard?strategy= | Dashboard (all/sigma/forecast_edge/spectrum) |
| GET /api/trades?strategy=&status= | Trade history with strategy filter |
| GET /api/stats/by-strategy | Three-way strategy comparison |
| POST /api/scan | Trigger manual scan |
| POST /api/admin/reset-bankroll?strategy= | Reset specific strategy bankroll |

---

## Deploy / Migration

### Adding Strategy C to existing A/B deployment
1. Push migrate_spectrum.py to repo root
2. In railway.json set: "startCommand": "python migrate_spectrum.py"
3. Deploy — migration adds bucket columns + Strategy C bankroll row
4. Restore railway.json to: "startCommand": "python run.py"
5. Push all updated files
6. Deploy — all three strategies begin scanning

---

## Key URLs
- Dashboard: https://web-production-5e27c.up.railway.app/
- Analysis: https://web-production-5e27c.up.railway.app/analysis
- Strategy comparison: https://web-production-5e27c.up.railway.app/api/stats/by-strategy
