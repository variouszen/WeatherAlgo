# Weather Arb Bot 🌡️
**Paper trading simulator — real NOAA + real Polymarket prices, fake money**

Scans live Polymarket temperature markets every 5 minutes, computes true probability from NOAA/NWS forecasts, and paper trades when edge > 8%. All state persists in Postgres — survives Railway restarts.

---

## What's real vs simulated

| Component | Real or Simulated |
|-----------|------------------|
| NOAA forecast high/low | ✅ Real — direct NWS API |
| Polymarket Yes/No prices | ✅ Real — Gamma + CLOB API |
| Market volume | ✅ Real |
| Resolution check | ✅ Real — NWS station observations |
| Trade execution | 🟡 Simulated — paper money only |
| Bankroll | 🟡 Simulated — starts at $2,000 |
| Polymarket fees | ✅ Modeled — 2% on winnings |

---

## Local setup

```bash
git clone https://github.com/YOUR_USERNAME/weather-arb-bot
cd weather-arb-bot

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# Edit .env: set DATABASE_URL to your local Postgres

python run.py
# API at http://localhost:8000
# Docs at http://localhost:8000/docs
```

---

## Railway deploy

### 1. Fork to your GitHub

### 2. Create Railway project
- railway.app → New Project → Deploy from GitHub
- Select your fork

### 3. Add Postgres
- Railway dashboard → your project → Add Plugin → PostgreSQL
- Copy the `DATABASE_URL` from the plugin

### 4. Set environment variables
In Railway → your service → Variables:

```
DATABASE_URL        = (auto-set from Postgres plugin)
USER_AGENT          = WeatherArbBot/1.0 your@email.com
STARTING_BANKROLL   = 2000.0
DRY_RUN             = true
MIN_EDGE            = 0.08
MIN_CONFIDENCE      = 0.68
MIN_VOLUME          = 35000
```

### 5. Deploy
Railway auto-builds on push. Watch logs in Railway dashboard.

### 6. Verify it's running
```
https://your-app.railway.app/health
https://your-app.railway.app/api/dashboard
https://your-app.railway.app/docs
```

---

## API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /api/dashboard` | All stats, trades, equity curve |
| `POST /api/scan` | Trigger manual scan |
| `GET /api/trades?status=OPEN` | Trade history |
| `GET /api/calibration` | NOAA forecast vs actual |
| `GET /api/stats/by-city` | Per-city breakdown |

---

## Metrics tracked

**Performance**
- Total P&L (net of fees)
- Win rate
- Average edge %
- Expected value per trade
- Profit factor (gross wins / gross losses)
- Max drawdown %
- Sharpe ratio

**Per trade**
- NOAA forecast vs actual observed temp
- Forecast error (actual - forecast °F)
- Polymarket fees deducted
- Kelly sizing breakdown
- Entry price + shares

**Calibration**
- Daily NOAA forecast vs NWS observation for all cities
- Mean absolute error — tells you if σ=3.5°F assumption is correct

---

## Trading algorithm

```
Every 5 minutes:
  1. Fetch Polymarket prices via slug-based discovery (exact event lookup)
  2. Fetch NOAA/ECMWF primary forecasts for the correct market date per city
  3. Fetch GFS + ECMWF validator forecasts (multi-model consensus)
  4. Settle open positions where result is clear
  5. For each city × threshold (direct bucket matches only):
     - Directional gate: forecast must agree with trade direction
     - Buffer filter: forecast must clear threshold by ≥4°F / 1.5°C
     - P(high >= threshold) via Normal(forecast, sigma)
     - Edge = |NOAA_prob - market_price|
     - If edge >= 8% AND confidence >= 68% AND volume >= $35k:
       → Compute Quarter-Kelly size (capped at 2% bankroll)
       → Paper trade best signal per city
  6. Log everything to Postgres
```

---

## Scaling guide

| Stage | Bankroll | Condition to advance |
|-------|----------|---------------------|
| Paper | $2,000 fake | 2 weeks, 30+ trades |
| Seed | $100 real | Paper win rate > 55%, Sharpe > 1.0 |
| Grow | $500 real | Seed profitable over 2 weeks |
| Scale | $2,000 real | Consistent monthly profit |

---

## Warnings
- Paper trading does not guarantee real money results
- Edge decays as more bots enter the market
- Polymarket resolution uses specific NWS stations — verify per market
- Not financial advice — DYOR
