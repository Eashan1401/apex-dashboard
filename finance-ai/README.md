# APEX — API keys (from your .env)

**Use your existing API keys:** copy your `.env` file from your finance-ai folder (the one with `dashboard.html` and `server.py`) into this folder:

- Put your `.env` here: `Myfinance/finance-ai/.env`
- In `.env` use: `ALPHA_VANTAGE_KEY=...`, `FINNHUB_TOKEN=...`, `FRED_API_KEY=...` (or `FINNHUB_API_KEY`)

Then run the server from the **parent folder** (Myfinance):

```bash
cd /path/to/Myfinance
python finance-ai/server.py
```

Open **http://localhost:5000** — the dashboard will get live data via the server; keys never go to the browser.
