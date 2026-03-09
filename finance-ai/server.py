"""
APEX Research Terminal — local API proxy.
Reads API keys from .env in this folder. Serves dashboard and proxies live data.
Run: python server.py   then open http://localhost:5050
"""
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote
import json
import urllib.request
import time
import requests

# Load .env from finance-ai/ or parent
try:
    from dotenv import load_dotenv
    _dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(_dir, '.env'))
    load_dotenv(os.path.join(_dir, '..', '.env'))
except ImportError:
    pass

def _get_key(*names):
    for n in names:
        v = os.environ.get(n, '').strip()
        if v:
            return v
    return ''

ALPHA_KEY = _get_key('ALPHA_VANTAGE_KEY', 'ALPHA_VANTAGE_API_KEY', 'ALPHAVANTAGE_API_KEY')
FINNHUB_KEY = _get_key('FINNHUB_TOKEN', 'FINNHUB_API_KEY', 'FINNHUB_KEY')
FRED_KEY = _get_key('FRED_API_KEY', 'FRED_KEY')
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '').strip()

def _fetch_one_quote(symbol):
    data = None
    if ALPHA_KEY:
        try:
            url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={ALPHA_KEY}'
            with urllib.request.urlopen(url, timeout=12) as r:
                j = json.loads(r.read().decode())
            q = j.get('Global Quote', {})
            if q and q.get('05. price'):
                price = float(q['05. price'])
                prev = float(q.get('08. previous close') or price)
                chg = price - prev
                pct = (chg / prev * 100) if prev else 0
                data = {'price': price, 'chg': chg, 'pct': f'{"+" if chg >= 0 else ""}{pct:.2f}%', 'u': chg >= 0}
        except Exception:
            pass
    if data is None and FINNHUB_KEY:
        try:
            url = f'https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_KEY}'
            with urllib.request.urlopen(url, timeout=10) as r:
                q = json.loads(r.read().decode())
            if q.get('c') is not None:
                c = q['c']
                d = q.get('d') or 0
                dp = q.get('dp') or 0
                data = {'price': c, 'chg': d, 'pct': f'{"+" if dp >= 0 else ""}{dp:.2f}%', 'u': d >= 0}
        except Exception:
            pass
    return data

def _fetch_fred_series(series_id):
    if not FRED_KEY:
        return None
    try:
        url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=1'
        with urllib.request.urlopen(url, timeout=10) as r:
            j = json.loads(r.read().decode())
        obs = j.get('observations', [])
        if obs and obs[0].get('value') and obs[0]['value'] != '.':
            return float(obs[0]['value'])
    except Exception:
        pass
    return None


def _build_macro_snapshot():
    """
    Lightweight macro snapshot for AI context.
    Currently focuses on the US Treasury curve via FRED.
    """
    dgs2 = _fetch_fred_series('DGS2')
    dgs10 = _fetch_fred_series('DGS10')
    dgs30 = _fetch_fred_series('DGS30')
    return {
        'treasury': {
            'dgs2': dgs2,
            'dgs10': dgs10,
            'dgs30': dgs30,
        }
    }


def _build_stocks_snapshot():
    """
    Lightweight equity / index snapshot for AI context.
    Uses key ETFs / indices that should be supported by Alpha Vantage / Finnhub.
    """
    symbols = [
        'SPY',   # US large-cap (SPX proxy)
        'QQQ',   # US tech / NDX proxy
        'DIA',   # Dow Jones
        'IWM',   # Russell 2000
        'VIX',   # Volatility index
        'EFA',   # Developed ex-US proxy
        'EEM',   # EM equities proxy
        'DAX',   # German index ETF / ticker (if supported)
    ]
    out = {}
    for sym in symbols:
        q = _fetch_one_quote(sym)
        if q:
            out[sym] = q
        time.sleep(0.1)
    return out

class ProxyHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.path.join(os.path.dirname(__file__), '..'), **kwargs)

    def _send_json(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        # Batch quotes: /api/quotes?symbols=NVDA,MSFT,SPY,...
        if path == '/api/quotes' and qs.get('symbols'):
            raw = (qs.get('symbols') or [''])[0]
            symbols = [s.strip().upper() for s in unquote(raw).split(',') if s.strip()]
            out = {}
            for sym in symbols:
                q = _fetch_one_quote(sym)
                if q:
                    out[sym] = q
                time.sleep(0.15)  # avoid rate limit
            self._send_json(out)
            return

        # Single quote
        if path == '/api/quote' and parsed.query:
            symbol = (qs.get('symbol') or [''])[0].strip()
            if not symbol:
                self.send_error(400, 'Missing symbol')
                return
            data = _fetch_one_quote(symbol)
            if data is not None:
                self._send_json(data)
                return
            self._send_json({'error': 'no data'}, 404)
            return

        # Treasury / FRED: 2Y, 10Y, 30Y
        if path == '/api/treasury':
            dgs2 = _fetch_fred_series('DGS2')
            dgs10 = _fetch_fred_series('DGS10')
            dgs30 = _fetch_fred_series('DGS30')
            self._send_json({'dgs2': dgs2, 'dgs10': dgs10, 'dgs30': dgs30})
            return

        # Macro snapshot (currently wraps treasury curve; extendable later)
        if path == '/api/macro':
            macro = _build_macro_snapshot()
            self._send_json(macro)
            return

        # Stocks / indices snapshot used for AI context
        if path == '/api/stocks':
            stocks = _build_stocks_snapshot()
            self._send_json(stocks)
            return

        # Serve dashboard
        if path == '/' or path == '':
            self.path = '/dashboard.html'
        return SimpleHTTPRequestHandler.do_GET(self)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/ai':
            length = int(self.headers.get('Content-Length', 0))
            try:
                raw = self.rfile.read(length).decode() if length else '{}'
                body = json.loads(raw or '{}')
            except Exception:
                body = {}

            user_message = (body.get('message') or body.get('prompt') or '').strip()
            if not user_message:
                self._send_json({'error': 'no prompt'}, 400)
                return

            if not GROQ_API_KEY:
                self._send_json({'error': 'GROQ_API_KEY not configured on server'}, 500)
                return

            # Build current market context from local helpers (same data as /api/macro and /api/stocks)
            macro = _build_macro_snapshot()
            stocks = _build_stocks_snapshot()

            lines = []
            lines.append("CURRENT MARKET DATA SNAPSHOT")

            # Stocks / indices
            if stocks:
                lines.append("\nEQUITY & INDEX MOVES:")
                for sym, q in stocks.items():
                    price = q.get('price')
                    pct = q.get('pct')
                    if price is not None and pct is not None:
                        lines.append(f"  {sym}: {price} ({pct})")

            # Macro / rates
            t = (macro or {}).get('treasury') or {}
            if any(t.values()):
                lines.append("\nUS TREASURY YIELDS (FRED):")
                if t.get('dgs2') is not None:
                    lines.append(f"  2Y: {t['dgs2']}%")
                if t.get('dgs10') is not None:
                    lines.append(f"  10Y: {t['dgs10']}%")
                if t.get('dgs30') is not None:
                    lines.append(f"  30Y: {t['dgs30']}%")
                if t.get('dgs2') is not None and t.get('dgs10') is not None:
                    spread = t['dgs10'] - t['dgs2']
                    shape = 'INVERTED' if spread < 0 else 'NORMAL'
                    lines.append(f"  2s10s spread: {spread:.2f}% ({shape})")

            market_context = "\n".join(lines)

            system_prompt = (
                "You are APEX, an institutional-grade financial analyst. "
                "You have live market data context. Be concise, precise, think like a CFA charterholder. "
                "Reference actual data provided, no generic advice."
            )

            payload = {
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": f"{market_context}\n\nUser question: {user_message}",
                    },
                ],
                "temperature": 0.3,
            }

            try:
                resp = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=60,
                )
                if resp.status_code != 200:
                    self._send_json(
                        {
                            'error': f'Groq API error {resp.status_code}: {resp.text[:200]}'
                        },
                        502,
                    )
                    return
                data = resp.json()
                choices = data.get("choices") or []
                if not choices:
                    self._send_json({'error': 'No choices returned from Groq API'}, 502)
                    return
                content = (choices[0].get("message") or {}).get("content") or ""
                self._send_json({'response': content})
            except Exception as e:
                self._send_json({'error': str(e)}, 500)
            return
        self.send_error(404)

    def log_message(self, format, *args):
        print("[%s] %s" % (self.log_date_time_string(), format % args))

def run(port=None):
    port = int(os.environ.get('PORT', port or 5050))
    os.chdir(os.path.join(os.path.dirname(__file__), '..'))
    server = HTTPServer(('', port), ProxyHandler)
    print('APEX server at http://localhost:%s — open in browser' % port)
    server.serve_forever()

if __name__ == '__main__':
    run()
