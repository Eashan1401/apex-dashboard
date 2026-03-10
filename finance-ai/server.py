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
from datetime import datetime, timedelta

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
EODHD_KEY = os.environ.get('EODHD_API_KEY', '').strip()
POLYGON_KEY = os.environ.get('POLYGON_API_KEY', '').strip()

_CACHE = {}


def _cache_get(key, ttl_seconds):
    item = _CACHE.get(key)
    if not item:
        return None
    ts, value = item
    if time.time() - ts > ttl_seconds:
        return None
    return value


def _cache_set(key, value):
    _CACHE[key] = (time.time(), value)

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


def _build_fear_greed():
    """
    CNN Fear & Greed Index snapshot.
    Cached ~30 minutes.
    """
    cache_key = 'feargreed'
    cached = _cache_get(cache_key, 1800)
    if cached is not None:
        return cached

    url = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata'
    try:
        resp = requests.get(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (APEX Research Terminal)',
            },
            timeout=10,
        )
        resp.raise_for_status()
        j = resp.json()
        series = (j.get('fear_and_greed_historical') or {}).get('data') or []
        if not series:
            data = None
        else:
            # Assume last point is latest
            latest = series[-1]
            score = latest.get('y')
            rating = (latest.get('rating') or '').title()

            def _ago(days):
                target = datetime.utcnow() - timedelta(days=days)
                best = None
                best_diff = None
                for p in series:
                    ts = p.get('x')
                    if ts is None:
                        continue
                    dt = datetime.utcfromtimestamp(ts / 1000.0)
                    diff = abs((dt - target).days)
                    if best is None or diff < best_diff:
                        best = p
                        best_diff = diff
                return best

            prev = _ago(1)
            wk = _ago(7)
            mo = _ago(30)
            yr = _ago(365)

            def _val(p):
                return None if p is None else p.get('y')

            data = {
                'score': score,
                'rating': rating,
                'previous_close': _val(prev),
                'one_week_ago': _val(wk),
                'one_month_ago': _val(mo),
                'one_year_ago': _val(yr),
            }
    except Exception:
        data = None

    _cache_set(cache_key, data)
    return data


def _build_vix_term():
    """
    VIX term structure from CBOE CSV.
    Cached ~1 hour.
    """
    cache_key = 'vix_term'
    cached = _cache_get(cache_key, 3600)
    if cached is not None:
        return cached

    url = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv'
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        lines = resp.text.splitlines()
        # Expect header then rows: date, open, high, low, close
        rows = []
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) < 5:
                continue
            try:
                dt = datetime.strptime(parts[0], '%Y-%m-%d')
                close = float(parts[4])
                rows.append((dt, close))
            except Exception:
                continue
        if not rows:
            data = None
        else:
            rows.sort(key=lambda x: x[0])
            today_dt, today_vix = rows[-1]

            def _find_offset(days):
                target = today_dt - timedelta(days=days)
                best = None
                best_diff = None
                for d, v in rows:
                    diff = abs((d - target).days)
                    if best is None or diff < best_diff:
                        best = v
                        best_diff = diff
                return best

            vix_1w = _find_offset(7)
            vix_1m = _find_offset(30)
            vix_1y = _find_offset(365)

            # Percentile vs last 1y
            cutoff = today_dt - timedelta(days=365)
            last_year = [v for d, v in rows if d >= cutoff]
            if last_year:
                count = sum(1 for v in last_year if v <= today_vix)
                pct = count / len(last_year) * 100.0
            else:
                pct = None

            data = {
                'current': today_vix,
                'one_week_ago': vix_1w,
                'one_month_ago': vix_1m,
                'one_year_ago': vix_1y,
                'percentile_1y': pct,
            }
    except Exception:
        data = None

    _cache_set(cache_key, data)
    return data


def _eodhd_get(path, params=None, cache_key=None, ttl=300):
    if not EODHD_KEY:
        return None
    if cache_key:
        cached = _cache_get(cache_key, ttl)
        if cached is not None:
            return cached
    url = f'https://eodhd.com/api/{path}'
    p = {'api_token': EODHD_KEY, 'fmt': 'json'}
    if params:
        p.update(params)
    try:
        resp = requests.get(url, params=p, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        data = None
    if cache_key:
        _cache_set(cache_key, data)
    return data


def _polygon_get(path, params=None, cache_key=None, ttl=900):
    if not POLYGON_KEY:
        return None
    if cache_key:
        cached = _cache_get(cache_key, ttl)
        if cached is not None:
            return cached
    url = f'https://api.polygon.io{path}'
    p = {'apiKey': POLYGON_KEY}
    if params:
        p.update(params)
    try:
        resp = requests.get(url, params=p, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        data = None
    if cache_key:
        _cache_set(cache_key, data)
    return data


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

        # CNN Fear & Greed
        if path == '/api/feargreed':
            data = _build_fear_greed()
            self._send_json(data if data is not None else {'score': None})
            return

        # VIX term structure
        if path == '/api/vix-term':
            data = _build_vix_term()
            self._send_json(data if data is not None else {'current': None})
            return

        # Earnings calendar (EODHD)
        if path == '/api/earnings':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            raw = _eodhd_get(
                'calendar/earnings',
                params={'symbols': symbol},
                cache_key=f'earnings_{symbol}',
                ttl=3600,
            )
            out = {
                'symbol': symbol,
                'next_earnings': None,
                'history': [],
            }
            try:
                items = raw or []
                # Next earnings = first future date
                today = datetime.utcnow().date()
                future = [i for i in items if 'date' in i]
                future.sort(key=lambda x: x.get('date'))
                next_date = None
                for it in future:
                    try:
                        d = datetime.strptime(it['date'], '%Y-%m-%d').date()
                        if d >= today:
                            next_date = it['date']
                            break
                    except Exception:
                        continue
                out['next_earnings'] = next_date
                # Last 4 quarters EPS and revenue surprise
                past = []
                for it in items:
                    if len(past) >= 4:
                        break
                    if not it.get('eps_estimate') and not it.get('eps_actual'):
                        continue
                    est = it.get('eps_estimate')
                    act = it.get('eps_actual')
                    rev_est = it.get('revenue_estimate')
                    rev_act = it.get('revenue_actual')
                    surprise = None
                    if est not in (None, 0):
                        try:
                            surprise = (float(act) - float(est)) / float(est) * 100.0
                        except Exception:
                            surprise = None
                    out_item = {
                        'date': it.get('date'),
                        'eps_estimate': est,
                        'eps_actual': act,
                        'eps_surprise_pct': surprise,
                        'revenue_estimate': rev_est,
                        'revenue_actual': rev_act,
                    }
                    past.append(out_item)
                out['history'] = past
            except Exception:
                pass
            self._send_json(out)
            return

        # Fundamentals snapshot (EODHD)
        if path == '/api/fundamentals':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            # EODHD expects .US suffix for US names
            code = symbol + '.US'
            raw = _eodhd_get(
                f'fundamentals/{code}',
                cache_key=f'fund_{code}',
                ttl=300,
            )
            out = {
                'symbol': symbol,
            }
            try:
                if raw:
                    g = raw.get('General', {})
                    h = raw.get('Highlights', {})
                    v = raw.get('Valuation', {})
                    s = raw.get('SharesStats', {})
                    out.update(
                        {
                            'pe': h.get('PERatio'),
                            'pb': v.get('PriceBookMRQ'),
                            'ev_ebitda': v.get('EnterpriseValueEbitda'),
                            'roe': h.get('ReturnOnEquityTTM'),
                            'roic': h.get('ReturnOnInvestedCapitalTTM'),
                            'gross_margin': h.get('GrossMarginTTM'),
                            'debt_to_equity': h.get('DebtEquityRatio'),
                            'market_cap': h.get('MarketCapitalization'),
                            'revenue_ttm': h.get('RevenueTTM'),
                            'eps_ttm': h.get('EpsTTM'),
                            'dividend_yield': h.get('DividendYield'),
                            'high_52w': h.get('Week52High'),
                            'low_52w': h.get('Week52Low'),
                            'beta': h.get('Beta'),
                            'shares_outstanding': s.get('SharesOutstanding'),
                            'float_shares': s.get('SharesFloat'),
                        }
                    )
            except Exception:
                pass
            self._send_json(out)
            return

        # Insider transactions (EODHD)
        if path == '/api/insiders':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            code = symbol + '.US'
            raw = _eodhd_get(
                'insider-transactions',
                params={'code': code, 'limit': 10},
                cache_key=f'insiders_{code}',
                ttl=300,
            )
            items = []
            try:
                for it in raw or []:
                    items.append(
                        {
                            'code': it.get('Code'),
                            'name': it.get('Name'),
                            'position': it.get('Position'),
                            'transaction_type': it.get('Type'),
                            'shares': it.get('Shares'),
                            'value': it.get('Value'),
                            'date': it.get('FilingDate') or it.get('TransactionDate'),
                        }
                    )
            except Exception:
                items = []
            self._send_json({'symbol': symbol, 'transactions': items})
            return

        # Institutional ownership (EODHD fundamentals holders section)
        if path == '/api/institutional':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            code = symbol + '.US'
            raw = _eodhd_get(
                f'fundamentals/{code}',
                cache_key=f'inst_{code}',
                ttl=3600,
            )
            out = {
                'symbol': symbol,
                'institutional_ownership_pct': None,
                'top_holders': [],
                'ownership_qoq_change_pct': None,
            }
            try:
                if raw:
                    h = raw.get('Holders', {})
                    inst_pct = h.get('InstitutionalHoldersPercent')
                    out['institutional_ownership_pct'] = inst_pct
                    top = []
                    for it in (h.get('InstitutionalHolders') or [])[:5]:
                        top.append(
                            {
                                'holder': it.get('Holder'),
                                'shares': it.get('Shares'),
                                'pct_out': it.get('PctOut'),
                            }
                        )
                    out['top_holders'] = top
                    # QoQ ownership change – approximate from change field if present
                    ch = h.get('InstitutionalHoldersChange')
                    out['ownership_qoq_change_pct'] = ch
            except Exception:
                pass
            self._send_json(out)
            return

        # Economic calendar (EODHD)
        if path == '/api/economic-calendar':
            today = datetime.utcnow().date()
            to_date = today + timedelta(days=30)
            raw = _eodhd_get(
                'economic-events',
                params={'from': today.isoformat(), 'to': to_date.isoformat()},
                cache_key='econ_calendar',
                ttl=3600,
            )
            events = []
            try:
                for it in raw or []:
                    events.append(
                        {
                            'date': it.get('date'),
                            'event': it.get('event'),
                            'country': it.get('country'),
                            'actual': it.get('actual'),
                            'estimate': it.get('estimate'),
                            'previous': it.get('previous'),
                            'impact': it.get('importance'),
                        }
                    )
            except Exception:
                events = []
            self._send_json({'events': events})
            return

        # Options intelligence (Polygon snapshot)
        if path == '/api/options':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            raw = _polygon_get(
                f'/v3/snapshot/options/{symbol}',
                params={'limit': 50},
                cache_key=f'options_{symbol}',
                ttl=900,
            )
            out = {
                'symbol': symbol,
                'iv30': None,
                'put_call_ratio': None,
                'max_pain': None,
                'unusual_activity': False,
            }
            try:
                if raw:
                    results = raw.get('results') or []
                    calls = 0
                    puts = 0
                    strikes = {}
                    ivs = []
                    for opt in results:
                        details = opt.get('details') or {}
                        o_type = details.get('contract_type') or details.get('exercise_style')
                        if o_type:
                            t = str(o_type).upper()
                        else:
                            t = ''
                        last_quote = opt.get('last_quote') or {}
                        open_interest = last_quote.get('open_interest') or 0
                        iv = last_quote.get('implied_volatility')
                        if iv:
                            ivs.append(iv)
                        strike = details.get('strike_price')
                        if strike is not None and open_interest:
                            strikes[strike] = strikes.get(strike, 0) + open_interest
                        if 'PUT' in t:
                            puts += open_interest
                        elif 'CALL' in t:
                            calls += open_interest
                    if ivs:
                        out['iv30'] = sum(ivs) / len(ivs)
                    if calls or puts:
                        out['put_call_ratio'] = puts / (calls or 1)
                    if strikes:
                        # Max pain approximated as strike with max OI
                        out['max_pain'] = max(strikes.items(), key=lambda kv: kv[1])[0]
                    # Unusual activity heuristic: very high put/call or elevated IV
                    if (out['put_call_ratio'] is not None and out['put_call_ratio'] > 1.5) or (
                        out['iv30'] is not None and out['iv30'] > 1.0
                    ):
                        out['unusual_activity'] = True
            except Exception:
                pass
            self._send_json(out)
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

            # Build current market context from local helpers (same data as /api/macro, /api/stocks, /api/feargreed)
            macro = _build_macro_snapshot()
            stocks = _build_stocks_snapshot()
            fear = _build_fear_greed()
            vix_term = _build_vix_term()

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

            # Fear & Greed
            if fear:
                lines.append("\nCNN FEAR & GREED INDEX:")
                fg_score = fear.get('score')
                fg_rating = fear.get('rating')
                lines.append(f"  Score: {fg_score} ({fg_rating})")
                lines.append(
                    f"  Prev: {fear.get('previous_close')} · 1W: {fear.get('one_week_ago')} · 1M: {fear.get('one_month_ago')} · 1Y: {fear.get('one_year_ago')}"
                )

            # VIX term structure snapshot
            if vix_term:
                lines.append("\nVIX TERM STRUCTURE:")
                lines.append(
                    f"  Spot: {vix_term.get('current')} · 1W: {vix_term.get('one_week_ago')} · 1M: {vix_term.get('one_month_ago')} · 1Y: {vix_term.get('one_year_ago')}"
                )
                if vix_term.get('percentile_1y') is not None:
                    lines.append(f"  1Y percentile: {vix_term['percentile_1y']:.1f}%")

            market_context = "\n".join(lines)

            system_prompt = (
                "You are APEX, a Goldman Sachs MD-level financial analyst and CFA charterholder. "
                "You have live market data. Think in terms of regime, cross-asset signals, risk-adjusted returns, and factor exposure. "
                "Be precise and concise. Reference specific data points from the context. No generic advice. "
                "Always consider second and third order effects of macro and positioning."
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
