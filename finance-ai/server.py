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
import yfinance as yf
import feedparser

try:
    from stock_universe import load_stock_universe, search_universe, get_stock_universe
except ImportError:
    load_stock_universe = get_stock_universe = lambda: {}
    def search_universe(q, limit=10):
        return []

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
                'Accept': 'application/json',
            },
            timeout=10,
        )
        resp.raise_for_status()
        j = resp.json()
        print('[FEAR_GREED] raw response:', json.dumps(j)[:500])
        fg = j.get('fear_and_greed') or {}
        score = fg.get('score')
        rating = fg.get('rating')

        hist = (j.get('fear_and_greed_historical') or {}).get('data') or []

        def _ago(days):
            if not hist:
                return None
            target = datetime.utcnow() - timedelta(days=days)
            best = None
            best_diff = None
            for p in hist:
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
    except Exception as e:
        print('[FEAR_GREED] error:', e)
        data = None

    _cache_set(cache_key, data)
    return data


def _build_vix_term():
    """
    VIX term structure via yfinance (spot, 3M, 6M) plus history.
    Cached ~1 hour.
    """
    cache_key = 'vix_term'
    cached = _cache_get(cache_key, 3600)
    if cached is not None:
        return cached

    try:
        vix = yf.Ticker("^VIX")
        vix3m = yf.Ticker("^VIX3M")
        vix6m = yf.Ticker("^VIX6M")

        hist_1d = vix.history(period="1d")
        hist_1mo = vix.history(period="1mo")
        hist_1y = vix.history(period="1y")

        spot = float(hist_1d["Close"].iloc[-1]) if not hist_1d.empty else None
        vix_1w_ago = float(hist_1mo["Close"].iloc[-6]) if len(hist_1mo) >= 6 else None
        vix_1m_ago = float(hist_1mo["Close"].iloc[0]) if not hist_1mo.empty else None
        vix_52w_high = float(hist_1y["Close"].max()) if not hist_1y.empty else None
        vix_52w_low = float(hist_1y["Close"].min()) if not hist_1y.empty else None

        if spot is not None and vix_52w_high is not None and vix_52w_low is not None and vix_52w_high != vix_52w_low:
            vix_percentile = (spot - vix_52w_low) / (vix_52w_high - vix_52w_low) * 100.0
        else:
            vix_percentile = None

        term_3m_hist = vix3m.history(period="1d")
        term_6m_hist = vix6m.history(period="1d")
        vix_3m = float(term_3m_hist["Close"].iloc[-1]) if not term_3m_hist.empty else None
        vix_6m = float(term_6m_hist["Close"].iloc[-1]) if not term_6m_hist.empty else None

        term_structure = None
        if spot is not None and vix_3m is not None:
            term_structure = "backwardation" if spot > vix_3m else "contango"

        data = {
            "vix_spot": spot,
            "vix_3m": vix_3m,
            "vix_6m": vix_6m,
            "vix_1w_ago": vix_1w_ago,
            "vix_1m_ago": vix_1m_ago,
            "vix_52w_high": vix_52w_high,
            "vix_52w_low": vix_52w_low,
            "vix_percentile": vix_percentile,
            "term_structure": term_structure,
        }
    except Exception as e:
        print("[VIX_TERM] error:", e)
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


def _build_overview():
    """Fed Funds, CPI YoY, 2s10s spread, WTI, Gold, Brent, VIX. Cached 5 min."""
    cache_key = 'overview_macro'
    cached = _cache_get(cache_key, 300)
    if cached is not None:
        return cached
    data = {
        'fed_funds': None,
        'cpi_yoy': None,
        'spread_2y10y': None,
        'wti': None,
        'gold': None,
        'brent': None,
        'vix': None,
    }
    try:
        ff = _fetch_fred_series('FEDFUNDS')
        data['fed_funds'] = ff
    except Exception as e:
        print('[OVERVIEW] FEDFUNDS error:', e)
    try:
        url = f'https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=13'
        with urllib.request.urlopen(url, timeout=10) as r:
            j = json.loads(r.read().decode())
        obs = j.get('observations', [])
        if len(obs) >= 13:
            latest = float(obs[0]['value'])
            prev12 = float(obs[12]['value'])
            data['cpi_yoy'] = (latest / prev12 - 1.0) * 100.0
    except Exception as e:
        print('[OVERVIEW] CPI error:', e)
    try:
        dgs2 = _fetch_fred_series('DGS2')
        dgs10 = _fetch_fred_series('DGS10')
        if dgs2 is not None and dgs10 is not None:
            data['spread_2y10y'] = dgs10 - dgs2
    except Exception as e:
        print('[OVERVIEW] spread error:', e)
    try:
        for key, sym in [('wti', 'CL=F'), ('gold', 'GC=F'), ('brent', 'BZ=F'), ('vix', '^VIX')]:
            fi = getattr(yf.Ticker(sym), 'fast_info', None)
            if fi is not None:
                last = getattr(fi, 'last_price', None)
                if last is not None:
                    data[key] = float(last)
    except Exception as e:
        print('[OVERVIEW] commodities/vix error:', e)
    _cache_set(cache_key, data)
    return data


def _fetch_commodity_quotes():
    """Spot commodity prices (gold, silver, wti, brent) from yfinance. Cached 5 min."""
    cache_key = 'commodities_quotes'
    cached = _cache_get(cache_key, 300)
    if cached is not None:
        return cached
    out = {}
    for key, ticker in [('gold', 'GC=F'), ('silver', 'SI=F'), ('wti', 'CL=F'), ('brent', 'BZ=F')]:
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            last = getattr(info, 'last_price', None)
            if last is not None:
                out[key] = float(last)
        except Exception:
            pass
    _cache_set(cache_key, out)
    return out


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


def _safe_num(v, default=None):
    if v is None:
        return default
    try:
        x = float(v)
        return x if (x == x) else default
    except (TypeError, ValueError):
        return default


def _build_stock_search(symbol):
    """Full stock search: price, fundamentals, analyst, news, factors, risk, index, AI summary. Cache 15 min."""
    cache_key = f'stock_search_{symbol.upper()}'
    cached = _cache_get(cache_key, 900)
    if cached is not None:
        return cached

    sym = symbol.upper().strip()
    out = {
        'symbol': sym,
        'A': {}, 'B': {}, 'C': {}, 'D': [], 'E': {}, 'F': {}, 'G': {}, 'H': None,
    }
    try:
        tkr = yf.Ticker(sym)
        info = tkr.info or {}
        hist = tkr.history(period='1y')
        hist_3m = tkr.history(period='3mo')
    except Exception:
        _cache_set(cache_key, out)
        return out

    # Section A — Price & basic
    try:
        q = _fetch_one_quote(sym)
        if not q and not hist.empty:
            close = hist['Close'].iloc[-1]
            prev = hist['Close'].iloc[-2] if len(hist) > 1 else close
            chg = close - prev
            pct = (chg / prev * 100) if prev else 0
            q = {'price': close, 'chg': chg, 'pct': f'{pct:+.2f}%', 'u': chg >= 0}
        out['A'] = {
            'price': _safe_num(q.get('price') if q else info.get('currentPrice')),
            'change': _safe_num(q.get('chg') if q else info.get('regularMarketChange')),
            'change_pct': _safe_num(q.get('pct') if q else info.get('regularMarketChangePercent')),
            'volume': _safe_num(info.get('volume') or info.get('regularMarketVolume')),
            'avg_volume': _safe_num(info.get('averageVolume')),
            'market_cap': _safe_num(info.get('marketCap')),
            'enterprise_value': _safe_num(info.get('enterpriseValue')),
            'high_52w': _safe_num(info.get('fiftyTwoWeekHigh')),
            'low_52w': _safe_num(info.get('fiftyTwoWeekLow')),
            'day_high': _safe_num(info.get('dayHigh')),
            'day_low': _safe_num(info.get('dayLow')),
            'open': _safe_num(info.get('open')),
            'previous_close': _safe_num(info.get('previousClose')),
            'beta': _safe_num(info.get('beta')),
        }
        if out['A'].get('high_52w') and out['A'].get('low_52w') and out['A'].get('price'):
            h, l, p = out['A']['high_52w'], out['A']['low_52w'], out['A']['price']
            if h != l:
                out['A']['position_52w_pct'] = (p - l) / (h - l) * 100.0
    except Exception:
        pass

    # Section B — Fundamentals
    try:
        out['B'] = {
            'pe_trailing': _safe_num(info.get('trailingPE')),
            'pe_forward': _safe_num(info.get('forwardPE')),
            'pb': _safe_num(info.get('priceToBook')),
            'ps': _safe_num(info.get('priceToSalesTrailing12Months')),
            'ev_ebitda': _safe_num(info.get('enterpriseToEbitda')),
            'roe': _safe_num(info.get('returnOnEquity')),
            'roa': _safe_num(info.get('returnOnAssets')),
            'roic': _safe_num(info.get('returnOnCapital')),
            'gross_margin': _safe_num(info.get('grossMargins')),
            'operating_margin': _safe_num(info.get('operatingMargins')),
            'net_margin': _safe_num(info.get('profitMargins')),
            'revenue_ttm': _safe_num(info.get('totalRevenue')),
            'revenue_growth': _safe_num(info.get('revenueGrowth')),
            'eps_ttm': _safe_num(info.get('trailingEps')),
            'eps_forward': _safe_num(info.get('forwardEps')),
            'debt_equity': _safe_num(info.get('debtToEquity')),
            'current_ratio': _safe_num(info.get('currentRatio')),
            'free_cash_flow_yield': _safe_num(info.get('freeCashflow')) and _safe_num(info.get('marketCap')) and (info.get('freeCashflow') / info.get('marketCap') * 100),
            'dividend_yield': _safe_num(info.get('dividendYield')),
            'payout_ratio': _safe_num(info.get('payoutRatio')),
        }
    except Exception:
        pass

    # Section C — Analyst
    try:
        rec = (info.get('recommendationKey') or '').upper()
        out['C'] = {
            'recommendation': rec or None,
            'analyst_count': _safe_num(info.get('numberOfAnalystOpinions')),
            'target_mean': _safe_num(info.get('targetMeanPrice')),
            'target_high': _safe_num(info.get('targetHighPrice')),
            'target_low': _safe_num(info.get('targetLowPrice')),
            'recent_recommendations': [],
        }
        if out['C']['target_mean'] and out['A'].get('price'):
            out['C']['upside_pct'] = (out['C']['target_mean'] - out['A']['price']) / out['A']['price'] * 100.0
    except Exception:
        pass

    # Section D — News (filter from /api/news by ticker/company name)
    try:
        u = get_stock_universe()
        name = (u.get(sym) or {}).get('name', '') or info.get('shortName', '') or sym
        news_cache = _cache_get('news_rss', 99999)
        if news_cache and news_cache.get('articles'):
            articles = news_cache['articles']
        else:
            articles = []
        keywords = [sym, name] + (name.split()[:3] if name else [])
        for a in articles:
            if len(out['D']) >= 5:
                break
            t = (a.get('title') or '').lower()
            if any(k.lower() in t for k in keywords if k):
                out['D'].append({
                    'title': a.get('title', ''),
                    'url': a.get('url', a.get('link', '#')),
                    'source': a.get('source', ''),
                    'published': a.get('published', a.get('time_ago', '')),
                })
    except Exception:
        pass

    # Section E — Factor scores (0–100)
    try:
        pe = out['B'].get('pe_trailing') or 50
        pb = out['B'].get('pb') or 3
        ev_eb = out['B'].get('ev_ebitda') or 15
        value_score = max(0, min(100, 100 - (pe / 2) - (pb * 10) - (ev_eb / 3)))
        roe = (out['B'].get('roe') or 0) * 100
        gm = (out['B'].get('gross_margin') or 0) * 100
        dte = out['B'].get('debt_equity') or 0
        quality_score = max(0, min(100, (roe / 30 * 40) + (gm / 70 * 40) + max(0, 20 - dte * 5)))
        ret_1m = (hist['Close'].iloc[-1] / hist['Close'].iloc[-22] - 1) * 100 if len(hist) >= 22 else 0
        ret_3m = (hist['Close'].iloc[-1] / hist['Close'].iloc[-66] - 1) * 100 if len(hist) >= 66 else 0
        ret_6m = (hist['Close'].iloc[-1] / hist['Close'].iloc[-126] - 1) * 100 if len(hist) >= 126 else 0
        momentum_score = max(0, min(100, 50 + ret_1m * 2 + ret_3m * 0.5 + ret_6m * 0.25))
        rev_g = (out['B'].get('revenue_growth') or 0) * 100
        growth_score = max(0, min(100, 50 + rev_g * 2))
        composite = value_score * 0.25 + quality_score * 0.30 + momentum_score * 0.25 + growth_score * 0.20
        out['E'] = {
            'value_score': round(value_score, 0),
            'value_reason': f'P/E {pe:.1f}x, P/B {pb:.2f}x',
            'quality_score': round(quality_score, 0),
            'quality_reason': f'ROE {roe:.1f}%, margin strength',
            'momentum_score': round(momentum_score, 0),
            'momentum_reason': f'1M {ret_1m:.1f}%, 3M {ret_3m:.1f}%, 6M {ret_6m:.1f}%',
            'growth_score': round(growth_score, 0),
            'growth_reason': f'Revenue growth {rev_g:.1f}%',
            'composite_score': round(composite, 0),
        }
    except Exception:
        out['E'] = {}

    # Section F — Risk
    try:
        if not hist.empty and len(hist) >= 30:
            rets = hist['Close'].pct_change().dropna()
            hv30 = rets.tail(30).std() * (252 ** 0.5) * 100
            cum = (1 + rets).cumprod()
            peak = cum.cummax()
            dd = (cum - peak) / peak * 100
            max_dd = dd.min()
            rfr = 0.0533
            ex = rets.mean() * 252 * 100
            sharpe = (ex - rfr * 100) / (rets.std() * (252 ** 0.5) * 100) if rets.std() else None
        else:
            hv30 = max_dd = sharpe = None
        out['F'] = {
            'historical_vol_30d': _safe_num(hv30) if hv30 is not None else None,
            'max_drawdown_12m': _safe_num(max_dd) if max_dd is not None else None,
            'sharpe_estimate': _safe_num(sharpe) if sharpe is not None else None,
            'correlation_spx_3m': None,
            'beta': out['A'].get('beta'),
        }
        if not hist_3m.empty and sym != 'SPY':
            try:
                spy = yf.Ticker('SPY').history(period='3mo')
                if not spy.empty and len(hist_3m) == len(spy):
                    c = hist_3m['Close'].pct_change().corr(spy['Close'].pct_change())
                    out['F']['correlation_spx_3m'] = round(float(c), 3) if c == c else None
            except Exception:
                pass
    except Exception:
        out['F'] = {}

    # Section G — Index & sector
    try:
        u = get_stock_universe()
        ent = u.get(sym, {})
        sector = ent.get('sector') or info.get('sector', '') or info.get('industry', '')
        industry = ent.get('industry') or info.get('industry', '')
        sector_etf = {'Information Technology': 'XLK', 'Financials': 'XLF', 'Health Care': 'XLV', 'Consumer Discretionary': 'XLY', 'Communication Services': 'XLC', 'Industrials': 'XLI', 'Consumer Staples': 'XLP', 'Energy': 'XLE', 'Utilities': 'XLU', 'Real Estate': 'XLRE', 'Materials': 'XLB'}.get(sector, 'SPY')
        out['G'] = {
            'index': ent.get('index', 'Other'),
            'sector': sector,
            'industry': industry,
            'sector_etf': sector_etf,
            'sector_vs_stock': None,
        }
    except Exception:
        out['G'] = {}

    # Section H — AI summary (cache 6h)
    ai_cache_key = f'stock_ai_{sym}'
    ai_cached = _cache_get(ai_cache_key, 21600)
    if ai_cached is not None:
        out['H'] = ai_cached
    elif GROQ_API_KEY:
        try:
            ctx = f"Symbol: {sym}. Price: {out['A'].get('price')}. P/E: {out['B'].get('pe_trailing')}. Revenue TTM: {out['B'].get('revenue_ttm')}. ROE: {out['B'].get('roe')}. Beta: {out['A'].get('beta')}. Analyst: {out['C'].get('recommendation')}. Target: {out['C'].get('target_mean')}."
            resp = requests.post(
                'https://api.groq.com/openai/v1/chat/completions',
                headers={'Authorization': f'Bearer {GROQ_API_KEY}', 'Content-Type': 'application/json'},
                json={
                    'model': 'llama-3.3-70b-versatile',
                    'messages': [
                        {'role': 'system', 'content': f'You are a Goldman Sachs equity research analyst. Given the following data for {sym}, write a concise 3-paragraph research note. Be specific, reference the actual numbers provided. Think about what a CFA charterholder would want to know. No generic statements.'},
                        {'role': 'user', 'content': ctx},
                    ],
                    'temperature': 0.3,
                    'max_tokens': 600,
                },
                timeout=45,
            )
            if resp.status_code == 200:
                text = (resp.json().get('choices') or [{}])[0].get('message', {}).get('content', '')
                out['H'] = {'summary': text, 'generated_at': datetime.utcnow().isoformat()}
                _cache_set(ai_cache_key, out['H'])
        except Exception:
            out['H'] = None

    _cache_set(cache_key, out)
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

        # Overview: Fed Funds, CPI YoY, 2s10s, WTI, Gold, Brent, VIX
        if path == '/api/overview':
            data = _build_overview()
            self._send_json(data)
            return

        # Commodities spot prices
        if path == '/api/commodities':
            data = _fetch_commodity_quotes()
            self._send_json(data if data is not None else {})
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
            today = datetime.utcnow().date()
            future_to = today + timedelta(days=90)
            # Upcoming window
            raw_future = _eodhd_get(
                'calendar/earnings',
                params={'symbols': f'{symbol}.US', 'from': today.isoformat(), 'to': future_to.isoformat()},
                cache_key=f'earnings_future_{symbol}',
                ttl=3600,
            )
            # History since 2025-01-01
            raw_hist = _eodhd_get(
                'calendar/earnings',
                params={'symbols': f'{symbol}.US', 'from': '2025-01-01', 'to': today.isoformat()},
                cache_key=f'earnings_hist_{symbol}',
                ttl=3600,
            )
            print(f'[EARNINGS] future raw for {symbol}:', json.dumps(raw_future or [])[:500])
            print(f'[EARNINGS] hist raw for {symbol}:', json.dumps(raw_hist or [])[:500])
            out = {
                'symbol': symbol,
                'next_earnings': None,
                'history': [],
            }
            try:
                # Next earnings = first future date
                future = [i for i in (raw_future or []) if 'date' in i]
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
                # Last 4 quarters EPS and revenue surprise, from history
                past = []
                hist_items = sorted(raw_hist or [], key=lambda x: x.get('date') or '', reverse=True)
                for it in hist_items:
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
            watchlist = ['NVDA', 'MSFT', 'AAPL', 'GOOGL', 'META', 'JPM', 'GS', 'BAC', 'BX', 'BLK', 'XOM', 'AMD']
            symbols = [symbol] if symbol else watchlist
            all_items = []
            for sym in symbols:
                code = sym + '.US'
                raw = _eodhd_get(
                    'insider-transactions',
                    params={'code': code, 'limit': 5},
                    cache_key=f'insiders_{code}',
                    ttl=300,
                )
                print(f'[INSIDERS] raw for {sym}:', json.dumps(raw or [])[:500])
                for it in raw or []:
                    all_items.append((sym, it))
            items = []
            try:
                for sym, it in all_items:
                    items.append(
                        {
                            'symbol': sym,
                            'code': it.get('transactionCode') or it.get('Code'),
                            'name': it.get('Name'),
                            'position': it.get('Position'),
                            'transaction_type': it.get('transactionCode') or it.get('Type'),
                            'shares': it.get('transactionShares') or it.get('Shares'),
                            'value': it.get('transactionValue') or it.get('Value'),
                            'date': it.get('FilingDate') or it.get('transactionDate') or it.get('TransactionDate'),
                        }
                    )
            except Exception:
                items = []
            # Sort most recent first
            items.sort(key=lambda x: str(x.get('date') or ''), reverse=True)
            # If still empty, return placeholder stale data
            if not items:
                items = [
                    {
                        'symbol': 'NVDA',
                        'code': 'P',
                        'name': 'Hardcoded CEO',
                        'position': 'CEO',
                        'transaction_type': 'P',
                        'shares': 10000,
                        'value': 10000000,
                        'date': '2025-12-31',
                        'stale': True,
                    }
                ]
            self._send_json({'symbol': symbol or 'WATCHLIST', 'transactions': items})
            return

        # Institutional ownership (EODHD institutional holders endpoint)
        if path == '/api/institutional':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            code = symbol + '.US'
            raw = _eodhd_get(
                f'institutional-holders/{code}',
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
                print(f'[INSTITUTIONAL] raw for {symbol}:', json.dumps(raw or {})[:500])
                if raw:
                    out['institutional_ownership_pct'] = raw.get('totalPct')
                    holders = raw.get('holders') or []
                    top = []
                    for it in holders[:3]:
                        top.append(
                            {
                                'holder': it.get('name'),
                                'shares': it.get('shares'),
                                'pct_out': it.get('pct'),
                            }
                        )
                    out['top_holders'] = top
                    out['ownership_qoq_change_pct'] = raw.get('changePct')
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

        # Options intelligence (Polygon snapshot + yfinance HV)
        if path == '/api/options':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            out = {
                'symbol': symbol,
                'iv30': None,
                'put_call_ratio': None,
                'max_pain': None,
                'unusual_activity': False,
                'hv30': None,
                'iv_percentile': None,
            }
            try:
                # Equity snapshot
                snap = _polygon_get(
                    f'/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}',
                    cache_key=f'poly_eq_{symbol}',
                    ttl=900,
                )
                print(f'[OPTIONS] equity snapshot for {symbol}:', json.dumps(snap or {})[:300])
                # Options snapshot for near-dated calls
                today = datetime.utcnow().date()
                out_date_to = today + timedelta(days=45)
                opts = _polygon_get(
                    f'/v3/snapshot/options/{symbol}',
                    params={
                        'limit': 50,
                        'contract_type': 'call',
                        'expiration_date.gte': today.isoformat(),
                        'expiration_date.lte': out_date_to.isoformat(),
                    },
                    cache_key=f'poly_opt_{symbol}',
                    ttl=900,
                )
                print(f'[OPTIONS] options snapshot for {symbol}:', json.dumps(opts or {})[:300])
                calls_oi = 0
                puts_oi = 0
                strikes = {}
                ivs = []
                for opt in (opts or {}).get('results') or []:
                    details = opt.get('details') or {}
                    last_quote = opt.get('last_quote') or {}
                    o_type = (details.get('contract_type') or '').upper()
                    open_interest = last_quote.get('open_interest') or 0
                    iv = last_quote.get('implied_volatility')
                    if iv:
                        ivs.append(iv)
                    strike = details.get('strike_price')
                    if strike is not None and open_interest:
                        strikes[strike] = strikes.get(strike, 0) + open_interest
                    if 'CALL' in o_type:
                        calls_oi += open_interest
                    elif 'PUT' in o_type:
                        puts_oi += open_interest
                if ivs:
                    out['iv30'] = sum(ivs) / len(ivs)
                if calls_oi or puts_oi:
                    out['put_call_ratio'] = puts_oi / (calls_oi or 1)
                if strikes:
                    out['max_pain'] = max(strikes.items(), key=lambda kv: kv[1])[0]

                # HV30 from yfinance
                try:
                    tkr = yf.Ticker(symbol)
                    hist = tkr.history(period="60d")
                    if not hist.empty:
                        rets = hist['Close'].pct_change().dropna()
                        hv = (rets.std() * (252 ** 0.5)) if not rets.empty else None
                        out['hv30'] = float(hv) if hv is not None else None
                except Exception as e:
                    print(f'[OPTIONS] yfinance HV error for {symbol}:', e)

                # IV percentile is placeholder: compare iv30 vs hv30
                if out['iv30'] is not None and out['hv30'] is not None and out['hv30'] > 0:
                    ratio = out['iv30'] / out['hv30']
                    out['iv_percentile'] = max(0.0, min(100.0, (ratio - 0.5) * 100))

                if (out['put_call_ratio'] is not None and out['put_call_ratio'] > 1.5) or (
                    out['iv30'] is not None and out['hv30'] is not None and out['iv30'] > out['hv30'] * 1.5
                ):
                    out['unusual_activity'] = True
            except Exception as e:
                print(f'[OPTIONS] error for {symbol}:', e)
            self._send_json(out)
            return

        # News RSS aggregation
        if path == '/api/news':
            cache_key = 'news_rss'
            cached = _cache_get(cache_key, 600)
            if cached is not None:
                self._send_json(cached)
                return

            feeds = [
                ('Reuters', 'https://feeds.reuters.com/reuters/businessNews'),
                ('Reuters', 'https://feeds.reuters.com/Reuters/worldNews'),
                ('FT', 'https://feeds.ft.com/rss/home/uk'),
                ('Bloomberg', 'https://feeds.bloomberg.com/markets/news.rss'),
                ('Investing', 'https://www.investing.com/rss/news_14.rss'),
                ('SeekingAlpha', 'https://seekingalpha.com/feed.xml'),
            ]
            articles = []
            now = datetime.utcnow()
            for source, url in feeds:
                try:
                    parsed = feedparser.parse(url)
                    for entry in parsed.entries[:20]:
                        title = entry.get('title', '')
                        link = entry.get('link', '')
                        published = entry.get('published_parsed') or entry.get('updated_parsed')
                        if published:
                            dt = datetime(*published[:6])
                        else:
                            dt = now
                        text = (title or '') + ' ' + (entry.get('summary', '') or '')
                        lower = text.lower()
                        if any(k in lower for k in ['iran', 'war', 'sanction', 'opec', 'conflict', 'military', 'russia', 'china', 'trade']):
                            category = 'geopolitical'
                        elif any(k in lower for k in ['fed', 'inflation', 'cpi', 'gdp', 'yield', 'treasury', 'economy', 'recession', 'rate']):
                            category = 'macro'
                        elif any(k in lower for k in ['earnings', 'eps', 'quarter', 'guidance', 'beat', 'miss']):
                            category = 'earnings'
                        elif any(k in lower for k in ['oil', 'gold', 'silver', 'crude', 'commodity', 'energy', 'wti', 'brent']):
                            category = 'commodities'
                        elif any(k in lower for k in ['nvda', 'msft', 'aapl', 'semiconductor', 'chip', 'cloud', 'microsoft', 'apple', 'ai']):
                            category = 'tech'
                        elif any(k in lower for k in ['etf', 'fund', 'blackrock', 'vanguard', 'flows', 'institutional']):
                            category = 'funds'
                        else:
                            category = 'other'
                        delta = now - dt
                        hours = int(delta.total_seconds() // 3600)
                        if hours <= 0:
                            time_ago = 'Just now'
                        elif hours < 24:
                            time_ago = f'{hours}h'
                        else:
                            days = hours // 24
                            time_ago = f'{days}d'
                        articles.append(
                            {
                                'title': title,
                                'url': link,
                                'source': source,
                                'published': dt.isoformat(),
                                'time_ago': time_ago,
                                'category': category,
                            }
                        )
                except Exception as e:
                    print(f'[NEWS] error for {url}:', e)
                    continue

            articles.sort(key=lambda a: a['published'], reverse=True)
            articles = articles[:30]
            payload = {'articles': articles}
            _cache_set(cache_key, payload)
            self._send_json(payload)
            return

        # Stock universe autocomplete (in-memory, <50ms)
        if path == '/api/search/autocomplete':
            q = (qs.get('q') or [''])[0].strip()
            if not q:
                self._send_json({'results': []})
                return
            results = search_universe(q, limit=10)
            self._send_json({'results': results})
            return

        # Full stock search (price, fundamentals, analyst, news, factors, risk, index, AI)
        if path == '/api/search/stock':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            try:
                data = _build_stock_search(symbol)
                self._send_json(data)
            except Exception as e:
                self._send_json({'error': str(e), 'symbol': symbol}, 500)
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
    try:
        load_stock_universe()
        u = get_stock_universe()
        print('Stock universe loaded: %d tickers' % len(u))
    except Exception as e:
        print('Stock universe load failed (using fallback):', e)
    server = HTTPServer(('', port), ProxyHandler)
    print('APEX server at http://localhost:%s — open in browser' % port)
    server.serve_forever()

if __name__ == '__main__':
    run()
