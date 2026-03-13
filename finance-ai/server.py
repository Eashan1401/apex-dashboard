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
from datetime import datetime, timedelta, date

import requests
import yfinance as yf
import feedparser

try:
    from stock_universe import load_stock_universe, search_universe, get_stock_universe
except ImportError:
    load_stock_universe = get_stock_universe = lambda: {}
    def search_universe(q, limit=10):
        return []

try:
    from fredapi import Fred
    _fred = Fred(api_key=os.environ.get('FRED_API_KEY', os.environ.get('FRED_KEY', '')))
except Exception:
    _fred = None

# Company name -> ticker for autocomplete (so "BLACKSTONE" resolves to BX)
COMPANY_NAME_MAP = {
    "BLACKSTONE": "BX", "BLACKROCK": "BLK", "GOLDMAN": "GS", "GOLDMAN SACHS": "GS",
    "JPMORGAN": "JPM", "JP MORGAN": "JPM", "MICROSOFT": "MSFT", "APPLE": "AAPL",
    "NVIDIA": "NVDA", "ALPHABET": "GOOGL", "GOOGLE": "GOOGL", "META": "META", "FACEBOOK": "META",
    "AMAZON": "AMZN", "TESLA": "TSLA", "EXXON": "XOM", "EXXONMOBIL": "XOM", "AMD": "AMD",
    "ADVANCED MICRO": "AMD", "BANK OF AMERICA": "BAC", "MORGAN STANLEY": "MS",
    "WELLS FARGO": "WFC", "CITIGROUP": "C", "CITI": "C", "ASML": "ASML", "SAP": "SAP",
    "SHELL": "SHEL", "TOTALENERGIES": "TTE", "NESTLE": "NESN.SW", "SIEMENS": "SIE.DE", "LVMH": "MC.PA",
}
# Ticker -> searchable name for news filtering (symbol + name in RSS)
COMPANY_NAME_FOR_NEWS = {
    "NVDA": "NVIDIA", "MSFT": "Microsoft", "AAPL": "Apple", "GOOGL": "Google",
    "META": "Meta", "JPM": "JPMorgan", "GS": "Goldman", "BAC": "Bank of America",
    "BX": "Blackstone", "BLK": "BlackRock", "XOM": "ExxonMobil", "AMD": "AMD",
    "AMZN": "Amazon", "TSLA": "Tesla", "ASML": "ASML", "SAP": "SAP",
    "MS": "Morgan Stanley", "WFC": "Wells Fargo", "C": "Citigroup",
}

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
    if data is None:
        try:
            t = yf.Ticker(symbol)
            fi = getattr(t, 'fast_info', None)
            if fi is not None:
                last = getattr(fi, 'last_price', None)
                prev = getattr(fi, 'previous_close', None) or last
                if last is not None:
                    last, prev = float(last), float(prev) if prev else last
                    chg = last - prev
                    pct = (chg / prev * 100) if prev and prev != 0 else 0
                    data = {'price': last, 'chg': chg, 'pct': f'{"+" if chg >= 0 else ""}{pct:.2f}%', 'u': chg >= 0}
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


def _calc_fear_greed():
    """
    VIX + SPY momentum proxy for Fear & Greed. No external API. Returns score 0-100 and rating.
    """
    score = 50
    details = {}
    try:
        vix_t = yf.Ticker("^VIX")
        fi = getattr(vix_t, 'fast_info', None)
        vix = getattr(fi, 'last_price', None) if fi else None
        if vix is not None:
            vix = float(vix)
            details['vix'] = vix
            if vix > 35:
                score -= 30
            elif vix > 28:
                score -= 20
            elif vix > 22:
                score -= 10
            elif vix < 14:
                score += 20
            elif vix < 17:
                score += 10
    except Exception as e:
        details['vix_error'] = str(e)
    try:
        spy_hist = yf.Ticker("SPY").history(period="1mo")
        if spy_hist is not None and not spy_hist.empty and len(spy_hist["Close"]) >= 2:
            spy_return = (spy_hist["Close"].iloc[-1] / spy_hist["Close"].iloc[0] - 1) * 100
            details['spy_20d_return'] = round(spy_return, 2)
            if spy_return < -8:
                score -= 20
            elif spy_return < -4:
                score -= 10
            elif spy_return < -1:
                score -= 5
            elif spy_return > 5:
                score += 15
            elif spy_return > 2:
                score += 8
    except Exception as e:
        details['spy_error'] = str(e)
    score = max(0, min(100, score))
    if score <= 25:
        rating = "Extreme Fear"
    elif score <= 45:
        rating = "Fear"
    elif score <= 55:
        rating = "Neutral"
    elif score <= 75:
        rating = "Greed"
    else:
        rating = "Extreme Greed"
    print('[FEAR_GREED] details:', details)
    return {"score": score, "rating": rating, "details": details}


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
    """Spot commodity prices with change % (gold, silver, wti, brent) from yfinance. Cached 5 min."""
    cache_key = 'commodities_quotes'
    cached = _cache_get(cache_key, 300)
    if cached is not None:
        return cached
    out = {}
    for key, ticker in [('gold', 'GC=F'), ('silver', 'SI=F'), ('wti', 'CL=F'), ('brent', 'BZ=F')]:
        try:
            t = yf.Ticker(ticker)
            fi = getattr(t, 'fast_info', None)
            if fi is None:
                continue
            last = getattr(fi, 'last_price', None)
            prev = getattr(fi, 'previous_close', None)
            if last is not None:
                last = float(last)
                prev = float(prev) if prev is not None else last
                change = last - prev
                change_pct = (change / prev * 100) if prev and prev != 0 else 0
                sign = '+' if change_pct >= 0 else ''
                out[key] = {
                    'price': round(last, 2),
                    'change': round(change, 2),
                    'change_pct': sign + str(round(change_pct, 2)) + '%',
                }
        except Exception:
            pass
    _cache_set(cache_key, out)
    return out


def _build_news_for_symbol(symbol):
    """Company-specific news: filter RSS by symbol/name + yfinance ticker.news, merge and dedupe. Returns up to 20."""
    symbol = (symbol or '').strip().upper()
    if not symbol:
        return []
    try:
        u = get_stock_universe()
        name = (u.get(symbol) or {}).get('name', '') or ''
        if not name:
            try:
                name = (yf.Ticker(symbol).info or {}).get('shortName', '') or ''
            except Exception:
                pass
        keywords = [symbol]
        if symbol in COMPANY_NAME_FOR_NEWS:
            keywords.append(COMPANY_NAME_FOR_NEWS[symbol])
        keywords += [w for w in (name or '').split() if len(w) > 2][:5]
        seen_titles = set()
        articles = []
        now = datetime.utcnow()

        # yfinance news
        try:
            for item in (yf.Ticker(symbol).news or [])[:15]:
                title = (item.get('title') or '').strip()
                if not title or title.lower() in seen_titles:
                    continue
                seen_titles.add(title.lower())
                link = item.get('link') or item.get('url') or '#'
                pub_ts = item.get('providerPublishTime') or item.get('published') or 0
                if isinstance(pub_ts, (int, float)) and pub_ts:
                    try:
                        dt = datetime.utcfromtimestamp(int(pub_ts))
                    except Exception:
                        dt = now
                else:
                    dt = now
                delta = now - dt
                hours = int(delta.total_seconds() // 3600)
                if hours <= 0:
                    time_ago = 'Just now'
                elif hours < 24:
                    time_ago = f'{hours}h'
                else:
                    time_ago = f'{hours // 24}d'
                articles.append({
                    'title': title,
                    'url': link,
                    'source': (item.get('publisher') or item.get('provider') or 'Yahoo'),
                    'published': dt.isoformat(),
                    'time_ago': time_ago,
                })
        except Exception as e:
            print('[NEWS] yfinance for', symbol, e)

        # RSS filter (from cache or one-off)
        cache_key = 'news_rss'
        rss_articles = _cache_get(cache_key, 99999)
        if rss_articles and rss_articles.get('articles'):
            for a in rss_articles['articles']:
                if len(articles) >= 20:
                    break
                title = (a.get('title') or '').strip()
                if not title or title.lower() in seen_titles:
                    continue
                t_lower = title.lower()
                if not any((k and k.lower() in t_lower) for k in keywords):
                    continue
                seen_titles.add(title.lower())
                articles.append({
                    'title': title,
                    'url': a.get('url', a.get('link', '#')),
                    'source': a.get('source', ''),
                    'published': a.get('published', ''),
                    'time_ago': a.get('time_ago', ''),
                })

        articles.sort(key=lambda x: x.get('published') or '', reverse=True)
        return articles[:20]
    except Exception as e:
        print('[NEWS] _build_news_for_symbol', symbol, e)
        return []


_ECON_CALENDAR_FALLBACK = [
    {"date": "2026-03-17", "event": "Retail Sales (Feb)", "importance": "HIGH", "forecast": "0.3%", "previous": "0.2%", "country": "US"},
    {"date": "2026-03-18", "event": "ZEW Economic Sentiment", "importance": "MED", "forecast": "52.0", "previous": "48.2", "country": "EU"},
    {"date": "2026-03-19", "event": "FOMC Rate Decision", "importance": "HIGH", "forecast": "Hold 3.50%", "previous": "3.50%", "country": "US"},
    {"date": "2026-03-19", "event": "GDP Q4 Final", "importance": "MED", "forecast": "2.3%", "previous": "2.8%", "country": "US"},
    {"date": "2026-03-20", "event": "BOE Rate Decision", "importance": "HIGH", "forecast": "Hold 4.50%", "previous": "4.50%", "country": "UK"},
    {"date": "2026-03-26", "event": "PCE Inflation (Jan)", "importance": "HIGH", "forecast": "2.6%", "previous": "2.5%", "country": "US"},
    {"date": "2026-04-02", "event": "NFP + Unemployment", "importance": "HIGH", "forecast": "170k", "previous": "182k", "country": "US"},
    {"date": "2026-04-10", "event": "CPI (Mar)", "importance": "HIGH", "forecast": "2.3%", "previous": "2.4%", "country": "US"},
    {"date": "2026-04-29", "event": "GDP Q1 Advance", "importance": "HIGH", "forecast": "1.8%", "previous": "2.3%", "country": "US"},
    {"date": "2026-04-06", "event": "ISM Services PMI", "importance": "MED", "forecast": "52.5", "previous": "52.1", "country": "US"},
]


def _build_economic_calendar():
    """FRED release dates + static fallback. Cache 6 hours."""
    cache_key = 'economic_calendar'
    cached = _cache_get(cache_key, 6 * 3600)
    if cached is not None:
        return cached
    events = []
    source = 'fred'
    today = date.today()
    thirty_days_out = today + timedelta(days=30)
    release_map = {
        10: ("Employment Situation (NFP)", "HIGH", "US"),
        48: ("CPI Inflation", "HIGH", "US"),
        53: ("GDP Growth", "HIGH", "US"),
        22: ("Retail Sales", "MED", "US"),
        21: ("PCE / Personal Income", "HIGH", "US"),
        46: ("Producer Price Index", "MED", "US"),
        82: ("FOMC Meeting", "HIGH", "US"),
    }
    if _fred is not None:
        for rid, (event_name, importance, country) in release_map.items():
            try:
                df = _fred.get_release_dates(release_id=rid, realtime_start=today.isoformat(), realtime_end=thirty_days_out.isoformat(), limit=2)
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        d = row.get('date')
                        if d is not None:
                            dstr = d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10]
                            events.append({"date": dstr, "event": event_name, "importance": importance, "country": country, "forecast": "", "previous": ""})
            except Exception as e:
                print('[ECON_CAL] FRED release', rid, e)
    if len(events) < 4:
        events = list(_ECON_CALENDAR_FALLBACK)
        source = 'scheduled'
    events.sort(key=lambda x: x.get('date') or '')
    result = {"events": events, "source": source, "last_updated": datetime.utcnow().isoformat()}
    if source == 'scheduled':
        result["last_verified"] = "2026-03-13"
    _cache_set(cache_key, result)
    return result


def _build_indicators():
    """GDP, unemployment, retail, housing, PMI, VIX, yield curve, CPI, real rate. Cache 30 min."""
    cache_key = 'indicators'
    cached = _cache_get(cache_key, 30 * 60)
    if cached is not None:
        return cached
    out = {"gdp": None, "unemployment": None, "retail_mom": None, "housing_starts": None, "pmi_mfg": 49.2, "pmi_services": 52.1,
           "vix": None, "yield_curve_2s10s": None, "cpi_yoy": None, "real_rate": None, "last_updated": datetime.utcnow().isoformat()}
    if _fred is not None:
        try:
            s = _fred.get_series('A191RL1Q225SBEA')
            if s is not None and not s.empty:
                out["gdp"] = round(float(s.dropna().iloc[-1]), 2)
        except Exception:
            pass
        try:
            s = _fred.get_series('UNRATE')
            if s is not None and not s.empty:
                out["unemployment"] = round(float(s.dropna().iloc[-1]), 2)
        except Exception:
            pass
        try:
            s = _fred.get_series('RSAFS')
            if s is not None and not s.dropna().empty and len(s) >= 2:
                arr = s.dropna()
                out["retail_mom"] = round((float(arr.iloc[-1]) / float(arr.iloc[-2]) - 1) * 100, 2)
        except Exception:
            pass
        try:
            s = _fred.get_series('HOUST')
            if s is not None and not s.empty:
                out["housing_starts"] = round(float(s.dropna().iloc[-1]) / 1000, 2)
        except Exception:
            pass
    try:
        fi = getattr(yf.Ticker("^VIX"), 'fast_info', None)
        if fi is not None:
            v = getattr(fi, 'last_price', None)
            if v is not None:
                out["vix"] = round(float(v), 2)
    except Exception:
        pass
    try:
        macro = _build_macro_snapshot()
        t = macro.get('treasury') or {}
        dgs2, dgs10 = t.get('dgs2'), t.get('dgs10')
        if dgs2 is not None and dgs10 is not None:
            out["yield_curve_2s10s"] = round((dgs10 - dgs2) * 100, 0)
    except Exception:
        pass
    try:
        ov = _build_overview()
        if ov.get('cpi_yoy') is not None:
            out["cpi_yoy"] = round(ov["cpi_yoy"], 2)
        dgs10 = (_build_macro_snapshot().get('treasury') or {}).get('dgs10')
        dgs2 = (_build_macro_snapshot().get('treasury') or {}).get('dgs2')
        if out.get("yield_curve_2s10s") is None and dgs10 is not None and dgs2 is not None:
            out["yield_curve_2s10s"] = round((dgs10 - dgs2) * 100, 0)
        if ov.get('cpi_yoy') is not None and dgs10 is not None:
            out["real_rate"] = round(dgs10 - ov["cpi_yoy"], 2)
    except Exception:
        pass
    _cache_set(cache_key, out)
    return out


def _sec_cik_for_ticker(symbol):
    """Resolve ticker to SEC CIK. Uses cached company_tickers.json."""
    cache_key = 'sec_tickers'
    cached = _cache_get(cache_key, 86400)
    if cached is not None:
        return cached.get(symbol.upper())
    try:
        r = requests.get('https://www.sec.gov/files/company_tickers.json', headers={'User-Agent': 'APEX Research Terminal contact@apex.com'}, timeout=10)
        r.raise_for_status()
        data = r.json()
        mapping = {}
        for item in (data or {}).values():
            if isinstance(item, dict) and 'ticker' in item and 'cik_str' in item:
                mapping[item['ticker'].upper()] = str(item['cik_str']).zfill(10)
        _cache_set(cache_key, mapping)
        return mapping.get(symbol.upper())
    except Exception as e:
        print('[SEC] company_tickers', e)
        return None


_SEC_CIK_NAME_CACHE = {}


def _sec_name_for_cik(filer_cik):
    """Resolve filer CIK to person/entity name via data.sec.gov/submissions/CIK{cik}.json. Cached."""
    if not filer_cik:
        return None
    cik_str = str(filer_cik).zfill(10)
    if cik_str in _SEC_CIK_NAME_CACHE:
        return _SEC_CIK_NAME_CACHE[cik_str]
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik_str}.json"
        r = requests.get(url, headers={"User-Agent": "APEX Research Terminal contact@apex.com"}, timeout=8)
        r.raise_for_status()
        j = r.json()
        name = (j.get('name') or '').strip() or None
        _SEC_CIK_NAME_CACHE[cik_str] = name
        return name
    except Exception:
        _SEC_CIK_NAME_CACHE[cik_str] = None
        return None


def _parse_insider_entry(entry, link):
    """Extract name, transaction_type, shares, value from EDGAR entry. Optionally resolve name via filer CIK from URL."""
    import re
    summary = (entry.get('summary') or '')
    title = (entry.get('title') or '').strip()
    name = None
    if hasattr(entry.get('author'), 'get'):
        name = (entry.get('author') or {}).get('name', '').strip()
    if not name and entry.get('authors'):
        authors = entry.get('authors') or []
        if authors and isinstance(authors[0], dict):
            name = (authors[0].get('name') or '').strip()
    if not name and summary:
        m = re.search(r'(?:Reporting Owner|Filer|Name):\s*([^\n<]+)', summary, re.I)
        if m:
            name = m.group(1).strip()
    if not name and ' - ' in title:
        name = title.split(' - ')[0].strip()
    if not name:
        name = title or 'Unknown'
    if link and name in ('4', 'Unknown', '') or re.match(r'^\d+$', (name or '')):
        m = re.search(r'/CIK0*(\d+)/', link) or re.search(r'/data/(\d+)/', link) or re.search(r'(\d{10})', link)
        if m:
            cik = m.group(1)
            resolved = _sec_name_for_cik(cik)
            if resolved:
                name = resolved
    tx_type = 'Form 4'
    if summary:
        if 'S-Sale' in summary or 'S-S' in summary:
            tx_type = 'Sell'
        elif 'P-Purchase' in summary or 'P-P' in summary:
            tx_type = 'Buy'
    shares = None
    value = None
    if summary:
        sm = re.search(r'(\d[\d,]*)\s*shares?', summary, re.I)
        if sm:
            try:
                shares = int(sm.group(1).replace(',', ''))
            except Exception:
                pass
        vm = re.search(r'\$[\s]*([\d,]+(?:\.[\d]+)?)', summary)
        if vm:
            try:
                value = float(vm.group(1).replace(',', ''))
            except Exception:
                pass
    return {'name': name or 'Unknown', 'transaction_type': tx_type, 'shares': shares, 'value': value}


def _build_insiders_edgar(symbol):
    """SEC EDGAR Form 4 RSS + full-text search. Cache 2 hours. Name from author/summary/CIK lookup."""
    symbol = (symbol or '').strip().upper()
    if not symbol:
        return {"symbol": symbol, "transactions": []}
    cache_key = f'insiders_edgar_{symbol}'
    cached = _cache_get(cache_key, 2 * 3600)
    if cached is not None:
        return cached
    transactions = []
    try:
        cik = _sec_cik_for_ticker(symbol)
        if cik:
            url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=4&dateb=&owner=include&count=10&output=atom"
            r = requests.get(url, headers={"User-Agent": "APEX Research Terminal contact@apex.com"}, timeout=10)
            r.raise_for_status()
            feed = feedparser.parse(r.text)
            for entry in (feed.entries or [])[:15]:
                link = next((l.get('href') for l in (entry.get('links') or []) if l.get('rel') == 'alternate'), None) or entry.get('link') or ''
                updated = entry.get('updated') or entry.get('published') or ''
                try:
                    from datetime import datetime as dt
                    if updated:
                        d = dt.fromisoformat(updated.replace('Z', '+00:00')).date() if 'T' in updated else updated[:10]
                        date_str = d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(updated)[:10]
                    else:
                        date_str = ''
                except Exception:
                    date_str = str(updated)[:10] if updated else ''
                parsed = _parse_insider_entry(entry, link)
                transactions.append({
                    "name": parsed['name'],
                    "position": "unknown",
                    "transaction_type": parsed['transaction_type'],
                    "shares": parsed.get('shares'),
                    "value": parsed.get('value'),
                    "date": date_str,
                    "source": "SEC EDGAR",
                    "url": link,
                })
    except Exception as e:
        print('[INSIDERS] EDGAR', symbol, e)
    try:
        today = date.today()
        thirty_days_ago = (today - timedelta(days=30)).isoformat()
        url2 = f"https://efts.sec.gov/LATEST/search-index?q=%22{symbol}%22&forms=4&dateRange=custom&startdt={thirty_days_ago}&enddt={today.isoformat()}"
        r = requests.get(url2, headers={"User-Agent": "APEX Research Terminal contact@apex.com"}, timeout=10)
        if r.status_code == 200:
            j = r.json()
            hits = (j.get('hits') or {}).get('hits') or []
            seen = {t.get("date", "") + t.get("url", "") for t in transactions}
            for h in hits[:10]:
                src = h.get('_source') or {}
                fd = src.get('file_date') or ''
                url = (src.get('link') or '') if isinstance(src.get('link'), str) else ''
                names = src.get('display_names') or src.get('entity_name') or 'unknown'
                if isinstance(names, list):
                    names = names[0] if names else 'unknown'
                key = str(fd) + url
                if key not in seen:
                    seen.add(key)
                    transactions.append({"name": names, "position": "unknown", "transaction_type": "Form 4", "date": fd[:10] if fd else "", "source": "SEC EDGAR", "url": url or '#'})
    except Exception as e:
        print('[INSIDERS] EFFTS', symbol, e)
    transactions.sort(key=lambda x: x.get('date') or '', reverse=True)
    result = {"symbol": symbol, "transactions": transactions[:20]}
    _cache_set(cache_key, result)
    return result


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
            data = _calc_fear_greed()
            self._send_json(data)
            return

        # VIX term structure
        if path == '/api/vix-term':
            data = _build_vix_term()
            self._send_json(data if data is not None else {'current': None})
            return

        # Earnings (yfinance; EODHD calendar not on free plan)
        if path == '/api/earnings':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            cache_key = f'earnings_yf_{symbol}'
            cached = _cache_get(cache_key, 3600)
            if cached is not None:
                self._send_json(cached)
                return
            out = {'symbol': symbol, 'next_earnings': None, 'history': []}
            try:
                t = yf.Ticker(symbol)
                cal = getattr(t, 'calendar', None)
                if cal is not None:
                    try:
                        if hasattr(cal, 'columns') and 'Earnings Date' in (cal.columns if hasattr(cal.columns, '__contains__') else []):
                            dates = cal['Earnings Date']
                            if hasattr(dates, 'iloc') and len(dates) > 0:
                                first = dates.iloc[0]
                                out['next_earnings'] = first.strftime('%Y-%m-%d') if hasattr(first, 'strftime') else str(first)[:10]
                            elif isinstance(dates, (list, tuple)) and len(dates) > 0:
                                first = dates[0]
                                out['next_earnings'] = first.strftime('%Y-%m-%d') if hasattr(first, 'strftime') else str(first)[:10]
                            elif hasattr(dates, '__len__') and len(dates) > 0:
                                out['next_earnings'] = str(dates)[:10]
                        elif hasattr(cal, 'iloc') and not cal.empty:
                            next_dates = cal.index.astype(str).tolist() if hasattr(cal.index, 'astype') else []
                            for dstr in next_dates:
                                if dstr >= datetime.utcnow().strftime('%Y-%m-%d'):
                                    out['next_earnings'] = dstr[:10]
                                    break
                    except Exception:
                        pass
                if out['next_earnings'] is None:
                    ed = getattr(t, 'earnings_dates', None)
                    if ed is not None and not getattr(ed, 'empty', True) and len(ed) > 0:
                        try:
                            first_date = ed.index[0]
                            out['next_earnings'] = first_date.strftime('%Y-%m-%d') if hasattr(first_date, 'strftime') else str(first_date)[:10]
                        except Exception:
                            pass
                eh = getattr(t, 'earnings_history', None)
                if eh is not None and not (getattr(eh, 'empty', True)):
                    try:
                        df = eh.tail(4)
                        for _, row in df.iterrows():
                            est = row.get('epsEstimate') if hasattr(row, 'get') else None
                            act = row.get('epsActual') if hasattr(row, 'get') else None
                            surprise = None
                            if est not in (None, 0) and act is not None:
                                try:
                                    surprise = (float(act) - float(est)) / float(est) * 100.0
                                except Exception:
                                    pass
                            out['history'].append({
                                'date': str(row.get('startdatetime', ''))[:10] if hasattr(row, 'get') else '',
                                'eps_estimate': est,
                                'eps_actual': act,
                                'eps_surprise_pct': surprise,
                                'revenue_estimate': getattr(row, 'revenueEstimate', None),
                                'revenue_actual': getattr(row, 'revenueActual', None),
                            })
                        out['history'] = sorted(out['history'], key=lambda x: x.get('date') or '', reverse=True)[:4]
                    except Exception:
                        pass
            except Exception as e:
                print('[EARNINGS] yfinance', symbol, e)
            _cache_set(cache_key, out)
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
            out = {'symbol': symbol}
            if raw is None:
                out['source'] = 'unavailable'
                out['reason'] = 'API plan limitation'
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

        # Insider transactions (SEC EDGAR Form 4)
        if path == '/api/insiders':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'symbol': '', 'transactions': []})
                return
            data = _build_insiders_edgar(symbol)
            self._send_json(data)
            return

        # Institutional ownership (yfinance)
        if path == '/api/institutional':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            cache_key = f'inst_yf_{symbol}'
            cached = _cache_get(cache_key, 3600)
            if cached is not None:
                self._send_json(cached)
                return
            out = {
                'symbol': symbol,
                'inst_ownership_pct': None,
                'institutional_ownership_pct': None,
                'insider_pct': None,
                'top_holders': [],
                'ownership_qoq_change_pct': None,
                'source': 'yfinance',
            }
            try:
                t = yf.Ticker(symbol)
                info = t.info or {}
                v = info.get('heldPercentInstitutions')
                out['inst_ownership_pct'] = (v * 100 if v is not None and v < 2 else v)
                out['institutional_ownership_pct'] = out['inst_ownership_pct']
                v2 = info.get('heldPercentInsiders')
                out['insider_pct'] = (v2 * 100 if v2 is not None and v2 < 2 else v2)
                holders_df = getattr(t, 'institutional_holders', None)
                if holders_df is not None and not getattr(holders_df, 'empty', True):
                    for _, row in holders_df.head(3).iterrows():
                        holder = row['Holder'] if 'Holder' in row.index else '—'
                        pct = row['% Out'] if '% Out' in row.index else None
                        out['top_holders'].append({'holder': holder, 'pct': pct, 'pct_out': pct})
            except Exception as e:
                print('[INSTITUTIONAL] yfinance', symbol, e)
            _cache_set(cache_key, out)
            self._send_json(out)
            return

        # Economic calendar (FRED + static fallback)
        if path == '/api/economic-calendar':
            data = _build_economic_calendar()
            self._send_json(data)
            return

        # Macro indicators (FRED + overview)
        if path == '/api/indicators':
            data = _build_indicators()
            self._send_json(data)
            return

        # Options intelligence (yfinance option chain)
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
                tkr = yf.Ticker(symbol)
                expirations = getattr(tkr, 'options', None) or []
                if not expirations:
                    self._send_json(out)
                    return
                today = date.today()
                target = today + timedelta(days=30)
                def _days_to(d):
                    try:
                        dt = datetime.strptime(d, '%Y-%m-%d').date() if isinstance(d, str) else d
                        return abs((dt - target).days)
                    except Exception:
                        return 999
                closest_exp = min(expirations, key=_days_to) if expirations else None
                if not closest_exp:
                    self._send_json(out)
                    return
                chain = tkr.option_chain(closest_exp)
                calls = chain.calls.dropna(subset=['impliedVolatility']) if chain.calls is not None and not chain.calls.empty else None
                puts = chain.puts.dropna(subset=['impliedVolatility']) if chain.puts is not None and not chain.puts.empty else None
                if calls is not None and not calls.empty and puts is not None and not puts.empty:
                    iv_c = calls['impliedVolatility'].mean()
                    iv_p = puts['impliedVolatility'].mean()
                    out['iv30'] = round(((iv_c + iv_p) / 2) * 100, 1)
                coi = (chain.calls['openInterest'].sum() if chain.calls is not None and 'openInterest' in chain.calls.columns else 0) or 1
                poi = chain.puts['openInterest'].sum() if chain.puts is not None and 'openInterest' in chain.puts.columns else 0
                out['put_call_ratio'] = round(poi / coi, 2)
                all_strikes = sorted(set((chain.calls['strike'].tolist() if chain.calls is not None else []) + (chain.puts['strike'].tolist() if chain.puts is not None else [])))
                pain = {}
                for s in all_strikes:
                    c_pain = p_pain = 0
                    if chain.calls is not None and 'openInterest' in chain.calls.columns:
                        c_sub = chain.calls[chain.calls['strike'] < s]
                        if not c_sub.empty:
                            c_pain = ((s - c_sub['strike']) * c_sub['openInterest']).sum()
                    if chain.puts is not None and 'openInterest' in chain.puts.columns:
                        p_sub = chain.puts[chain.puts['strike'] > s]
                        if not p_sub.empty:
                            p_pain = ((p_sub['strike'] - s) * p_sub['openInterest']).sum()
                    pain[s] = c_pain + p_pain
                if pain:
                    out['max_pain'] = min(pain, key=pain.get)
                hist = tkr.history(period="60d")
                if not hist.empty:
                    rets = hist['Close'].pct_change().dropna()
                    hv = (rets.std() * (252 ** 0.5)) if not rets.empty else None
                    out['hv30'] = round(float(hv), 4) if hv is not None else None
                if out['iv30'] is not None and out['hv30'] is not None and out['hv30'] > 0:
                    out['iv_percentile'] = round(min((out['iv30'] / 100) / out['hv30'] * 50, 100), 0)
                if (out.get('put_call_ratio') and out['put_call_ratio'] > 1.5) or (out['iv30'] and out['hv30'] and out['iv30'] > out['hv30'] * 100 * 1.5):
                    out['unusual_activity'] = True
            except Exception as e:
                print(f'[OPTIONS] error for {symbol}:', e)
            self._send_json(out)
            return

        # News RSS aggregation (optional ?symbol= or ?topic= for filtered news)
        if path == '/api/news':
            symbol_param = (qs.get('symbol') or [''])[0].strip().upper() if qs.get('symbol') else ''
            topic_param = (qs.get('topic') or [''])[0].strip() if qs.get('topic') else ''
            if symbol_param:
                # Company-specific: filter RSS by symbol/name + merge yfinance news
                articles = _build_news_for_symbol(symbol_param)
                payload = {'articles': articles[:20]}
                self._send_json(payload)
                return
            if topic_param:
                # Topic brief: filter cached RSS by topic keywords
                cache_key = 'news_rss'
                cached = _cache_get(cache_key, 99999)
                articles = []
                if cached and cached.get('articles'):
                    keywords = [w.lower() for w in topic_param.split() if len(w) > 1]
                    for a in cached['articles']:
                        tit = (a.get('title') or '').lower()
                        if any(kw in tit for kw in keywords):
                            articles.append(a)
                self._send_json({'articles': articles[:15]})
                return
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

        # Stock universe autocomplete (in-memory, <50ms); company name -> ticker first
        if path == '/api/search/autocomplete':
            q = (qs.get('q') or [''])[0].strip()
            if not q:
                self._send_json({'results': []})
                return
            qu = q.upper()
            results = []
            if qu in COMPANY_NAME_MAP:
                ticker = COMPANY_NAME_MAP[qu]
                u = get_stock_universe()
                info = (u or {}).get(ticker) or {}
                results.append({
                    'ticker': ticker,
                    'name': info.get('name', qu.replace('_', ' ').title()),
                    'sector': info.get('sector', ''),
                    'industry': info.get('industry', ''),
                    'index': info.get('index', ''),
                })
            universe_results = search_universe(q, limit=10)
            seen = {r['ticker'] for r in results}
            for r in universe_results:
                if r['ticker'] not in seen:
                    seen.add(r['ticker'])
                    results.append(r)
            self._send_json({'results': results[:10]})
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
            fear = _calc_fear_greed()
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

            # Fear & Greed (VIX + SPY proxy)
            if fear:
                lines.append("\nFEAR & GREED (proxy):")
                fg_score = fear.get('score')
                fg_rating = fear.get('rating')
                lines.append(f"  Score: {fg_score} ({fg_rating})")
                details = fear.get('details') or {}
                if details:
                    parts = [f"{k}: {v}" for k, v in details.items()]
                    lines.append("  " + " · ".join(parts))

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
