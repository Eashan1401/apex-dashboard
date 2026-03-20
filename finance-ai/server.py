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
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))
except ImportError:
    pass

try:
    from stock_universe import load_stock_universe, search_universe, get_stock_universe
except ImportError:
    load_stock_universe = get_stock_universe = lambda: {}
    def search_universe(q, limit=10):
        return []

try:
    from fredapi import Fred
    _fred_api_key = (os.getenv('FRED_API_KEY') or os.getenv('FRED_KEY') or '').strip()
    _fred = Fred(api_key=_fred_api_key) if _fred_api_key else None
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

def _get_key(*names):
    for n in names:
        v = os.environ.get(n, '').strip()
        if v:
            return v
    return ''

ALPHA_KEY = _get_key('ALPHA_VANTAGE_KEY', 'ALPHA_VANTAGE_API_KEY', 'ALPHAVANTAGE_API_KEY')
FINNHUB_KEY = _get_key('FINNHUB_TOKEN', 'FINNHUB_API_KEY', 'FINNHUB_KEY')
FRED_KEY = (os.getenv('FRED_API_KEY') or os.getenv('FRED_KEY') or '').strip()
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


def _yf_treasury_yield_pct(symbol):
    """Approximate Treasury yield (%) from Yahoo index (e.g. ^TNX, ^IRX) when FRED fails."""
    try:
        t = yf.Ticker(symbol)
        info = getattr(t, 'info', None) or {}
        for k in ('regularMarketPrice', 'previousClose', 'open'):
            v = info.get(k)
            if v is not None and float(v) > 0:
                return float(v)
        fi = getattr(t, 'fast_info', None)
        if fi is not None:
            lp = getattr(fi, 'last_price', None)
            if lp is not None:
                return float(lp)
        hist = t.history(period='5d')
        if hist is not None and not hist.empty:
            return float(hist['Close'].iloc[-1])
    except Exception as e:
        print('[YF_TSY]', symbol, e)
    return None


def _yf_index_price_and_change_pct(symbol):
    """
    Index level and daily % for ^GSPC etc. Prefer info regularMarketPrice + regularMarketChangePercent;
    use history close if price missing or implausibly scaled.
    """
    price = None
    chg_pct = None
    try:
        t = yf.Ticker(symbol)
        info = getattr(t, 'info', None) or {}
        rp = info.get('regularMarketPrice') or info.get('currentPrice')
        if rp is not None:
            price = float(rp)
        rcp = info.get('regularMarketChangePercent')
        if rcp is not None:
            chg_pct = float(rcp)
            if abs(chg_pct) < 0.02 and chg_pct != 0:
                chg_pct *= 100.0
        chg = info.get('regularMarketChange')
        prev = info.get('regularMarketPreviousClose') or info.get('previousClose')
        if chg_pct is None and chg is not None and prev not in (None, 0):
            try:
                chg_pct = (float(chg) / float(prev)) * 100.0
            except Exception:
                pass
        hist = t.history(period='10d')
        if hist is not None and not hist.empty:
            last_c = float(hist['Close'].iloc[-1])
            if price is None:
                price = last_c
            elif symbol in ('^GSPC', '^NDX', '^DJI', '^RUT', '^GDAXI', '^FTSE', '^N225', '^HSI'):
                if price < 500 and last_c > 500:
                    price = last_c
            if chg_pct is None and len(hist) >= 2:
                prev_c = float(hist['Close'].iloc[-2])
                if prev_c:
                    chg_pct = (price - prev_c) / prev_c * 100.0
        if price is None and hist is not None and not hist.empty:
            price = float(hist['Close'].iloc[-1])
        if price is None:
            fi = getattr(t, 'fast_info', None)
            if fi is not None:
                lp = getattr(fi, 'last_price', None)
                if lp is not None:
                    price = float(lp)
    except Exception as e:
        print('[YF_INDEX]', symbol, e)
    return price, chg_pct


def _fed_funds_fallback_yf():
    """
    When FRED DFF is unavailable: ^IRX (13-week T-bill yield %) tracks policy rates loosely.
    Else hardcoded ~effective rate (update after FOMC).
    """
    v = _yf_treasury_yield_pct('^IRX')
    if v is not None and 0.5 < v < 15:
        return round(v, 4)
    return 3.64  # Approx effective Fed Funds — update after each FOMC if FRED unavailable


def _fred_treasury_yield_curve_with_fallback():
    """FRED DGS* with Yahoo ^TNX/^IRX/^FVX/^TYX/2YY=F fallbacks when API key missing or series empty."""
    t = {
        'dgs3mo': _fetch_fred_series('DGS3MO'),
        'dgs2': _fetch_fred_series('DGS2'),
        'dgs5': _fetch_fred_series('DGS5'),
        'dgs10': _fetch_fred_series('DGS10'),
        'dgs30': _fetch_fred_series('DGS30'),
    }
    ymap = [('dgs3mo', '^IRX'), ('dgs5', '^FVX'), ('dgs10', '^TNX'), ('dgs30', '^TYX')]
    for key, ysym in ymap:
        if t.get(key) is None:
            tv = _yf_treasury_yield_pct(ysym)
            if tv is not None:
                t[key] = tv
    if t.get('dgs2') is None:
        for sym in ('2YY=F', '^ZTWO'):
            tv = _yf_treasury_yield_pct(sym)
            if tv is not None:
                t['dgs2'] = tv
                break
    if t.get('dgs2') is None and t.get('dgs10') is not None:
        t['dgs2'] = max(0.05, float(t['dgs10']) - 0.46)
    if t.get('dgs3mo') is None and t.get('dgs10') is not None:
        t['dgs3mo'] = max(0.05, float(t['dgs10']) - 0.85)
    return t


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


def _fetch_fred_observations(series_id, limit=2):
    """Latest N observations (newest first) for MoM / change calculations."""
    if not FRED_KEY:
        return []
    try:
        url = (
            f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}'
            f'&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}'
        )
        with urllib.request.urlopen(url, timeout=10) as r:
            j = json.loads(r.read().decode())
        out = []
        for o in j.get('observations', []):
            v = o.get('value')
            if v and v != '.':
                out.append(float(v))
        return out
    except Exception:
        return []


def _fetch_cpi_yoy_fred():
    """CPI YoY % (same logic as overview) — for macro real Fed Funds."""
    try:
        if FRED_KEY:
            url = (
                f'https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL'
                f'&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=13'
            )
            with urllib.request.urlopen(url, timeout=10) as r:
                j = json.loads(r.read().decode())
            obs = j.get('observations', [])
            if len(obs) >= 13:
                latest = float(obs[0]['value'])
                prev12 = float(obs[12]['value'])
                return (latest / prev12 - 1.0) * 100.0
    except Exception:
        pass
    return 3.1  # approximate CPI YoY when FRED unavailable — update quarterly


def _fear_greed_apply_vix(score, vix):
    if vix is None:
        return score
    vix = float(vix)
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
    return score


def _fear_greed_apply_spy_momentum(score, spy_return):
    if spy_return is None:
        return score
    spy_return = float(spy_return)
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
    return score


def _fear_greed_score(vix, spy_return_pct):
    """Same VIX + SPY momentum composite as live card, 0–100."""
    s = 50
    s = _fear_greed_apply_vix(s, vix)
    s = _fear_greed_apply_spy_momentum(s, spy_return_pct)
    return max(0, min(100, int(round(s))))


def _fear_greed_spy_window_return(spy_close, end_idx, window=21):
    """~1 month proxy: return % from spy_close[start] to spy_close[end_idx] (inclusive)."""
    if spy_close is None or spy_close.empty:
        return None
    if end_idx < 0:
        end_idx = len(spy_close) + end_idx
    start_idx = end_idx - (window - 1)
    if start_idx < 0 or end_idx > len(spy_close) - 1:
        return None
    c0, c1 = float(spy_close.iloc[start_idx]), float(spy_close.iloc[end_idx])
    if c0 == 0:
        return None
    return (c1 / c0 - 1) * 100.0


def _calc_fear_greed():
    """
    VIX + SPY momentum proxy for Fear & Greed. No external API. Returns score 0-100 and rating.
    Includes approximate historical scores using VIX close + SPY ~21d return at past horizons.
    """
    score = 50
    details = {}
    previous_close = one_week_ago = one_month_ago = one_year_ago = None
    try:
        vix_t = yf.Ticker("^VIX")
        fi = getattr(vix_t, 'fast_info', None)
        vix = getattr(fi, 'last_price', None) if fi else None
        if vix is not None:
            vix = float(vix)
            details['vix'] = vix
            score = _fear_greed_apply_vix(score, vix)
    except Exception as e:
        details['vix_error'] = str(e)
    try:
        spy_hist = yf.Ticker("SPY").history(period="1mo")
        if spy_hist is not None and not spy_hist.empty and len(spy_hist["Close"]) >= 2:
            spy_return = (spy_hist["Close"].iloc[-1] / spy_hist["Close"].iloc[0] - 1) * 100
            details['spy_20d_return'] = round(spy_return, 2)
            score = _fear_greed_apply_spy_momentum(score, spy_return)
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
    # Historical approximate scores: VIX close at horizon + SPY ~21 trading-day return ending that day
    try:
        spy_long = yf.Ticker("SPY").history(period="2y")
        vix_long = yf.Ticker("^VIX").history(period="2y")
        if spy_long is not None and not spy_long.empty and vix_long is not None and not vix_long.empty:
            joined = spy_long["Close"].to_frame(name="spy").join(
                vix_long["Close"].to_frame(name="vix"), how="inner"
            )
            if joined is not None and len(joined) >= 22:
                closes = joined["spy"]
                vix_c = joined["vix"]
                n = len(joined)

                def _hist_at(offset_from_end):
                    """offset_from_end: 1 = prev bar, 6 = ~1w, 22 = ~1m, 253 = ~1y."""
                    end = n - 1 - offset_from_end
                    if end < 21:
                        return None, None
                    vx = float(vix_c.iloc[end])
                    spr = _fear_greed_spy_window_return(closes, end, window=21)
                    return vx, spr

                pc = _hist_at(1)
                if pc[0] is not None:
                    previous_close = _fear_greed_score(pc[0], pc[1])
                wk = _hist_at(6)
                if wk[0] is not None:
                    one_week_ago = _fear_greed_score(wk[0], wk[1])
                mo = _hist_at(22)
                if mo[0] is not None:
                    one_month_ago = _fear_greed_score(mo[0], mo[1])
                yr = _hist_at(253)
                if yr[0] is not None:
                    one_year_ago = _fear_greed_score(yr[0], yr[1])
    except Exception as e:
        details['fear_greed_hist_error'] = str(e)
    print('[FEAR_GREED] details:', details)
    return {
        "score": score,
        "rating": rating,
        "details": details,
        "previous_close": previous_close,
        "one_week_ago": one_week_ago,
        "one_month_ago": one_month_ago,
        "one_year_ago": one_year_ago,
    }


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


def _fred_treasury_yield_curve():
    """FRED DGS* with Yahoo fallbacks — shared by /api/treasury, /api/overview, /api/macro."""
    return _fred_treasury_yield_curve_with_fallback()


def _build_macro_snapshot():
    """
    Macro tab: full Treasury curve + Fed (DFF daily effective, DFEDTARL/U target band) + major CB rates.
    All spreads: DGS10−DGS2 (2s10s), DGS10−DGS3MO (3m10y), DGS30−DGS5 (5s30s).
    When FRED is unavailable, uses yfinance (^IRX, ^TNX, …) and hardcoded policy approximations.
    """
    treasury = _fred_treasury_yield_curve_with_fallback()
    cpi_yoy = _fetch_cpi_yoy_fred()
    # DFF = daily effective Fed Funds
    dff = _fetch_fred_series('DFF')
    tar_lo = _fetch_fred_series('DFEDTARL')
    tar_hi = _fetch_fred_series('DFEDTARU')
    ecb = _fetch_fred_series('ECBDFR')
    boe = _fetch_fred_series('INTDSRGBM')
    boj = _fetch_fred_series('IRSTCB01JPM156N')
    if boj is None:
        boj = _fetch_fred_series('IORBJ')
    if dff is None:
        dff = _fed_funds_fallback_yf()
    if tar_lo is None:
        tar_lo = 3.50  # Fed target band low — update after FOMC (Mar 2026 hold)
    if tar_hi is None:
        tar_hi = 3.75  # Fed target band high
    if ecb is None:
        ecb = 2.65  # ECB depo — update quarterly (-25bp Mar 6)
    if boe is None:
        boe = 4.50  # BOE bank rate — update quarterly
    if boj is None:
        boj = 0.50  # BOJ policy — update quarterly
    boc = 4.50  # BOC — update quarterly (held Mar 5)
    rba = 4.35  # RBA — update quarterly (held Feb 18)
    real_fed = None
    if dff is not None and cpi_yoy is not None:
        real_fed = round(float(dff) - float(cpi_yoy), 2)

    def _r4(x):
        return round(float(x), 4) if x is not None else None

    return {
        'treasury': {k: _r4(v) for k, v in treasury.items()},
        'fed': {
            'effective': _r4(dff),
            'target_low': _r4(tar_lo),
            'target_high': _r4(tar_hi),
            'cpi_yoy': round(float(cpi_yoy), 2) if cpi_yoy is not None else None,
            'real_fed': real_fed,
        },
        'policy_rates': {
            'ecb': _r4(ecb),
            'boe': _r4(boe),
            'boj': _r4(boj),
            'boc': _r4(boc),
            'rba': _r4(rba),
        },
    }


# Equities tab: global indices (yfinance). TTL 60s — cache key separate from other quotes.
_MARKET_INDICES_DEF = [
    ('SPX', '^GSPC'),
    ('NDX', '^NDX'),
    ('DJIA', '^DJI'),
    ('RUT', '^RUT'),
    ('VIX', '^VIX'),
    ('DAX', '^GDAXI'),
    ('FTSE', '^FTSE'),
    ('Nikkei', '^N225'),
    ('HSI', '^HSI'),
]


def _fetch_market_indices_equities():
    cache_key = 'market_indices_equities_v2'
    cached = _cache_get(cache_key, 60)
    if cached is not None:
        return cached
    indices = []
    for label, sym in _MARKET_INDICES_DEF:
        price, chg_pct = None, None
        try:
            price, chg_pct = _yf_index_price_and_change_pct(sym)
        except Exception as e:
            print('[MARKET_INDICES]', sym, e)
        indices.append({
            'label': label,
            'symbol': sym,
            'price': round(price, 4) if price is not None else None,
            'change_pct': round(chg_pct, 2) if chg_pct is not None else None,
        })
        time.sleep(0.06)
    out = {'indices': indices}
    _cache_set(cache_key, out)
    return out


_ANALYST_FALLBACK_PT = {
    'NVDA': 250.0,
    'MSFT': 450.0,
    'AAPL': 275.0,
    'GOOGL': 350.0,
    'META': 650.0,
    'JPM': 320.0,
    'GS': 900.0,
    'BAC': 55.0,
    'BX': 135.0,
    'BLK': 1100.0,
    'XOM': 185.0,
    'AMD': 250.0,
}
_ANALYST_BHS = {
    'NVDA': (42, 4, 0),
    'MSFT': (38, 6, 0),
    'AAPL': (22, 12, 1),
    'GOOGL': (28, 10, 0),
    'META': (35, 5, 0),
    'JPM': (12, 18, 2),
    'GS': (18, 14, 2),
    'BAC': (14, 20, 2),
    'BX': (16, 16, 2),
    'BLK': (22, 12, 1),
    'XOM': (8, 14, 3),
    'AMD': (32, 8, 2),
}

_ANALYST_TICKERS = [
    'NVDA', 'MSFT', 'AAPL', 'GOOGL', 'META', 'JPM', 'GS', 'BAC', 'BX', 'BLK', 'XOM', 'AMD',
]


def _fetch_analyst_consensus_equities():
    cache_key = 'analyst_consensus_equities_v2'
    cached = _cache_get(cache_key, 60)
    if cached is not None:
        return cached
    consensus = []
    for sym in _ANALYST_TICKERS:
        buy, hold, sell = _ANALYST_BHS.get(sym, (0, 0, 0))
        pt = None
        current = None
        upside = None
        try:
            t = yf.Ticker(sym)
            info = getattr(t, 'info', None) or {}
            tm = info.get('targetMeanPrice')
            if tm is not None:
                pt = float(tm)
            current = info.get('currentPrice') or info.get('regularMarketPrice')
            if current is None:
                fi = getattr(t, 'fast_info', None)
                if fi is not None:
                    current = getattr(fi, 'last_price', None)
            if current is not None:
                current = float(current)
            if pt is None:
                pt = _ANALYST_FALLBACK_PT.get(sym)
            if current is None:
                q = _fetch_one_quote(sym)
                if q and q.get('price') is not None:
                    current = float(q['price'])
            if pt is not None and current and current != 0:
                upside = (float(pt) - current) / current * 100.0
        except Exception as e:
            print('[ANALYST_CONSENSUS]', sym, e)
            pt = _ANALYST_FALLBACK_PT.get(sym)
            current = None
        if current is None:
            q = _fetch_one_quote(sym)
            if q and q.get('price') is not None:
                try:
                    current = float(q['price'])
                except Exception:
                    pass
        if pt is None:
            pt = _ANALYST_FALLBACK_PT.get(sym)
        if upside is None and pt is not None and current and current != 0:
            try:
                upside = (float(pt) - float(current)) / float(current) * 100.0
            except Exception:
                pass
        consensus.append({
            'ticker': sym,
            'buy': buy,
            'hold': hold,
            'sell': sell,
            'pt': round(float(pt), 2) if pt is not None else None,
            'current': round(float(current), 2) if current is not None else None,
            'upside_pct': round(upside, 1) if upside is not None else None,
        })
        time.sleep(0.06)
    out = {'consensus': consensus}
    _cache_set(cache_key, out)
    return out


def _build_overview():
    """Fed Funds, CPI YoY, 2s10s spread, WTI, Gold, Brent, VIX. Cached 5 min."""
    cache_key = 'overview_macro_v3'
    cached = _cache_get(cache_key, 300)
    if cached is not None:
        return cached
    data = {
        'fed_funds': None,
        'cpi_yoy': None,
        'spread_2y10y': None,
        'dgs2': None,
        'dgs10': None,
        'wti': None,
        'gold': None,
        'brent': None,
        'vix': None,
    }
    try:
        ff = _fetch_fred_series('FEDFUNDS')
        data['fed_funds'] = ff
        if data['fed_funds'] is None:
            ff2 = _fetch_fred_series('DFF')
            if ff2 is not None:
                data['fed_funds'] = ff2
        if data['fed_funds'] is None:
            data['fed_funds'] = _fed_funds_fallback_yf()
    except Exception as e:
        print('[OVERVIEW] FEDFUNDS error:', e)
    if data.get('fed_funds') is None:
        try:
            data['fed_funds'] = _fed_funds_fallback_yf()
        except Exception:
            data['fed_funds'] = 3.64  # update after FOMC if all sources fail
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
    if data.get('cpi_yoy') is None:
        data['cpi_yoy'] = 3.1
    try:
        tcurve = _fred_treasury_yield_curve()
        data['dgs2'] = tcurve['dgs2']
        data['dgs10'] = tcurve['dgs10']
        if tcurve['dgs2'] is not None and tcurve['dgs10'] is not None:
            # Same definition as treasury card: 10Y − 2Y (percentage points; negative = inverted)
            data['spread_2y10y'] = tcurve['dgs10'] - tcurve['dgs2']
    except Exception as e:
        print('[OVERVIEW] spread error:', e)
    # Commodities & VIX: prefer FRED for commodities, yfinance for VIX
    try:
        # WTI crude (USD/bbl)
        wti = _fetch_fred_series('DCOILWTICO')
        if wti is not None:
            data['wti'] = wti
        # Gold spot (USD/oz)
        gold = _fetch_fred_series('GOLDAMGBD228NLBM')
        if gold is not None:
            data['gold'] = gold
        # Brent crude (USD/bbl)
        brent = _fetch_fred_series('DCOILBRENTEU')
        if brent is not None:
            data['brent'] = brent
        # VIX from yfinance
        fi_vix = getattr(yf.Ticker('^VIX'), 'fast_info', None)
        if fi_vix is not None:
            last_vix = getattr(fi_vix, 'last_price', None)
            if last_vix is not None:
                data['vix'] = float(last_vix)
    except Exception as e:
        print('[OVERVIEW] commodities/vix error:', e)
    # Prefer yfinance futures (GC=F, CL=F, BZ=F) so overview matches /api/commodities; FRED remains fallback if yf fails
    try:
        for sym_key, ticker in [('gold', 'GC=F'), ('wti', 'CL=F'), ('brent', 'BZ=F')]:
            spot = _yfinance_commodity_spot(ticker)
            if spot is not None:
                data[sym_key] = spot
    except Exception as e:
        print('[OVERVIEW] yfinance commodity spot error:', e)
    _cache_set(cache_key, data)
    return data


def _yfinance_commodity_spot(symbol):
    """Last price for overview macro row (matches commodity futures)."""
    try:
        t = yf.Ticker(symbol)
        fi = getattr(t, 'fast_info', None)
        if fi is not None:
            lp = getattr(fi, 'last_price', None)
            if lp is not None:
                return float(lp)
        info = getattr(t, 'info', None) or {}
        if isinstance(info, dict):
            p = info.get('regularMarketPrice') or info.get('currentPrice')
            if p is not None:
                return float(p)
    except Exception:
        pass
    return None


def _yfinance_commodity_detail(symbol):
    """
    Futures row: price, daily change / %, 52w range via yfinance (regularMarket* / fast_info, fiftyTwoWeek* or 1y history).
    """
    try:
        t = yf.Ticker(symbol)
        price = prev = None
        low52 = high52 = None
        try:
            info = getattr(t, 'info', None) or {}
            if isinstance(info, dict):
                price = info.get('regularMarketPrice') or info.get('currentPrice') or info.get('postMarketPrice')
                prev = info.get('regularMarketPreviousClose') or info.get('previousClose')
                low52 = info.get('fiftyTwoWeekLow')
                high52 = info.get('fiftyTwoWeekHigh')
        except Exception:
            pass
        fi = getattr(t, 'fast_info', None)
        if price is None and fi is not None:
            lp = getattr(fi, 'last_price', None)
            if lp is not None:
                price = float(lp)
        if prev is None and fi is not None:
            pc = getattr(fi, 'previous_close', None)
            if pc is not None:
                prev = float(pc)
        if price is None:
            return None
        price = float(price)
        if prev is None:
            prev = price
        else:
            prev = float(prev)
        change = price - prev
        if prev and prev != 0:
            change_pct_val = (change / prev) * 100.0
        else:
            change_pct_val = 0.0
        sign = '+' if change_pct_val >= 0 else ''
        change_pct = f'{sign}{change_pct_val:.2f}%'
        if low52 is None or high52 is None:
            try:
                h = t.history(period='1y')
                if h is not None and not h.empty:
                    if low52 is None:
                        low52 = float(h['Low'].min())
                    if high52 is None:
                        high52 = float(h['High'].max())
            except Exception:
                pass
        row = {
            'price': round(price, 2),
            'change': round(change, 2),
            'change_pct': change_pct,
            'range_low': round(float(low52), 2) if low52 is not None else None,
            'range_high': round(float(high52), 2) if high52 is not None else None,
        }
        return row
    except Exception:
        return None


def _fetch_commodity_quotes():
    """Spot commodity prices from yfinance futures (GC=F, SI=F, CL=F, BZ=F); FRED fallback if needed. Cached 5 min."""
    cache_key = 'commodities_quotes_v2'
    cached = _cache_get(cache_key, 300)
    if cached is not None:
        return cached
    out = {}
    for key, sym in [('gold', 'GC=F'), ('silver', 'SI=F'), ('wti', 'CL=F'), ('brent', 'BZ=F')]:
        row = _yfinance_commodity_detail(sym)
        if row:
            out[key] = row
    # SI=F: yfinance occasionally reports implausible 52w high (wrong contract); real spot silver ~$28–$80/oz
    if out.get('silver'):
        rh, rl = out['silver'].get('range_high'), out['silver'].get('range_low')
        try:
            if rh is not None and float(rh) > 100:
                print('[COMMODITIES] silver SI=F: dropping unreliable 52w range (high=%s)' % rh)
                out['silver']['range_low'] = None
                out['silver']['range_high'] = None
            if rl is not None and float(rl) < 0:
                out['silver']['range_low'] = None
                out['silver']['range_high'] = None
        except Exception:
            out['silver']['range_low'] = None
            out['silver']['range_high'] = None
    fred_map = {
        'gold': 'GOLDAMGBD228NLBM',
        'silver': 'SLVPRUSD',
        'wti': 'DCOILWTICO',
        'brent': 'DCOILBRENTEU',
    }
    for key, sid in fred_map.items():
        if key in out and out[key].get('price') is not None:
            continue
        try:
            val = _fetch_fred_series(sid)
            if val is not None:
                out[key] = {
                    'price': round(float(val), 2),
                    'change': None,
                    'change_pct': '—',
                    'range_low': None,
                    'range_high': None,
                }
        except Exception:
            continue
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


# Static fallback when FRED release calendar is thin. UPDATE MONTHLY or replace with API (FRED release_dates,
# Trading Economics, etc.) — dates must stay >= "today" or they are filtered server-side.
_ECON_CALENDAR_FALLBACK = [
    {"date": "2026-03-27", "event": "GDP (Q4 Final)", "importance": "HIGH", "forecast": "", "previous": "", "country": "US"},
    {"date": "2026-03-28", "event": "Core PCE Deflator", "importance": "HIGH", "forecast": "", "previous": "", "country": "US"},
    {"date": "2026-04-01", "event": "ISM Manufacturing PMI", "importance": "HIGH", "forecast": "", "previous": "", "country": "US"},
    {"date": "2026-04-04", "event": "NFP + Unemployment", "importance": "HIGH", "forecast": "", "previous": "", "country": "US"},
    {"date": "2026-04-10", "event": "CPI (Mar)", "importance": "HIGH", "forecast": "", "previous": "", "country": "US"},
    {"date": "2026-04-30", "event": "FOMC Rate Decision", "importance": "HIGH", "forecast": "", "previous": "", "country": "US"},
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
    # Only upcoming / today (avoid showing past releases as "upcoming")
    today_s = today.isoformat()
    events = [e for e in events if (e.get('date') or '') >= today_s]
    if len(events) < 2:
        events = [e for e in _ECON_CALENDAR_FALLBACK if (e.get('date') or '') >= today_s]
        source = 'scheduled'
    result = {"events": events, "source": source, "last_updated": datetime.utcnow().isoformat()}
    if source == 'scheduled':
        result["last_verified"] = "2026-03-13"
    _cache_set(cache_key, result)
    return result


def _build_indicators():
    """GDP, unemployment, retail, housing, PMI, VIX, yield curve, CPI, real rate. Cache 30 min."""
    cache_key = 'indicators_v2'
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
    # REST fallbacks when fredapi missing or series empty (same FRED IDs as above)
    if out.get("gdp") is None:
        g = _fetch_fred_series('A191RL1Q225SBEA')
        if g is not None:
            out["gdp"] = round(float(g), 2)
    if out.get("unemployment") is None:
        u = _fetch_fred_series('UNRATE')
        if u is not None:
            out["unemployment"] = round(float(u), 2)
    if out.get("retail_mom") is None:
        obs = _fetch_fred_observations('RSAFS', 2)
        if len(obs) >= 2 and obs[1] != 0:
            out["retail_mom"] = round((obs[0] / obs[1] - 1.0) * 100.0, 2)
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
    # Align with Economic Indicators card when FRED + REST both fail — update quarterly
    if out.get("gdp") is None:
        out["gdp"] = 2.8
    if out.get("unemployment") is None:
        out["unemployment"] = 3.9
    if out.get("retail_mom") is None:
        out["retail_mom"] = 0.6
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
        if re.search(r'S-Sale|disposition|sold', summary, re.I):
            tx_type = 'Sell'
        elif re.search(r'P-Purchase|acquisition|bought|purchase', summary, re.I):
            tx_type = 'Buy'
    shares = None
    value = None
    if summary:
        sm = re.search(r'(?:shares|qty|quantity)[:\s]+([0-9,]+)', summary, re.I)
        if sm:
            try:
                shares = int(sm.group(1).replace(',', ''))
            except Exception:
                shares = None
        vm = re.search(r'(?:value|amount|total)[:\s]+\$?([0-9,]+)', summary, re.I)
        if vm:
            try:
                value = float(vm.group(1).replace(',', ''))
            except Exception:
                value = None
    return {'name': name or 'Unknown', 'transaction_type': tx_type, 'shares': shares, 'value': value}


def _build_insiders_yfinance(symbol):
    """Insider transactions from yfinance (preferred when EDGAR RSS lacks detail)."""
    symbol = (symbol or '').strip().upper()
    if not symbol:
        return None
    try:
        t = yf.Ticker(symbol)
        df = getattr(t, 'insider_transactions', None)
        if df is None or getattr(df, 'empty', True):
            return None
        transactions = []

        def _cell(row, *names):
            for n in names:
                if n in row.index:
                    v = row[n]
                    if v is not None and (not hasattr(v, 'item') or str(type(v)) != "<class 'pandas._libs.tslibs.nattype.NaTType'>"):
                        try:
                            if hasattr(v, 'item'):
                                v = v.item()
                        except Exception:
                            pass
                        if v is not None and str(v).lower() != 'nan':
                            return v
            return None

        for _, row in df.head(25).iterrows():
            name = _cell(row, 'Insider', 'insider', 'Owner', 'owner')
            if name is None:
                try:
                    name = row.iloc[0] if len(row) > 0 else '—'
                except Exception:
                    name = '—'
            position = _cell(row, 'Position', 'position', 'Relationship', 'relationship', 'Title', 'title', 'Type', 'type') or '—'
            shares = _cell(row, 'Shares', 'shares', 'Share', 'share')
            value = _cell(row, 'Value', 'value')
            tx_raw = _cell(row, 'Transaction', 'transaction', 'Text', 'text') or ''
            tstr = str(tx_raw).lower()
            if 'buy' in tstr or 'purchase' in tstr or 'acquisition' in tstr:
                transaction_type = 'Buy'
            elif 'sale' in tstr or 'sell' in tstr or 'disposition' in tstr:
                transaction_type = 'Sell'
            else:
                transaction_type = str(tx_raw)[:24] if tx_raw else '—'
            d = _cell(row, 'Start Date', 'startDate', 'Date', 'date')
            date_str = ''
            if d is not None:
                try:
                    if hasattr(d, 'strftime'):
                        date_str = d.strftime('%Y-%m-%d')
                    else:
                        date_str = str(d)[:10]
                except Exception:
                    date_str = str(d)[:10]
            try:
                sh = int(float(shares)) if shares is not None and str(shares) != 'nan' else None
            except Exception:
                sh = None
            try:
                valf = float(value) if value is not None and str(value) != 'nan' else None
            except Exception:
                valf = None
            transactions.append({
                'name': str(name).strip() or '—',
                'position': str(position).strip() if position else '—',
                'transaction_type': transaction_type,
                'shares': sh,
                'value': valf,
                'date': date_str,
                'source': 'yfinance',
                'url': '',
            })
        if not transactions:
            return None
        return {'symbol': symbol, 'transactions': transactions}
    except Exception as e:
        print('[INSIDERS] yfinance', symbol, e)
        return None


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
    is_future = sym.endswith("=F")
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
        # For futures and tickers missing quote, fall back to yfinance regularMarketPrice / previousClose
        price_fallback = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
        chg_fallback = info.get('regularMarketChange')
        pct_fallback = info.get('regularMarketChangePercent')
        out['A'] = {
            'price': _safe_num(q.get('price') if q else price_fallback),
            'change': _safe_num(q.get('chg') if q else chg_fallback),
            'change_pct': _safe_num(q.get('pct') if q else pct_fallback),
            'volume': _safe_num(info.get('volume') or info.get('regularMarketVolume')),
            'avg_volume': _safe_num(info.get('averageVolume')),
            'market_cap': None if is_future else _safe_num(info.get('marketCap')),
            'enterprise_value': _safe_num(info.get('enterpriseValue')),
            'high_52w': _safe_num(info.get('fiftyTwoWeekHigh')),
            'low_52w': _safe_num(info.get('fiftyTwoWeekLow')),
            'day_high': _safe_num(info.get('dayHigh')),
            'day_low': _safe_num(info.get('dayLow')),
            'open': _safe_num(info.get('open')),
            'previous_close': _safe_num(info.get('previousClose')),
            'beta': None if is_future else _safe_num(info.get('beta')),
        }
        if out['A'].get('high_52w') and out['A'].get('low_52w') and out['A'].get('price'):
            h, l, p = out['A']['high_52w'], out['A']['low_52w'], out['A']['price']
            if h != l:
                out['A']['position_52w_pct'] = (p - l) / (h - l) * 100.0
        # YTD performance (for commodities and stocks)
        try:
            if not hist.empty:
                first = hist['Close'].iloc[0]
                last = hist['Close'].iloc[-1]
                if first:
                    out['A']['ytd_pct'] = (last / first - 1.0) * 100.0
        except Exception:
            pass
    except Exception:
        pass

    # Override futures pricing with FRED-based commodity quotes where available
    if is_future:
        try:
            quotes = _fetch_commodity_quotes() or {}
            key_map = {'GC=F': 'gold', 'SI=F': 'silver', 'CL=F': 'wti', 'BZ=F': 'brent'}
            k = key_map.get(sym)
            if k and quotes.get(k) and quotes[k].get('price') is not None:
                out['A']['price'] = float(quotes[k]['price'])
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

        # Treasury / FRED: 2Y, 10Y, 30Y (same series as overview spread)
        if path == '/api/treasury':
            self._send_json(_fred_treasury_yield_curve())
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

        # Equities tab: global market indices (yfinance; 60s cache)
        if path == '/api/market-indices':
            data = _fetch_market_indices_equities()
            self._send_json(data)
            return

        # Equities tab: analyst consensus PT / upside (yfinance targetMeanPrice; 60s cache)
        if path == '/api/analyst-consensus':
            data = _fetch_analyst_consensus_equities()
            self._send_json(data)
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
            cache_key = f'earnings_yf_v2_{symbol}'
            cached = _cache_get(cache_key, 3600)
            if cached is not None:
                self._send_json(cached)
                return
            out = {'symbol': symbol, 'next_earnings': None, 'history': []}
            try:
                t = yf.Ticker(symbol)
                info = getattr(t, 'info', None) or {}
                today = datetime.utcnow().date()

                def _row_to_date(idx):
                    try:
                        if hasattr(idx, 'date'):
                            return idx.date()
                        return datetime.fromisoformat(str(idx)[:10]).date()
                    except Exception:
                        return None

                # Next date: earningsTimestamp*, then calendar, then earnings_dates / get_earnings_dates future row
                for ek in ('earningsTimestamp', 'earningsTimestampStart'):
                    ts = info.get(ek)
                    if ts:
                        try:
                            ds = datetime.utcfromtimestamp(int(ts)).date()
                            if ds >= today:
                                out['next_earnings'] = ds.strftime('%Y-%m-%d')
                                break
                        except Exception:
                            try:
                                ds = datetime.fromisoformat(str(ts)[:10]).date()
                                if ds >= today:
                                    out['next_earnings'] = ds.strftime('%Y-%m-%d')
                                    break
                            except Exception:
                                pass
                if out['next_earnings'] is None:
                    cal = getattr(t, 'calendar', None)
                    if cal is not None:
                        try:
                            if hasattr(cal, 'columns'):
                                for col in ('Earnings Date', 'Earnings Average', 'Event'):
                                    if col in cal.columns:
                                        cell = cal[col].iloc[0] if len(cal) > 0 else None
                                        if cell is not None:
                                            d = _row_to_date(cell) if not isinstance(cell, date) else cell
                                            if d and d >= today:
                                                out['next_earnings'] = d.strftime('%Y-%m-%d')
                                                break
                            if out['next_earnings'] is None and hasattr(cal, 'iloc') and not cal.empty:
                                for idx in (cal.index.tolist() if hasattr(cal.index, 'tolist') else []):
                                    d = _row_to_date(idx)
                                    if d and d >= today:
                                        out['next_earnings'] = d.strftime('%Y-%m-%d')
                                        break
                        except Exception:
                            pass
                hist_candidates = []

                def _yf_val_missing(v):
                    if v is None:
                        return True
                    if isinstance(v, float) and v != v:
                        return True
                    return str(v).lower() in ('nan', 'nat', 'none', '')

                def _df_to_hist(df2):
                    rows = []
                    if df2 is None or getattr(df2, 'empty', True):
                        return rows
                    for idx, row in df2.iterrows():
                        d = _row_to_date(idx)
                        if d is None or d > today:
                            continue

                        def gv(*keys):
                            for k in keys:
                                if k not in row.index:
                                    continue
                                v = row[k]
                                if not _yf_val_missing(v):
                                    return v
                            return None

                        est = gv('EPS Estimate', 'epsEstimate')
                        act = gv('Reported EPS', 'EPS Actual', 'epsActual')
                        surprise = gv('Surprise(%)', 'epsSurprisePct')
                        if not _yf_val_missing(surprise):
                            try:
                                surprise = float(surprise)
                            except Exception:
                                surprise = None
                        else:
                            surprise = None
                        if surprise is None and est not in (None, 0) and act is not None:
                            try:
                                surprise = (float(act) - float(est)) / float(est) * 100.0
                            except Exception:
                                pass
                        rev_e = gv('Revenue Estimate', 'revenueEstimate')
                        rev_a = gv('Revenue Actual', 'revenueActual')
                        rows.append({
                            'date': d.strftime('%Y-%m-%d'),
                            'eps_estimate': est,
                            'eps_actual': act,
                            'eps_surprise_pct': surprise,
                            'revenue_estimate': rev_e,
                            'revenue_actual': rev_a,
                        })
                    rows.sort(key=lambda x: x.get('date') or '', reverse=True)
                    return rows

                try:
                    get_ed = getattr(t, 'get_earnings_dates', None)
                    if callable(get_ed):
                        df2 = get_ed(limit=24)
                        hist_candidates = _df_to_hist(df2)
                        if out['next_earnings'] is None and df2 is not None and not getattr(df2, 'empty', True):
                            for idx, row in df2.iterrows():
                                d = _row_to_date(idx)
                                if d is None:
                                    continue
                                if d >= today:
                                    rep = row['Reported EPS'] if 'Reported EPS' in row.index else None
                                    if _yf_val_missing(rep):
                                        out['next_earnings'] = d.strftime('%Y-%m-%d')
                                        break
                except Exception:
                    pass

                if not hist_candidates:
                    eh = getattr(t, 'earnings_history', None)
                    if eh is not None and not getattr(eh, 'empty', True):
                        try:
                            for _, row in eh.tail(6).iterrows():
                                est = row.get('epsEstimate') if hasattr(row, 'get') else None
                                act = row.get('epsActual') if hasattr(row, 'get') else None
                                surprise = None
                                if est not in (None, 0) and act is not None:
                                    try:
                                        surprise = (float(act) - float(est)) / float(est) * 100.0
                                    except Exception:
                                        pass
                                hist_candidates.append({
                                    'date': str(row.get('startdatetime', ''))[:10] if hasattr(row, 'get') else '',
                                    'eps_estimate': est,
                                    'eps_actual': act,
                                    'eps_surprise_pct': surprise,
                                    'revenue_estimate': row.get('revenueEstimate') if hasattr(row, 'get') else None,
                                    'revenue_actual': row.get('revenueActual') if hasattr(row, 'get') else None,
                                })
                        except Exception:
                            pass

                if hist_candidates:
                    out['history'] = hist_candidates[:6]
                else:
                    te = info.get('trailingEps')
                    fe = info.get('forwardEps')
                    if te is not None or fe is not None:
                        out['history'].append({
                            'date': 'TTM',
                            'eps_estimate': fe,
                            'eps_actual': te,
                            'eps_surprise_pct': None,
                            'revenue_estimate': None,
                            'revenue_actual': None,
                        })

                if out['next_earnings'] is None:
                    ed = getattr(t, 'earnings_dates', None)
                    if ed is not None and not getattr(ed, 'empty', True):
                        for idx in ed.index:
                            d = _row_to_date(idx)
                            if d and d >= today:
                                out['next_earnings'] = d.strftime('%Y-%m-%d')
                                break
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
            data = _build_insiders_yfinance(symbol)
            if not data or not data.get('transactions'):
                data = _build_insiders_edgar(symbol)
            self._send_json(data)
            return

        # Institutional ownership (yfinance)
        if path == '/api/institutional':
            symbol = (qs.get('symbol') or [''])[0].strip().upper()
            if not symbol:
                self._send_json({'error': 'missing symbol'}, 400)
                return
            cache_key = f'inst_yf_v2_{symbol}'
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
                # Free yfinance has no prior-quarter institutional % — omit QoQ (no fake 0%)
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
