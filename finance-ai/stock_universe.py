"""
Stock universe: S&P 500 + S&P 400. Loaded once at startup, cached in memory.
Fallback to sp500_fallback.json if Wikipedia fetch fails.
"""
import os
import json

_UNIVERSE = None  # {ticker: {name, sector, industry, index}}


def _load_fallback():
    """Load from bundled sp500_fallback.json in same directory."""
    path = os.path.join(os.path.dirname(__file__), 'sp500_fallback.json')
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return {}
    out = {}
    for item in data:
        ticker = (item.get('ticker') or item.get('Symbol') or '').strip().upper()
        if not ticker:
            continue
        out[ticker] = {
            'name': item.get('name') or item.get('Security') or item.get('company') or ticker,
            'sector': item.get('sector') or item.get('GICS Sector') or '',
            'industry': item.get('industry') or item.get('GICS Sub-Industry') or '',
            'index': item.get('index') or 'S&P 500',
        }
    return out


def load_stock_universe():
    """Fetch S&P 500 and S&P 400 from Wikipedia; on failure use fallback JSON. Store in memory."""
    global _UNIVERSE
    if _UNIVERSE is not None:
        return _UNIVERSE

    try:
        import pandas as pd
    except ImportError:
        _UNIVERSE = _load_fallback()
        return _UNIVERSE

    out = {}
    # S&P 500
    try:
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = tables[0]
        # Common column names: Symbol, Security, GICS Sector, GICS Sub-Industry
        sym_col = 'Symbol' if 'Symbol' in df.columns else df.columns[0]
        name_col = 'Security' if 'Security' in df.columns else (df.columns[1] if len(df.columns) > 1 else sym_col)
        sec_col = [c for c in df.columns if 'GICS' in c and 'Sector' in c]
        ind_col = [c for c in df.columns if 'Sub-Industry' in c or 'Industry' in c]
        for _, row in df.iterrows():
            ticker = str(row.get(sym_col, '')).strip().upper()
            if not ticker or ticker in ('SYMBOL', 'TICKER', 'NAN', ''):
                continue
            out[ticker] = {
                'name': str(row.get(name_col, ticker)),
                'sector': str(row.get(sec_col[0], '')) if sec_col else '',
                'industry': str(row.get(ind_col[0], '')) if ind_col else '',
                'index': 'S&P 500',
            }
    except Exception:
        out = _load_fallback()
        _UNIVERSE = out
        return _UNIVERSE

    # S&P 400 (mid cap)
    try:
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_400_companies')
        df = tables[0]
        sym_col = 'Symbol' if 'Symbol' in df.columns else df.columns[0]
        name_col = 'Security' if 'Security' in df.columns else (df.columns[1] if len(df.columns) > 1 else sym_col)
        sec_col = [c for c in df.columns if 'GICS' in c and 'Sector' in c]
        ind_col = [c for c in df.columns if 'Sub-Industry' in c or 'Industry' in c]
        for _, row in df.iterrows():
            ticker = str(row.get(sym_col, '')).strip().upper()
            if not ticker or ticker in ('SYMBOL', 'TICKER', 'NAN', ''):
                continue
            if ticker not in out:
                out[ticker] = {
                    'name': str(row.get(name_col, ticker)),
                    'sector': str(row.get(sec_col[0], '')) if sec_col else '',
                    'industry': str(row.get(ind_col[0], '')) if ind_col else '',
                    'index': 'S&P 400',
                }
    except Exception:
        pass

    if not out:
        out = _load_fallback()
    _UNIVERSE = out
    return _UNIVERSE


def get_stock_universe():
    """Return the in-memory dict {ticker: {name, sector, industry, index}}."""
    if _UNIVERSE is None:
        load_stock_universe()
    return _UNIVERSE or {}


def search_universe(query, limit=10):
    """Case-insensitive search by ticker prefix and company name. Returns list of {ticker, name, sector, industry, index}."""
    u = get_stock_universe()
    if not query or not query.strip():
        return []
    q = query.strip().upper()
    q_lower = query.strip().lower()
    results = []
    # Ticker prefix match first
    for ticker, info in u.items():
        if ticker.startswith(q) or q in ticker:
            results.append({
                'ticker': ticker,
                'name': info.get('name', ''),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'index': info.get('index', ''),
            })
        elif q_lower in (info.get('name') or '').lower():
            results.append({
                'ticker': ticker,
                'name': info.get('name', ''),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'index': info.get('index', ''),
            })
    # Sort: exact ticker match first, then prefix, then name match
    def key(r):
        t = r['ticker']
        if t == q:
            return (0, t)
        if t.startswith(q):
            return (1, t)
        return (2, t)
    results.sort(key=key)
    return results[:limit]
