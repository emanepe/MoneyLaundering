#!/usr/bin/env python3
#
# Changes vs previous:
# - ONE-TIME per-sheet cache of existing dates (so we don't call col_values repeatedly)
# - Rate-limited gspread calls with exponential backoff on 429
# - Align-bybit-interest does a single A2:D read

import os, time, hmac, hashlib, bisect, requests, math
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

# ===================== CONFIG =====================
SHEET_ID = "1Ifaglxf5koRNEP8gK-RTm-97mMisrCS_6F1uddIxtcY"
SCOPES   = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

UA = {"User-Agent": "AppendOnlyRateCollector/2.1-quota"}

BINANCE_TOKENS = ['APT', 'OM', 'ENA', 'PENGU', 'SIGN']
BYBIT_TOKENS   = ['ME', 'ENA', 'BERA', 'PENGU']

BYBIT_CONTRACT_MAP: Dict[str, List[Tuple[str,str]]] = {}

INTERVALS = {'APT': 3, 'OM': 6, 'ENA': 6, 'PENGU': 6, 'ME': 24, 'BERA': 12, 'SIGN': 3}

HEADERS = [
    'Date','Funding Rate APR (%)','Funding Rate APR (%) reverse','Interest Rate APR (%)',
    'Spot Price (USDT)','Per-Coin Max Loan USD (API)','Collateral Max Limit USD (USDT)',
    'USDT initialLTV','USDT liquidationLTV','Collateral Needed (USDT) for Max Loan Amount',
    'Hypothetical liquidationStartingPrice (USDT/token)'
]

APPEND_BYBIT_SNAPSHOT_ROW = True
BYBIT_ALIGN_MAX_GAP_HOURS = 48

# ---------- gspread helpers with backoff ----------
def gs_retry(fn, *args, **kwargs):
    """Run a gspread call with exponential backoff on 429."""
    delay = 1.0
    for i in range(6):  # ~1+2+4+8+16+32 = 63s worst-case
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                time.sleep(delay)
                delay *= 2
                continue
            raise
    # one last try (raise if fails)
    return fn(*args, **kwargs)

def get_client():
    creds = Credentials.from_service_account_file('rateprinter-f4189491ecfc.json', scopes=SCOPES)
    return gspread.authorize(creds)

def ensure_sheet(ss, title: str):
    try:
        return gs_retry(ss.worksheet, title)
    except gspread.exceptions.WorksheetNotFound:
        ws = gs_retry(ss.add_worksheet, title=title, rows=5000, cols=len(HEADERS))
        gs_retry(ws.update, [HEADERS], 'A1')
        return ws

def ensure_headers(ws):
    first = gs_retry(ws.row_values, 1)
    if len(first) == 0 or first[:len(HEADERS)] != HEADERS:
        gs_retry(ws.update, [HEADERS], 'A1')

def _parse_dt(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%m/%d/%Y %H:%M:%S")
    except:
        return None

def _to_ms(s: str) -> Optional[int]:
    dt = _parse_dt(s)
    return int(dt.timestamp()*1000) if dt else None

# Per-sheet cache so we read dates only once
_EXISTING_DATES_CACHE: Dict[str, set] = {}
_LAST_FUNDING_DT_CACHE: Dict[str, Optional[datetime]] = {}

def get_existing_dates_set(ws) -> set:
    title = ws.title
    if title in _EXISTING_DATES_CACHE:
        return _EXISTING_DATES_CACHE[title]
    vals = gs_retry(ws.col_values, 1)  # A
    s = set(v for v in vals[1:] if v)  # skip header
    _EXISTING_DATES_CACHE[title] = s
    return s

def get_last_funding_dt(ws) -> Optional[datetime]:
    title = ws.title
    if title in _LAST_FUNDING_DT_CACHE:
        return _LAST_FUNDING_DT_CACHE[title]
    vals = gs_retry(ws.get, 'A2:B')
    last_dt = None
    for row in vals:
        if not row: continue
        ds = row[0] if len(row)>0 else ""
        b  = row[1] if len(row)>1 else ""
        if ds and str(b).strip() != "":
            dt = _parse_dt(ds)
            if dt and (last_dt is None or dt > last_dt):
                last_dt = dt
    _LAST_FUNDING_DT_CACHE[title] = last_dt
    return last_dt

def append_new_unique_rows(ws, rows: List[List]):
    if not rows: return
    existing = get_existing_dates_set(ws)
    to_add = [r for r in rows if r and str(r[0]) not in existing]
    if not to_add: return
    out = []
    for r in to_add:
        row = list(r)[:11]
        while len(row) < 11: row.append("")
        out.append(row)
        existing.add(str(row[0]))  # update cache so future calls don't reread
    gs_retry(ws.append_rows, out, value_input_option="USER_ENTERED")

# ===================== HTTP & time =====================
def _http_json(url: str, params: dict = None, headers: dict = None, retries: int = 3, sleep_sec: float = 0.6):
    hdr = dict(UA)
    if headers: hdr.update(headers)
    for _ in range(retries):
        try:
            r = requests.get(url, params=params, headers=hdr, timeout=12)
            if r.status_code != 200:
                # For debugging
                # print(f"⚠️ HTTP {r.status_code} {url} params={params} -> {r.text[:160]}")
                time.sleep(sleep_sec); continue
            return r.json()
        except Exception:
            time.sleep(sleep_sec)
    return None

def now_ms(): return int(time.time()*1000)
def minute_open(ts_ms: int) -> int: return ts_ms - (ts_ms % 60000)

def fmt_dt(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms/1000)
    return f"{dt.month}/{dt.day}/{dt.year} {dt.hour}:{dt.minute:02d}:{dt.second:02d}"

# ===================== Spot (Binance/Bybit) =====================
_SPOT_CACHE_BN: Dict[Tuple[str,int], float] = {}
_SYMBOL_CACHE_BN: Dict[str, Optional[str]] = {}
_SPOT_QUOTES = ["USDT", "FDUSD", "TUSD", "USD", "BUSD"]

def _bn_kline(symbol: str, mo: int) -> Optional[float]:
    j = _http_json("https://api.binance.com/api/v3/klines",
                   params={"symbol":symbol,"interval":"1m","startTime":mo,"endTime":mo+60000,"limit":1})
    if isinstance(j, list) and j:
        return float(j[0][4])
    return None

def _bn_resolve_symbol(token: str, probe_ms: int) -> Optional[str]:
    if token in _SYMBOL_CACHE_BN: return _SYMBOL_CACHE_BN[token]
    mo = minute_open(probe_ms)
    for q in _SPOT_QUOTES:
        sym = f"{token}{q}"
        px = _bn_kline(sym, mo)
        if px is not None:
            _SYMBOL_CACHE_BN[token] = sym
            return sym
    _SYMBOL_CACHE_BN[token] = None
    return None

def spot_binance(token: str, ts_ms: int) -> Optional[float]:
    mo = minute_open(ts_ms)
    sym = _SYMBOL_CACHE_BN.get(token) or _bn_resolve_symbol(token, ts_ms)
    if not sym: return None
    ck = (sym, mo)
    if ck in _SPOT_CACHE_BN: return _SPOT_CACHE_BN[ck]
    px = _bn_kline(sym, mo)
    if px is not None: _SPOT_CACHE_BN[ck] = px
    return px

def spot_bybit_from_contract_symbol(contract_symbol: str, ts_ms: int) -> Optional[float]:
    mo = minute_open(ts_ms)
    if contract_symbol.endswith("USDT"):
        spot_sym = contract_symbol
    elif contract_symbol.endswith("USDC"):
        spot_sym = contract_symbol[:-4] + "USDT"
    elif contract_symbol.endswith("USD"):
        spot_sym = contract_symbol[:-3] + "USDT"
    else:
        spot_sym = contract_symbol + "USDT"
    j = _http_json("https://api.bybit.com/v5/market/kline",
                   params={"category":"spot","symbol":spot_sym,"interval":"1","start":mo,"end":mo+60000,"limit":1})
    try:
        if j and j.get("retCode")==0 and j.get("result") and j["result"].get("list"):
            return float(j["result"]["list"][0][4])
    except: pass
    return None

# ===================== Loan math =====================
def collateral_needed_usdt(loan_usd: float, init_ltv: float) -> Optional[float]:
    if not loan_usd or not init_ltv or init_ltv <= 0: return None
    return loan_usd / init_ltv

def liquidation_start_price(spot: float, init_ltv: float, liq_ltv: float) -> Optional[float]:
    if not spot or spot <= 0 or not init_ltv or not liq_ltv: return None
    return spot * (liq_ltv / init_ltv)

# ===================== Binance APIs =====================
def _sign(params: Dict[str,str]) -> Dict[str,str]:
    qs  = "&".join(f"{k}={params[k]}" for k in params)
    sig = hmac.new(BINANCE_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return {**params, "signature": sig}

def bn_funding(token: str) -> List[dict]:
    j = _http_json("https://fapi.binance.com/fapi/v1/fundingRate",
                   params={"symbol":f"{token}USDT","limit":1000})
    return j if isinstance(j, list) else []

def bn_interest_history(token: str, max_pages: int = 10, page_size: int = 100) -> List[Tuple[int,float]]:
    out = []
    endpoint = "https://api.binance.com/sapi/v2/loan/interestRateHistory"
    for page in range(1, max_pages+1):
        params = _sign({"coin":token,"current":page,"limit":page_size,"recvWindow":60000,"timestamp":now_ms()})
        j = _http_json(endpoint, params=params, headers={"X-MBX-APIKEY":BINANCE_API_KEY})
        if not isinstance(j, dict) or not j.get("rows"): break
        for r in j["rows"]:
            try:
                out.append((int(r["time"]), float(r["annualizedInterestRate"])))  # decimal APR
            except: pass
        if len(j["rows"]) < page_size: break
        time.sleep(0.2)
    out.sort(key=lambda x:x[0])
    return out

def pick_closest_rate(hist: List[Tuple[int,float]], ts_ms: int) -> Optional[float]:
    if not hist: return None
    times = [t for (t, _) in hist]
    pos = bisect.bisect_left(times, ts_ms)
    cands = []
    if pos > 0: cands.append(hist[pos-1])
    if pos < len(hist): cands.append(hist[pos])
    best = min(cands, key=lambda p: (abs(p[0]-ts_ms), 0 if p[0] <= ts_ms else 1))
    return best[1]

def bn_flexible_loanable(token: str) -> Dict[str,float]:
    j = _http_json("https://api.binance.com/sapi/v2/loan/flexible/loanable/data",
                   params=_sign({"loanCoin":token,"timestamp":now_ms()}),
                   headers={"X-MBX-APIKEY":BINANCE_API_KEY})
    if isinstance(j, dict) and j.get("rows"):
        row = j["rows"][0]
        return {"flexibleMaxLimit": float(row.get("flexibleMaxLimit",0.0))}
    return {"flexibleMaxLimit": 0.0}

def bn_collateral_usdt() -> Dict[str,float]:
    j = _http_json("https://api.binance.com/sapi/v2/loan/flexible/collateral/data",
                   params=_sign({"collateralCoin":"USDT","timestamp":now_ms()}),
                   headers={"X-MBX-APIKEY":BINANCE_API_KEY})
    if isinstance(j, dict) and j.get("rows"):
        row = j["rows"][0]
        return {
            "initialLTV": float(row.get("initialLTV",0.0)),
            "liquidationLTV": float(row.get("liquidationLTV",0.0)),
            "collateralMaxLimit": float(row.get("maxLimit",0.0)),
        }
    return {"initialLTV":0.0,"liquidationLTV":0.0,"collateralMaxLimit":0.0}

# ===================== Bybit APIs =====================
def by_resolve_contracts(token: str) -> List[Tuple[str,str]]:
    t = token.upper()
    if token in BYBIT_CONTRACT_MAP and BYBIT_CONTRACT_MAP[token]:
        return BYBIT_CONTRACT_MAP[token][:]

    out: List[Tuple[str,str]] = []

    j = _http_json("https://api.bybit.com/v5/market/instruments-info",
                   params={"category":"linear","symbol":f"{t}USDT"})
    if j and j.get("retCode")==0 and j.get("result"):
        lst = j["result"].get("list") or []
        if lst and lst[0].get("symbol"):
            out.append(("linear", lst[0]["symbol"]))
    if not out:
        j2 = _http_json("https://api.bybit.com/v5/market/instruments-info",
                        params={"category":"linear"})
        if j2 and j2.get("retCode")==0 and j2.get("result"):
            lst = j2["result"].get("list") or []
            usdt = [it["symbol"] for it in lst if it.get("symbol","").startswith(t) and it["symbol"].endswith("USDT")]
            usdc = [it["symbol"] for it in lst if it.get("symbol","").startswith(t) and it["symbol"].endswith("USDC")]
            for s in usdt: out.append(("linear", s))
            for s in usdc: out.append(("linear", s))
    j3 = _http_json("https://api.bybit.com/v5/market/instruments-info",
                    params={"category":"inverse"})
    if j3 and j3.get("retCode")==0 and j3.get("result"):
        lst = j3["result"].get("list") or []
        usd = [it["symbol"] for it in lst if it.get("symbol","").startswith(t) and it["symbol"].endswith("USD")]
        for s in usd: out.append(("inverse", s))

    seen = set(); dedup = []
    for cat,sym in out:
        k = (cat,sym)
        if k not in seen:
            seen.add(k); dedup.append((cat,sym))
    return dedup

def by_funding_history_from(category: str, symbol: str) -> List[dict]:
    out, cursor = [], None
    while True:
        params = {"category":category, "symbol":symbol, "limit":200}
        if cursor: params["cursor"] = cursor
        j = _http_json("https://api.bybit.com/v5/market/funding/history", params=params)
        if not j or j.get("retCode") != 0:
            break
        res = j.get("result") or {}
        lst = res.get("list") or []
        if not lst: break
        out.extend(lst)
        cursor = res.get("nextPageCursor")
        if not cursor: break
        time.sleep(0.15)
    return out

def by_pick_contract_with_rows(token: str) -> Tuple[List[dict], Optional[Tuple[str,str]]]:
    candidates = by_resolve_contracts(token)
    if not candidates: return [], None
    for cat, sym in candidates:
        rows = by_funding_history_from(cat, sym)
        if rows:
            return rows, (cat, sym)
    return [], None

def by_collateral_usdt(vip="VIP0") -> Dict[str,float]:
    j = _http_json("https://api.bybit.com/v5/crypto-loan/collateral-data",
                   params={"currency":"USDT","vipLevel":vip})
    if not j or j.get("retCode") != 0:
        return {"initialLTV":0.0,"liquidationLTV":0.0,"collateralMaxLimit":0.0}
    vip_list = ((j.get("result") or {}).get("vipCoinList") or [])
    if not vip_list:
        return {"initialLTV":0.0,"liquidationLTV":0.0,"collateralMaxLimit":0.0}
    items = (vip_list[0].get("list") or [])
    row = next((r for r in items if r.get("currency")=="USDT"), (items[0] if items else {}))
    try: init=float(row.get("initialLTV",0.0))
    except: init=0.0
    try: liq=float(row.get("liquidationLTV",0.0))
    except: liq=0.0
    try: cap=float(row.get("maxLimit",0.0))
    except: cap=0.0
    return {"initialLTV":init,"liquidationLTV":liq,"collateralMaxLimit":cap}

# ===================== Row building =====================
def funding_apr_percent(per_period: float, token: str) -> float:
    periods = INTERVALS.get(token, 3)
    return (per_period * 100.0) * periods * 365.0

def build_binance_rows_since(token: str, after_dt: Optional[datetime], bn_usdt: Dict[str,float]) -> List[List]:
    fund = bn_funding(token)
    fund.sort(key=lambda r: int(r['fundingTime']))  # old->new
    hist = bn_interest_history(token)               # [(t, decimal APR)]
    init_ltv = bn_usdt.get("initialLTV",0.0)
    liq_ltv  = bn_usdt.get("liquidationLTV",0.0)
    cap_usdt = bn_usdt.get("collateralMaxLimit",0.0)
    loanable = bn_flexible_loanable(token)
    per_coin_max_usd = float(loanable.get("flexibleMaxLimit",0.0))

    rows = []
    for r in fund:
        t_ms = int(r['fundingTime'])
        ts   = fmt_dt(t_ms)
        t_dt = _parse_dt(ts)
        if after_dt and not (t_dt and t_dt > after_dt): continue
        b = funding_apr_percent(float(r['fundingRate']), token)
        c = -b
        d_dec = pick_closest_rate(hist, t_ms)
        d = (d_dec * 100.0) if d_dec is not None else None
        spot = spot_binance(token, t_ms)
        j_collat = collateral_needed_usdt(per_coin_max_usd, init_ltv) if per_coin_max_usd else None
        k_liq    = liquidation_start_price(spot, init_ltv, liq_ltv) if spot else None
        rows.append([ts, b, c, d, spot, per_coin_max_usd, cap_usdt, init_ltv, liq_ltv, j_collat, k_liq])
    return rows

def by_loan_snapshot(token: str, vip="VIP0") -> Dict[str,Optional[float]]:
    j = _http_json("https://api.bybit.com/v5/crypto-loan-common/loanable-data",
                   params={"currency":token,"vipLevel":vip})
    if not j or j.get("retCode")!=0: return {}
    lst = ((j.get("result") or {}).get("list") or [])
    if not lst: return {}
    row = lst[0]
    def _to_pct(x):
        if x in (None,""): return None
        try:
            s = str(x).strip()
            if s.endswith("%"): return float(s[:-1])
            v = float(s.replace(",",""))
            return v*100.0 if v<=1.5 else v
        except: return None
    max_coin = None
    for k in ["maxBorrowingAmount","maxBorrowAmount","maxLoanableAmount"]:
        if row.get(k):
            try: max_coin = float(str(row[k]).replace(",","")); break
            except: pass
    return {
        "flexible_pct": _to_pct(row.get("flexibleAnnualizedInterestRate")),
        "fixed90_pct":  _to_pct(row.get("annualizedInterestRate90D")),
        "fixed180_pct": _to_pct(row.get("annualizedInterestRate180D")),
        "maxBorrow_coin": max_coin,
    }

def build_bybit_rows_since(token: str, after_dt: Optional[datetime]) -> List[List]:
    fund, used = by_pick_contract_with_rows(token)
    try:
        fund.sort(key=lambda r: int(r['fundingRateTimestamp']))  # old->new
    except Exception:
        pass

    ltv = by_collateral_usdt()
    init_ltv = ltv.get("initialLTV",0.0)
    liq_ltv  = ltv.get("liquidationLTV",0.0)
    cap_usdt = ltv.get("collateralMaxLimit",0.0)

    snap = by_loan_snapshot(token) or {}
    max_borrow_coin = snap.get("maxBorrow_coin")

    rows = []
    for r in fund:
        try:
            t_ms = int(r['fundingRateTimestamp'])
            per_period = float(r['fundingRate'])
        except:
            continue
        ts   = fmt_dt(t_ms)
        t_dt = _parse_dt(ts)
        if after_dt and not (t_dt and t_dt > after_dt): continue
        b = funding_apr_percent(per_period, token)
        c = -b
        spot = spot_bybit_from_contract_symbol(used[1] if used else f"{token}USDT", t_ms)
        per_coin_max_usd = (max_borrow_coin * spot) if (max_borrow_coin and spot) else None
        j_collat = collateral_needed_usdt(per_coin_max_usd, init_ltv) if per_coin_max_usd else None
        k_liq    = liquidation_start_price(spot, init_ltv, liq_ltv) if spot else None
        rows.append([ts, b, c, None, spot, per_coin_max_usd, cap_usdt, init_ltv, liq_ltv, j_collat, k_liq])

    return rows

# ===================== Bybit interest alignment (single read) =====================
def align_bybit_interest_to_funding(ws):
    vals = gs_retry(ws.get, 'A2:D')  # one read
    if not vals: return
    snaps: List[Tuple[int,float]] = []
    targets: List[Tuple[int,int]] = []  # (t_ms, row_index_in_sheet starting at 2)
    for i, row in enumerate(vals, start=2):
        a = row[0] if len(row)>0 else ""
        b = row[1] if len(row)>1 else ""
        d = row[3] if len(row)>3 else ""
        if not a: continue
        t_ms = _to_ms(a)
        if t_ms is None: continue
        if (str(b).strip() == "") and (str(d).strip() != ""):
            try:
                rate = float(str(d).replace(",",""))
                snaps.append((t_ms, rate))
            except: pass
        elif (str(b).strip() != "") and (str(d).strip() == ""):
            targets.append((t_ms, i))
    if not snaps or not targets: return
    snaps.sort(key=lambda x:x[0])

    gap_ms = BYBIT_ALIGN_MAX_GAP_HOURS * 3600 * 1000
    snap_times = [t for (t,_) in snaps]

    # Batch updates: build (row, value) list
    updates = []
    for t_ms, row_idx in targets:
        pos = bisect.bisect_left(snap_times, t_ms)
        cands = []
        if pos > 0: cands.append(snaps[pos-1])
        if pos < len(snaps): cands.append(snaps[pos])
        if not cands: continue
        best = min(cands, key=lambda p: (abs(p[0]-t_ms), 0 if p[0] <= t_ms else 1))
        if abs(best[0]-t_ms) <= gap_ms:
            updates.append((row_idx, best[1]))

    # Apply updates one-by-one (low volume). Could be batched with batch_update if needed.
    for row_idx, val in updates:
        gs_retry(ws.update_cell, row_idx, 4, val)  # column D

# ===================== MAIN =====================
def main():
    print("="*70)
    print("Quota-friendly append-only updater")
    print("="*70)

    client = get_client()
    ss     = gs_retry(client.open_by_key, SHEET_ID)

    # Ensure tabs & headers
    for tk in BINANCE_TOKENS:
        ws = ensure_sheet(ss, f"{tk}_Binance"); ensure_headers(ws)
    for tk in BYBIT_TOKENS:
        ws = ensure_sheet(ss, f"{tk}_Bybit"); ensure_headers(ws)

    bn_usdt = bn_collateral_usdt()
    print(f"[Binance USDT] init={bn_usdt['initialLTV']}, liq={bn_usdt['liquidationLTV']}, cap={bn_usdt['collateralMaxLimit']}")

    # BINANCE
    print("\n📊 BINANCE")
    for tk in BINANCE_TOKENS:
        ws      = ensure_sheet(ss, f"{tk}_Binance"); ensure_headers(ws)
        last_dt = get_last_funding_dt(ws)
        rows    = build_binance_rows_since(tk, last_dt, bn_usdt)
        append_new_unique_rows(ws, rows)
        print(f"  ✅ {tk}_Binance: appended {len(rows)} funding rows")
        time.sleep(0.5)  # small pacing between sheets

    # BYBIT
    print("\n📊 BYBIT")
    for tk in BYBIT_TOKENS:
        ws      = ensure_sheet(ss, f"{tk}_Bybit"); ensure_headers(ws)
        last_dt = get_last_funding_dt(ws)
        rows    = build_bybit_rows_since(tk, last_dt)
        append_new_unique_rows(ws, rows)
        print(f"  ✅ {tk}_Bybit: appended {len(rows)} funding rows")

        if APPEND_BYBIT_SNAPSHOT_ROW:
            # Append one interest-only snapshot row (so history accrues)
            snap = by_loan_snapshot(tk)
            if snap:
                d = snap.get("flexible_pct") or snap.get("fixed90_pct") or snap.get("fixed180_pct")
                ts_ms = now_ms(); ts = fmt_dt(ts_ms)
                # lightweight spot for context (no need to resolve again here)
                spot = None
                row = [ts, 0.0, 0.0, d, spot, None, None, None, None, None, None]
                append_new_unique_rows(ws, [row])
                print(f"     • snapshot appended (D={d}%)")

        align_bybit_interest_to_funding(ws)
        print("     • interest aligned to nearest snapshot")
        time.sleep(0.5)

    print("\n" + "="*70)
    print("✅ COMPLETE (quota-friendly)")
    print("="*70)
    print(f"\n🔗 Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")

if __name__ == "__main__":
    main()
