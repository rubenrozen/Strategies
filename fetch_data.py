"""
fetch_data.py — BestMarketWatch · Strategies data fetcher
Authenticates via Google Service Account (JSON credentials stored in
GOOGLE_CREDENTIALS_JSON secret). Reads private Google Sheets via gspread.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("ERROR: Missing dependencies. Run: pip install gspread google-auth")
    sys.exit(1)


# ─── Auth ─────────────────────────────────────────────────────────────────────

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def get_gspread_client() -> gspread.Client:
    """Build a gspread client from the GOOGLE_CREDENTIALS_JSON env variable."""
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not raw:
        print("ERROR: GOOGLE_CREDENTIALS_JSON secret is not set.")
        sys.exit(1)
    try:
        creds_dict = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"ERROR: GOOGLE_CREDENTIALS_JSON is not valid JSON: {e}")
        sys.exit(1)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ─── Sheet fetcher ─────────────────────────────────────────────────────────────

def fetch_tab(gc: gspread.Client, sheet_id: str, gid: str) -> list[list[str]]:
    """Open a specific tab by GID and return all values as list of rows."""
    try:
        sh = gc.open_by_key(sheet_id)
        ws = sh.get_worksheet_by_id(int(gid))
        rows = ws.get_all_values()
        return rows
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"  ERROR: Sheet {sheet_id} not found or not shared with service account.")
        return []
    except gspread.exceptions.WorksheetNotFound:
        print(f"  ERROR: Tab with gid={gid} not found in sheet {sheet_id}.")
        return []
    except Exception as e:
        print(f"  ERROR fetching sheet {sheet_id} gid={gid}: {e}")
        return []


# ─── Cell helpers ─────────────────────────────────────────────────────────────

def col_letter_to_idx(col: str) -> int:
    col = col.upper().strip()
    result = 0
    for ch in col:
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result - 1


def safe_get(rows: list, row_1based: int, col_letter: str) -> str:
    r = row_1based - 1
    c = col_letter_to_idx(col_letter)
    if r >= len(rows): return ""
    row = rows[r]
    if c >= len(row): return ""
    return str(row[c]).strip()


def parse_float(s: str) -> float | None:
    if not s: return None
    s = (s.replace("\xa0", "").replace(" ", "").replace("%", "")
          .replace(",", ".").replace("\u202f", ""))
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def parse_date(s: str) -> str | None:
    from datetime import date, timedelta
    s = s.strip()
    if not s: return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y",
                "%d/%m/%y", "%Y/%m/%d"):
        try:
            from datetime import datetime as dt
            return dt.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    try:
        serial = int(float(s))
        return (date(1899, 12, 30) + timedelta(days=serial)).isoformat()
    except (ValueError, OverflowError):
        pass
    return None


# ─── Extraction functions ──────────────────────────────────────────────────────

def extract_strategy_data(lib_rows: list, prt_rows: list) -> dict:
    """Extract all mapped fields from the two tabs of a strategy sheet."""
    result = {"fetchError": False}

    # ── Portfolio library ──────────────────────────────────────────────────
    if lib_rows:
        result["title"]           = safe_get(lib_rows, 7, "T") or None
        result["benchmarkName"]   = safe_get(lib_rows, 1, "K") or None   # K1 — None if empty so JS ?? fallback works
        result["description"] = safe_get(lib_rows, 8, "T") or None

        # YTD series now loaded from year sheet (e.g. "2026") in main()
        result["ytd"] = {"dates": [], "portfolio": [], "benchmark": []}
    else:
        result["fetchError"] = True
        result["ytd"] = {"dates": [], "portfolio": [], "benchmark": []}

    # ── Portfolio metrics ──────────────────────────────────────────────────
    if prt_rows:
        raw_total = safe_get(prt_rows, 4, "K")
        result["portfolioTotal"] = parse_float(raw_total)

        result["metrics"] = {
            "sharpe":         parse_float(safe_get(prt_rows, 4,  "C")),
            "sortino":        parse_float(safe_get(prt_rows, 5,  "C")),
            "sortinoTarget":  parse_float(safe_get(prt_rows, 6,  "F")),
            "perfTarget":     parse_float(safe_get(prt_rows, 6,  "F")),   # F6 — performance target
            "ttm":            parse_float(safe_get(prt_rows, 4,  "F")),
            "volatility":     parse_float(safe_get(prt_rows, 18, "C")),
            "varParametric":  parse_float(safe_get(prt_rows, 22, "F")),   # F22
            "varHistoric":    parse_float(safe_get(prt_rows, 23, "F")),   # F23
            "varConditional": parse_float(safe_get(prt_rows, 24, "F")),   # F24
        }
        # Weekly top/bottom performers (Portfolio G35:H40)
        def gp(row, col): return safe_get(prt_rows, row, col) or None
        result["weeklyPerformers"] = {
            "top": [
                {"name": gp(35,"G"), "perf": parse_float(safe_get(prt_rows,35,"H"))},
                {"name": gp(36,"G"), "perf": parse_float(safe_get(prt_rows,36,"H"))},
                {"name": gp(37,"G"), "perf": parse_float(safe_get(prt_rows,37,"H"))},
            ],
            "bottom": [
                {"name": gp(38,"G"), "perf": parse_float(safe_get(prt_rows,38,"H"))},
                {"name": gp(39,"G"), "perf": parse_float(safe_get(prt_rows,39,"H"))},
                {"name": gp(40,"G"), "perf": parse_float(safe_get(prt_rows,40,"H"))},
            ],
        }
        result["composition"] = extract_composition(prt_rows)
        result["currencies"]  = extract_currencies(prt_rows)
        result["sectors"]     = compute_sectors(prt_rows)
    else:
        result["fetchError"]    = True
        result["metrics"]       = {}
        result["weeklyPerformers"] = {"top": [], "bottom": []}
        result["composition"]   = {"equities": [], "bonds": [], "crypto": [], "futures": []}
        result["currencies"]    = []
        result["sectors"]       = []

    return result


def extract_composition(prt_rows: list) -> dict:
    START = 5
    equities, bonds, crypto, futures = [], [], [], []
    for row_0 in range(START, len(prt_rows)):
        row = prt_rows[row_0]
        def gc(idx): return str(row[idx]).strip() if idx < len(row) else ""

        eq_name = gc(15)
        if eq_name:
            equities.append({"name": eq_name, "currency": gc(13), "sector": gc(16),
                             "value": parse_float(gc(19))})
        bd_name = gc(38)
        if bd_name:
            bonds.append({"name": bd_name, "underlying": gc(40), "currency": gc(39),
                          "quantity": parse_float(gc(52))})
        cr_name = gc(21)
        if cr_name:
            crypto.append({"name": cr_name, "value": parse_float(gc(23))})
        ft_name = gc(25)
        if ft_name:
            futures.append({"name": ft_name, "value": parse_float(gc(32)),
                            "currency": gc(31), "underlying": gc(27)})
    return {"equities": equities, "bonds": bonds, "crypto": crypto, "futures": futures}


CURRENCY_ROWS = [
    ("EUR", 23), ("USD", 24), ("JPY", 25), ("GBP", 26),
    ("ILS", 27), ("CHF", 28), ("CNY", 29), ("SEK", 30), ("CAD", 31),
]

def extract_currencies(prt_rows: list) -> list:
    result = []
    for code, row_1based in CURRENCY_ROWS:
        r = row_1based - 1
        if r >= len(prt_rows): continue
        row = prt_rows[r]
        balance  = parse_float(str(row[8]).strip() if 8 < len(row) else "")
        exposure = parse_float(str(row[9]).strip() if 9 < len(row) else "")
        if not balance and not exposure: continue
        result.append({
            "code":     code,
            "balance":  round(balance, 2)  if balance  is not None else None,
            "exposure": round(exposure, 2) if exposure is not None else None,
        })
    return result




def extract_ytd_from_year(year_rows: list) -> dict:
    """Extract YTD series from year sheet.
    Dates in col MJ (347), portfolio in col MK (348), benchmark in col ML (349).
    Series starts at row 7 (0-based index 6). Skips empty and future dates.
    """
    from datetime import date as date_cls
    COL_DATE = 347   # MJ
    COL_PORT = 348   # MK
    COL_BENC = 349   # ML
    START    = 6     # row 7, 0-based

    today_dt = date_cls.today()
    dates, portfolio, benchmark = [], [], []
    skipped_future = 0
    sample_raw = []

    for row_0 in range(START, len(year_rows)):
        row = year_rows[row_0]
        raw_date = str(row[COL_DATE]).strip() if COL_DATE < len(row) else ""
        raw_port = str(row[COL_PORT]).strip() if COL_PORT < len(row) else ""
        raw_benc = str(row[COL_BENC]).strip() if COL_BENC < len(row) else ""

        if not raw_date or not raw_port: continue

        # Collect raw samples for debugging
        if len(sample_raw) < 3:
            sample_raw.append(f"raw_date={repr(raw_date)} raw_port={repr(raw_port)}")

        iso_date = parse_date(raw_date)
        if iso_date is None: continue

        # Compare as date objects — unambiguous regardless of string format
        try:
            from datetime import datetime as dt_cls
            row_dt = dt_cls.strptime(iso_date, "%Y-%m-%d").date()
        except ValueError:
            continue
        if row_dt > today_dt:
            skipped_future += 1
            continue

        pf_val = parse_float(raw_port)
        bm_val = parse_float(raw_benc)
        if pf_val is None: continue

        dates.append(iso_date)
        portfolio.append(round(pf_val, 4))
        benchmark.append(round(bm_val, 4) if bm_val is not None else None)

    print(f"  YTD series: {len(dates)} points kept · {skipped_future} future dates skipped · today={today_dt}")
    if sample_raw:
        print(f"  Sample raw cells: {sample_raw}")
    if dates:
        print(f"  Date range: {dates[0]} → {dates[-1]}")

    return {"dates": dates, "portfolio": portfolio, "benchmark": benchmark}

# ─── Sector allocation ────────────────────────────────────────────────────────

def compute_sectors(prt_rows: list) -> list:
    """Build sector allocation from all 4 asset classes.
    Uses € values (or quantity for bonds) to compute percentage weights.
    Returns list of {sector, value, pct} sorted by value desc, omitting zeros.
    """
    from collections import defaultdict
    buckets = defaultdict(float)
    START = 5  # row 6, 0-based

    for row_0 in range(START, len(prt_rows)):
        row = prt_rows[row_0]
        def gc(idx): return str(row[idx]).strip() if idx < len(row) else ""

        # Equities — group by sector (Q=16), value T=19
        eq_sector = gc(16)
        eq_val    = parse_float(gc(19))
        if eq_sector and eq_val is not None:
            buckets[eq_sector] += abs(eq_val)

        # Bonds — group by underlying (AO=40), value BA=52
        bd_under = gc(40)
        bd_val   = parse_float(gc(52))
        if bd_under and bd_val is not None:
            buckets[bd_under] += abs(bd_val)

        # Futures — group by underlying (AB=27), value AG=32
        ft_under = gc(27)
        ft_val   = parse_float(gc(32))
        if ft_under and ft_val is not None:
            buckets[ft_under] += abs(ft_val)

        # Crypto — all grouped as "Crypto", value X=23
        cr_name = gc(21)
        cr_val  = parse_float(gc(23))
        if cr_name and cr_val is not None:
            buckets["Crypto"] += abs(cr_val)

    total = sum(buckets.values())
    if total == 0:
        return []

    sectors = [
        {"sector": k, "value": round(v, 2), "pct": round(v / total * 100, 2)}
        for k, v in sorted(buckets.items(), key=lambda x: x[1], reverse=True)
        if v > 0
    ]
    return sectors


# ─── Asset class breakdown (Charts sheet) ────────────────────────────────────

def extract_asset_classes(charts_rows: list) -> list:
    """Read asset class names (U=20) and values (V=21) from Charts sheet, rows 4+.
    Skips empty rows. Returns list of {name, value, pct}.
    """
    START = 3  # row 4, 0-based
    items = []
    for row_0 in range(START, len(charts_rows)):
        row = charts_rows[row_0]
        name = str(row[20]).strip() if 20 < len(row) else ""
        val  = parse_float(str(row[21]).strip() if 21 < len(row) else "")
        if name and val is not None:
            items.append({"name": name, "value": round(abs(val), 2)})

    total = sum(x["value"] for x in items)
    if total == 0:
        return items
    for x in items:
        x["pct"] = round(x["value"] / total * 100, 2)
    return items

# ─── Correlation matrix ────────────────────────────────────────────────────────

CORR_SHEET_ORDER = ["nexthorizon", "valueunderflow", "trendspotting"]
CORR_START_ROW   = 6   # 1-based
CORR_START_COL   = 9   # col J, 0-based

def fetch_correlation(gc: gspread.Client, cfg: dict) -> dict | None:
    sheet_id = os.environ.get(cfg.get("sheet_id_env", ""), "")
    if not sheet_id:
        print("  ⚠  CORRELATION_SHEET_ID not set — skipping")
        return None

    # ── Correlation matrix (Correlation tab) ──────────────────────────────
    gid = cfg.get("gid", "0")
    print(f"  Fetching correlation matrix (gid={gid})…")
    rows = fetch_tab(gc, sheet_id, gid)
    matrix = []
    if rows:
        for i in range(len(CORR_SHEET_ORDER)):
            row_0 = (CORR_START_ROW - 1) + i
            row_vals = []
            for j in range(len(CORR_SHEET_ORDER)):
                col_0 = CORR_START_COL + j
                raw = rows[row_0][col_0].strip() if (row_0 < len(rows) and col_0 < len(rows[row_0])) else ""
                val = parse_float(raw)
                row_vals.append(round(val, 4) if val is not None else None)
            matrix.append(row_vals)
        print(f"  ✓  Correlation matrix ({len(CORR_SHEET_ORDER)}×{len(CORR_SHEET_ORDER)})")

    # ── Strategy rankings (Performances tab) ──────────────────────────────
    perf_gid = cfg.get("performances_gid", "")
    rankings = {"weekly": [], "monthly": []}
    if perf_gid:
        print(f"  Fetching performances ranking (gid={perf_gid})…")
        p_rows = fetch_tab(gc, sheet_id, perf_gid)
        if p_rows:
            def gp(row_1based, col):
                return safe_get(p_rows, row_1based, col) or None
            rankings["weekly"] = [
                {"name": gp(15,"C"), "perf": parse_float(safe_get(p_rows,15,"D"))},
                {"name": gp(16,"C"), "perf": parse_float(safe_get(p_rows,16,"D"))},
                {"name": gp(17,"C"), "perf": parse_float(safe_get(p_rows,17,"D"))},
            ]
            rankings["monthly"] = [
                {"name": gp(18,"C"), "perf": parse_float(safe_get(p_rows,18,"D"))},
                {"name": gp(19,"C"), "perf": parse_float(safe_get(p_rows,19,"D"))},
                {"name": gp(20,"C"), "perf": parse_float(safe_get(p_rows,20,"D"))},
            ]
            print(f"  ✓  Rankings: weekly={[r['name'] for r in rankings['weekly']]} monthly={[r['name'] for r in rankings['monthly']]}")
    else:
        print("  ⚠  performances_gid not set in sheets_config.json — skipping rankings")

    return {"order": CORR_SHEET_ORDER, "values": matrix, "rankings": rankings}


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    config_path = Path("sheets_config.json")
    if not config_path.exists():
        print("ERROR: sheets_config.json not found.")
        sys.exit(1)
    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    strategies_cfg = config.get("strategies", [])
    if not strategies_cfg:
        print("ERROR: No strategies defined in sheets_config.json")
        sys.exit(1)

    # Resolve Sheet IDs from environment variables
    missing = []
    for s in strategies_cfg:
        env_var = s.get("sheet_id_env")
        if env_var:
            s["sheet_id"] = os.environ.get(env_var, "")
        if not s.get("sheet_id"):
            missing.append(s["id"])
    if missing:
        print(f"WARNING: Missing Sheet ID env vars for: {missing}")
        print("Skipping fetch, keeping existing data.json if present.")
        return

    # Authenticate once, reuse for all sheets
    print("Authenticating with Google service account…")
    gc = get_gspread_client()
    print("✓ Authenticated\n")

    output = {
        "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "strategies":  []
    }

    for strat_cfg in strategies_cfg:
        print(f"→ Processing: {strat_cfg['display_name']} ({strat_cfg['id']})")
        sid  = strat_cfg["sheet_id"]
        lib_gid = strat_cfg["tabs"]["portfolio_library_gid"]
        prt_gid = strat_cfg["tabs"]["portfolio_gid"]

        print(f"  Fetching 'Portfolio library' tab (gid={lib_gid})…")
        lib_rows = fetch_tab(gc, sid, lib_gid)
        print(f"  Fetching 'Portfolio' tab (gid={prt_gid})…")
        prt_rows = fetch_tab(gc, sid, prt_gid)

        # Year tab — dynamic name (e.g. "2026")
        year_name = str(datetime.now(timezone.utc).year)
        mwrr_ytd   = None
        ytd_pct    = None
        ytd_strat  = None
        ytd_bench  = None
        mwrr_strat = None
        mwrr_bench = None
        ytd_series = {"dates": [], "portfolio": [], "benchmark": []}
        try:
            sh = gc.open_by_key(sid)
            ws_year = sh.worksheet(year_name)
            # Use explicit range to ensure far-right columns (MJ-ML = 347-349) are included
            year_rows = ws_year.get_all_values()
            raw_mwrr      = year_rows[15][4].strip()   if len(year_rows) > 15  and 4   < len(year_rows[15])  else ""  # E16
            raw_ytd_pct   = year_rows[16][4].strip()   if len(year_rows) > 16  and 4   < len(year_rows[16])  else ""  # E17
            raw_ytd_strat  = year_rows[10][353].strip() if len(year_rows) > 10 and 353 < len(year_rows[10]) else ""  # MP11
            raw_ytd_bench  = year_rows[6][353].strip()  if len(year_rows) > 6  and 353 < len(year_rows[6])  else ""  # MP7
            raw_mwrr_strat = year_rows[10][352].strip() if len(year_rows) > 10 and 352 < len(year_rows[10]) else ""  # MO11
            raw_mwrr_bench = year_rows[6][352].strip()  if len(year_rows) > 6  and 352 < len(year_rows[6])  else ""  # MO7
            mwrr_ytd      = parse_float(raw_mwrr)
            ytd_pct       = parse_float(raw_ytd_pct)
            ytd_strat     = parse_float(raw_ytd_strat)
            ytd_bench     = parse_float(raw_ytd_bench)
            mwrr_strat    = parse_float(raw_mwrr_strat)
            mwrr_bench    = parse_float(raw_mwrr_bench)
            ytd_series    = extract_ytd_from_year(year_rows)
            print(f"  Year tab '{year_name}' → MWRR={mwrr_ytd} · YTD strat={ytd_strat} bench={ytd_bench} · MWRR strat={mwrr_strat} bench={mwrr_bench} · {len(ytd_series['dates'])} pts")
        except Exception as e:
            print(f"  ⚠  Year tab '{year_name}' not found or error: {e}")

        # Charts tab
        charts_rows = []
        charts_gid = strat_cfg["tabs"].get("charts_gid", "")
        if charts_gid and "REPLACE" not in charts_gid:
            print(f"  Fetching 'Charts' tab (gid={charts_gid})…")
            charts_rows = fetch_tab(gc, sid, charts_gid)
        else:
            print(f"  ⚠  charts_gid not configured — skipping asset class breakdown")

        data = extract_strategy_data(lib_rows, prt_rows)
        data["mwrrYtd"]      = mwrr_ytd
        data["ytdPct"]       = ytd_pct
        data["ytdStrat"]     = ytd_strat    # MP11 — displayed strategy YTD
        data["ytdBench"]     = ytd_bench    # MP7  — displayed benchmark YTD
        data["mwrrStrat"]    = mwrr_strat   # MO11 — MWRR strategy
        data["mwrrBench"]    = mwrr_bench   # MO7  — MWRR benchmark
        data["ytd"]          = ytd_series
        data["assetClasses"] = extract_asset_classes(charts_rows) if charts_rows else []
        data["id"]            = strat_cfg["id"]
        data["displayName"]   = strat_cfg["display_name"]
        data["color"]         = strat_cfg["color"]
        data["tag"]           = strat_cfg["tag"]
        # benchmarkName already set from K1 in extract_strategy_data()

        output["strategies"].append(data)
        if data["fetchError"]:
            print(f"  ⚠  Partial or failed fetch")
        else:
            n = len(data["ytd"]["dates"])
            m = data.get("metrics", {})
            print(f"  ✓  {n} YTD points · Sharpe={m.get('sharpe')} · Sortino={m.get('sortino')}")

    # Correlation matrix (separate file)
    corr_cfg = config.get("correlation")
    if corr_cfg:
        print("\n→ Processing: Correlation matrix")
        corr_data = fetch_correlation(gc, corr_cfg)
        if corr_data:
            output["correlationMatrix"] = corr_data

    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "strategies_data.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Written to {out_path}")
    print(f"  Strategies: {len(output['strategies'])} · Last updated: {output['lastUpdated']}")


if __name__ == "__main__":
    main()
