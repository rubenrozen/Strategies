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
    return row[c].strip()


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
        result["title"]       = safe_get(lib_rows, 7, "T")
        result["description"] = safe_get(lib_rows, 8, "T")

        dates, portfolio, benchmark = [], [], []
        col_a, col_g, col_h = col_letter_to_idx("A"), col_letter_to_idx("G"), col_letter_to_idx("H")

        for row_0 in range(2, len(lib_rows)):
            row = lib_rows[row_0]
            raw_date = row[col_a].strip() if col_a < len(row) else ""
            raw_port = row[col_g].strip() if col_g < len(row) else ""
            raw_benc = row[col_h].strip() if col_h < len(row) else ""
            if not raw_date or not raw_port: continue
            iso_date = parse_date(raw_date)
            if iso_date is None: continue
            pf_val = parse_float(raw_port)
            bm_val = parse_float(raw_benc)
            if pf_val is None: continue
            dates.append(iso_date)
            portfolio.append(round(pf_val, 4))
            benchmark.append(round(bm_val, 4) if bm_val is not None else None)

        result["ytd"] = {"dates": dates, "portfolio": portfolio, "benchmark": benchmark}
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
            "ttm":            parse_float(safe_get(prt_rows, 4,  "F")),
            "volatility":     parse_float(safe_get(prt_rows, 18, "C")),
            "varParametric":  parse_float(safe_get(prt_rows, 19, "F")),
            "varHistoric":    parse_float(safe_get(prt_rows, 20, "F")),
            "varConditional": parse_float(safe_get(prt_rows, 21, "F")),
        }
        result["composition"] = extract_composition(prt_rows)
        result["currencies"]  = extract_currencies(prt_rows)
    else:
        result["fetchError"]    = True
        result["metrics"]       = {}
        result["composition"]   = {"equities": [], "bonds": [], "crypto": [], "futures": []}
        result["currencies"]    = []

    return result


def extract_composition(prt_rows: list) -> dict:
    START = 5
    equities, bonds, crypto, futures = [], [], [], []
    for row_0 in range(START, len(prt_rows)):
        row = prt_rows[row_0]
        def gc(idx): return row[idx].strip() if idx < len(row) else ""

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
        balance  = parse_float(row[8].strip() if 8 < len(row) else "")
        exposure = parse_float(row[9].strip() if 9 < len(row) else "")
        if not balance and not exposure: continue
        result.append({
            "code":     code,
            "balance":  round(balance, 2)  if balance  is not None else None,
            "exposure": round(exposure, 2) if exposure is not None else None,
        })
    return result


# ─── Correlation matrix ────────────────────────────────────────────────────────

CORR_SHEET_ORDER = ["nexthorizon", "valueunderflow", "trendspotting"]
CORR_START_ROW   = 6   # 1-based
CORR_START_COL   = 9   # col J, 0-based

def fetch_correlation(gc: gspread.Client, cfg: dict) -> dict | None:
    sheet_id = os.environ.get(cfg.get("sheet_id_env", ""), "")
    if not sheet_id:
        print("  ⚠  CORRELATION_SHEET_ID not set — skipping")
        return None
    gid = cfg.get("gid", "0")
    print(f"  Fetching correlation matrix (gid={gid})…")
    rows = fetch_tab(gc, sheet_id, gid)
    if not rows: return None

    matrix = []
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
    for i, sid in enumerate(CORR_SHEET_ORDER):
        print(f"     {sid:20s} {matrix[i]}")
    return {"order": CORR_SHEET_ORDER, "values": matrix}


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

        data = extract_strategy_data(lib_rows, prt_rows)
        data["id"]            = strat_cfg["id"]
        data["displayName"]   = strat_cfg["display_name"]
        data["color"]         = strat_cfg["color"]
        data["tag"]           = strat_cfg["tag"]
        data["benchmarkName"] = strat_cfg["benchmark_name"]

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
