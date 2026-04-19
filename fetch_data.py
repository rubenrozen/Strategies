"""
fetch_data.py — BestMarketWatch · Strategies data fetcher
Reads sheets_config.json, fetches data from public Google Sheets,
outputs data/strategies_data.json for the front-end.
"""

import csv
import io
import json
import os
import sys
from datetime import datetime, date, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' not installed. Run: pip install requests")
    sys.exit(1)


# ─── Column helpers ───────────────────────────────────────────────────────────

def col_letter_to_idx(col: str) -> int:
    """'A' → 0, 'B' → 1, ..., 'Z' → 25, 'AA' → 26 ..."""
    col = col.upper().strip()
    result = 0
    for ch in col:
        result = result * 26 + (ord(ch) - ord("A") + 1)
    # ── Composition (4 categories)
    if prt_rows:
        result["composition"] = extract_composition(prt_rows)
        result["currencies"]  = extract_currencies(prt_rows)
    else:
        result["composition"] = {"equities": [], "bonds": [], "crypto": [], "futures": []}
        result["currencies"]  = []

    return result - 1


def safe_get(rows: list, row_1based: int, col_letter: str) -> str:
    """Get a cell value safely (returns '' if out of range)."""
    r = row_1based - 1
    c = col_letter_to_idx(col_letter)
    if r >= len(rows):
        return ""
    row = rows[r]
    if c >= len(row):
        return ""
    return row[c].strip()


def parse_float(s: str) -> float | None:
    """Parse a cell string to float. Handles %, French comma decimal, spaces."""
    if not s:
        return None
    s = (s.replace("\xa0", "")   # non-breaking space
          .replace(" ", "")
          .replace("%", "")
          .replace(",", "."))
    # Remove parentheses used for negatives in some locales: (3.2) → -3.2
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def parse_date(s: str) -> str | None:
    """Try common date formats, return ISO string or None."""
    s = s.strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y",
                "%d/%m/%y", "%Y/%m/%d", "%B %d, %Y", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Try numeric serial (Google Sheets sometimes exports dates as numbers)
    try:
        serial = int(float(s))
        # Excel/Sheets epoch: Dec 30, 1899
        from datetime import timedelta
        epoch = date(1899, 12, 30)
        return (epoch + timedelta(days=serial)).isoformat()
    except (ValueError, OverflowError):
        pass
    return None


# ─── Sheet fetcher ─────────────────────────────────────────────────────────────

def fetch_sheet_csv(sheet_id: str, gid: str) -> list[list[str]]:
    """Download a public Google Sheet tab as CSV, return list of rows."""
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        f"/export?format=csv&gid={gid}"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"  HTTP error fetching sheet {sheet_id} gid={gid}: {e}")
        return []
    except requests.RequestException as e:
        print(f"  Network error: {e}")
        return []

    content = resp.content.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(content))
    return list(reader)


# ─── Main extraction ───────────────────────────────────────────────────────────

def extract_strategy(strat_cfg: dict) -> dict:
    """Fetch both tabs and extract all mapped fields."""
    sid   = strat_cfg["sheet_id"]
    lib_gid = strat_cfg["tabs"]["portfolio_library_gid"]
    prt_gid = strat_cfg["tabs"]["portfolio_gid"]

    print(f"  Fetching 'Portfolio library' tab (gid={lib_gid})…")
    lib_rows = fetch_sheet_csv(sid, lib_gid)

    print(f"  Fetching 'Portfolio' tab (gid={prt_gid})…")
    prt_rows = fetch_sheet_csv(sid, prt_gid)

    result = {
        "id":            strat_cfg["id"],
        "displayName":   strat_cfg["display_name"],
        "color":         strat_cfg["color"],
        "tag":           strat_cfg["tag"],
        "benchmarkName": strat_cfg["benchmark_name"],
        "fetchError":    False,
    }

    # ── 'Portfolio library' fields ──────────────────────────────────────────
    if lib_rows:
        result["title"]       = safe_get(lib_rows, 7, "T")   # T7
        result["description"] = safe_get(lib_rows, 8, "T")   # T8

        # YTD time series: A3:A (dates), G3:G (portfolio), H3:H (benchmark)
        dates      = []
        portfolio  = []
        benchmark  = []

        col_a = col_letter_to_idx("A")
        col_g = col_letter_to_idx("G")
        col_h = col_letter_to_idx("H")

        for row_0 in range(2, len(lib_rows)):   # 0-based, starting at row 3
            row = lib_rows[row_0]

            raw_date = row[col_a].strip() if col_a < len(row) else ""
            raw_port = row[col_g].strip() if col_g < len(row) else ""
            raw_benc = row[col_h].strip() if col_h < len(row) else ""

            # Skip rows with no date or no portfolio value
            if not raw_date or not raw_port:
                continue

            iso_date = parse_date(raw_date)
            if iso_date is None:
                continue

            pf_val = parse_float(raw_port)
            bm_val = parse_float(raw_benc)  # can be None if missing

            if pf_val is None:
                continue

            dates.append(iso_date)
            portfolio.append(round(pf_val, 4))
            benchmark.append(round(bm_val, 4) if bm_val is not None else None)

        result["ytd"] = {
            "dates":     dates,
            "portfolio": portfolio,
            "benchmark": benchmark,
        }
    else:
        result["fetchError"] = True
        result["ytd"] = {"dates": [], "portfolio": [], "benchmark": []}

    # ── 'Portfolio' fields ─────────────────────────────────────────────────
    if prt_rows:
        # Portfolio total value (K4) — used for position share calculation
        raw_total = safe_get(prt_rows, 4, "K")   # K4
        result["portfolioTotal"] = parse_float(raw_total)

        result["metrics"] = {
            "sharpe":        parse_float(safe_get(prt_rows, 4, "C")),   # C4
            "sortino":       parse_float(safe_get(prt_rows, 5, "C")),   # C5
            "sortinoTarget": parse_float(safe_get(prt_rows, 6, "F")),   # F6  (target return)
            "ttm":           parse_float(safe_get(prt_rows, 4, "F")),   # F4
            "volatility":    parse_float(safe_get(prt_rows, 18, "C")),  # C18
            "varParametric": parse_float(safe_get(prt_rows, 19, "F")),  # F19
            "varHistoric":   parse_float(safe_get(prt_rows, 20, "F")),  # F20
            "varConditional":parse_float(safe_get(prt_rows, 21, "F")),  # F21
        }
    else:
        result["fetchError"] = True
        result["metrics"] = {}

    return result



CURRENCY_ROWS = [
    ("EUR", 23), ("USD", 24), ("JPY", 25), ("GBP", 26),
    ("ILS", 27), ("CHF", 28), ("CNY", 29), ("SEK", 30), ("CAD", 31),
]

def extract_currencies(prt_rows: list) -> list:
    """Extract cash balance and cash exposure per currency (rows 23-31).
    Returns only currencies where at least one value is non-null and non-zero.
    """
    result = []
    for code, row_1based in CURRENCY_ROWS:
        r = row_1based - 1  # 0-based
        if r >= len(prt_rows):
            continue
        row = prt_rows[r]
        balance  = parse_float(row[8].strip() if 8 < len(row) else "")   # col I
        exposure = parse_float(row[9].strip() if 9 < len(row) else "")   # col J
        # Skip if both are None or zero
        if not balance and not exposure:
            continue
        result.append({
            "code":     code,
            "balance":  round(balance, 2)  if balance  is not None else None,
            "exposure": round(exposure, 2) if exposure is not None else None,
        })
    return result


def extract_composition(prt_rows: list) -> dict:
    """Extract 4-category asset composition from the Portfolio sheet.
    All series start at row 6 (0-based index 5). Empty rows are skipped.
    """
    START = 5  # row 6, 0-based

    equities, bonds, crypto, futures = [], [], [], []

    for row_0 in range(START, len(prt_rows)):
        row = prt_rows[row_0]

        def gc(idx):
            """Get cell value at column index, strip whitespace."""
            return row[idx].strip() if idx < len(row) else ""

        # ── EQUITIES  (anchor: col P=15, non-empty name)
        eq_name = gc(15)
        if eq_name:
            equities.append({
                "name":     eq_name,
                "currency": gc(13),   # N
                "sector":   gc(16),   # Q
                "value":    parse_float(gc(19)),  # T  (€)
            })

        # ── BONDS  (anchor: col AM=38)
        bd_name = gc(38)
        if bd_name:
            bonds.append({
                "name":       bd_name,
                "underlying": gc(40),  # AO
                "currency":   gc(39),  # AN
                "quantity":   parse_float(gc(52)),  # BA
            })

        # ── CRYPTO  (anchor: col V=21)
        cr_name = gc(21)
        if cr_name:
            crypto.append({
                "name":  cr_name,
                "value": parse_float(gc(23)),  # X  (€)
            })

        # ── FUTURES  (anchor: col Z=25)
        ft_name = gc(25)
        if ft_name:
            futures.append({
                "name":       ft_name,
                "value":      parse_float(gc(32)),  # AG  (€)
                "currency":   gc(31),               # AF
                "underlying": gc(27),               # AB
            })

    return {
        "equities": equities,
        "bonds":    bonds,
        "crypto":   crypto,
        "futures":  futures,
    }


# Sheet order matches diagonal: nexthorizon=row6/colJ, valueunderflow=row7/colK, trendspotting=row8/colL
CORR_SHEET_ORDER = ["nexthorizon", "valueunderflow", "trendspotting"]
CORR_START_ROW   = 6   # 1-based
CORR_START_COL   = 9   # col J, 0-based

def fetch_correlation(cfg: dict) -> dict | None:
    """Fetch 3×3 correlation matrix from the separate correlation file."""
    sheet_id = os.environ.get(cfg.get("sheet_id_env", ""), "")
    if not sheet_id:
        print("  ⚠  CORRELATION_SHEET_ID not set — skipping")
        return None

    gid = cfg.get("gid", "0")
    print(f"  Fetching correlation matrix (gid={gid})…")
    rows = fetch_sheet_csv(sheet_id, gid)
    if not rows:
        print("  ⚠  Correlation sheet empty or unreachable")
        return None

    matrix = []
    for i, strat_id in enumerate(CORR_SHEET_ORDER):
        row_0 = (CORR_START_ROW - 1) + i          # 0-based row index
        row_vals = []
        for j in range(len(CORR_SHEET_ORDER)):
            col_0 = CORR_START_COL + j             # 0-based col index (J, K, L)
            raw = rows[row_0][col_0].strip() if (row_0 < len(rows) and col_0 < len(rows[row_0])) else ""
            val = parse_float(raw)
            row_vals.append(round(val, 4) if val is not None else None)
        matrix.append(row_vals)

    print(f"  ✓  Correlation matrix fetched ({len(CORR_SHEET_ORDER)}×{len(CORR_SHEET_ORDER)})")
    for i, sid in enumerate(CORR_SHEET_ORDER):
        print(f"     {sid:20s} {matrix[i]}")

    return {
        "order":  CORR_SHEET_ORDER,
        "values": matrix,
    }

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
        env_var = s.get("sheet_id_env")          # e.g. "VALUEUNDERFLOW_SHEET_ID"
        if env_var:
            s["sheet_id"] = os.environ.get(env_var, "")
        if not s.get("sheet_id"):
            missing.append(s["id"])

    if missing:
        print(f"WARNING: Missing Sheet ID env vars for: {missing}")
        print("Set secrets in GitHub: Settings → Secrets → Actions → New repository secret")
        print("Skipping fetch, keeping existing data.json if present.")
        return

    output = {
        "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "strategies":  []
    }

    for strat_cfg in strategies_cfg:
        print(f"\n→ Processing: {strat_cfg['display_name']} ({strat_cfg['id']})")
        data = extract_strategy(strat_cfg)
        output["strategies"].append(data)
        if data["fetchError"]:
            print(f"  ⚠  Partial or failed fetch for {strat_cfg['id']}")
        else:
            n = len(data["ytd"]["dates"])
            print(f"  ✓  {n} YTD data points fetched")
            m = data.get("metrics", {})
            print(f"  ✓  Metrics: Sharpe={m.get('sharpe')}, Sortino={m.get('sortino')}, "
                  f"TTM={m.get('ttm')}, Vol={m.get('volatility')}")

    # ── Fetch correlation matrix (separate file) ──────────────────────────
    corr_cfg = config.get("correlation")
    if corr_cfg:
        corr_data = fetch_correlation(corr_cfg)
        if corr_data:
            output["correlationMatrix"] = corr_data
        else:
            print("  Correlation matrix not available — keeping existing if present")
    else:
        print("WARNING: No 'correlation' block in sheets_config.json")

    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "strategies_data.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Written to {out_path}")
    print(f"  Total strategies: {len(output['strategies'])}")
    print(f"  Last updated: {output['lastUpdated']}")


if __name__ == "__main__":
    main()
