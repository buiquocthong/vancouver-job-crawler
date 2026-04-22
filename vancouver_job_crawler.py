#!/usr/bin/env python3
"""
Vancouver Job Crawler — Giai đoạn 1
====================================
Crawl Indeed + Glassdoor: tài chính / data science / C-suite
Lọc: Vancouver BC area  |  min $60k/yr hoặc $30/hr  |  3 ngày gần nhất

Cách chạy:
  pip install python-jobspy pandas
  python3 vancouver_job_crawler.py          # Chạy thật (cần proxy/VPN)
  python3 vancouver_job_crawler.py --demo   # Demo với dữ liệu mẫu

Kết quả: vancouver_jobs_YYYY-MM-DD.csv
"""

import sys, time, logging, re
from datetime import date, timedelta
from pathlib import Path
import pandas as pd
from jobspy import scrape_jobs

DEMO_MODE = "--demo" in sys.argv

# ─────────────────────────────────────────────────────────────────────────────
# ① CẤU HÌNH
# ─────────────────────────────────────────────────────────────────────────────

# VPN / Proxy (bắt buộc nếu Indeed trả 403)
# Format: "user:pass@proxy.host:port"  hoặc  "http://proxy.host:port"
# PROXIES: list[str] = []
PROXIES = ["user:password@proxy.host:8080"]


RESULTS_PER_SEARCH  = 50      # Jobs mỗi lần search
DAYS_OLD            = 3       # Lấy N ngày gần nhất
DISTANCE_KM         = 35      # Bán kính km
MIN_ANNUAL          = 60_000  # CAD/năm
MIN_HOURLY          = 30.0    # CAD/giờ
MIN_MONTHLY         = 5_000   # CAD/tháng
SITES               = ["indeed", "glassdoor"]
OUTPUT_FILE         = Path(f"vancouver_jobs_{date.today()}.csv")

# Keywords theo nhóm để search
KEYWORD_GROUPS = {
    "finance_analyst": [
        "Financial Analyst", "FP&A Analyst", "Investment Analyst",
        "Quantitative Researcher", "Actuarial Analyst", "CFA Analyst",
    ],
    "data_science": [
        "Data Scientist", "Machine Learning Engineer",
        "Quantitative Analyst", "Research Scientist",
    ],
    "credentials": [
        "CFA Investment", "Actuary", "PhD Research",
        "Investment Management",
    ],
    "c_suite": [
        "Chief Executive Officer CEO", "Chief Technology Officer CTO",
        "Chief Information Officer CIO", "Chief Science Officer",
        "Chief AI Officer", "Chief Investment Officer",
    ],
    "trading_investment": [
        "Equity Trader", "Portfolio Manager",
        "Capital Markets Analyst", "Investment Associate", "President",
    ],
}

# Thành phố cho phép trong kết quả
ALLOWED_CITIES = {
    "vancouver", "burnaby", "north vancouver", "west vancouver",
    "new westminster", "richmond", "coquitlam", "port moody",
    "remote",
}

LOCATIONS = [
    "Vancouver, BC, Canada",
    "Burnaby, BC, Canada",
    "North Vancouver, BC, Canada",
    "West Vancouver, BC, Canada",
]

# ─────────────────────────────────────────────────────────────────────────────
# ② LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# ③ DEMO DATA
# ─────────────────────────────────────────────────────────────────────────────

def make_demo_data() -> pd.DataFrame:
    """24 jobs mẫu để test pipeline mà không cần proxy."""
    today = date.today()
    rows = [
        # title, company, city, min, max, interval, listing_type, url_direct, days_ago, keyword
        ("Financial Analyst",        "Telus",              "Vancouver",       70000, 90000,  "yearly",  "organic",      "https://telus.com/careers/1",      0, "Financial Analyst"),
        ("FP&A Manager",             "Lululemon",          "Vancouver",       95000, 120000, "yearly",  "organic",      "https://lululemon.com/jobs/2",     1, "FP&A Analyst"),
        ("Investment Analyst",       "RBC Wealth Mgmt",    "Vancouver",       80000, 105000, "yearly",  "sponsored",    "https://rbc.com/careers/3",        1, "Investment Analyst"),
        ("Quantitative Researcher",  "Absolute Return",    "Vancouver",      110000, 150000, "yearly",  "organic",      "https://absolutereturn.com/4",     0, "Quantitative Researcher"),
        ("Data Scientist",           "Hootsuite",          "Vancouver",       90000, 115000, "yearly",  "apply_direct", "",                                 2, "Data Scientist"),
        ("Senior Data Scientist",    "SAP Canada",         "Vancouver",       95000, 130000, "yearly",  "organic",      "https://sap.com/ca/5",             0, "Data Scientist"),
        ("ML Engineer",              "Microsoft Canada",   "Vancouver",      105000, 145000, "yearly",  "organic",      "https://microsoft.com/jobs/6",     1, "Machine Learning Engineer"),
        ("Actuarial Analyst",        "Sun Life Financial", "Vancouver",       68000,  88000, "yearly",  "sponsored",    "https://sunlife.com/7",            2, "Actuarial Analyst"),
        ("Portfolio Manager CFA",    "Mackenzie Invest.",  "Vancouver",      130000, 175000, "yearly",  "organic",      "https://mackenzieinv.com/8",       0, "CFA Investment"),
        ("Equity Trader",            "Canaccord Genuity",  "Vancouver",      100000, 160000, "yearly",  "organic",      "https://canaccord.com/9",          1, "Equity Trader"),
        ("Chief Technology Officer", "Visier",             "Vancouver",      200000, 280000, "yearly",  "organic",      "https://visier.com/cto",           0, "Chief Technology Officer CTO"),
        ("Chief Investment Officer", "BC Investment Corp", "Vancouver",      250000, 350000, "yearly",  "organic",      "https://bcimc.com/cio",            1, "Chief Investment Officer"),
        ("Chief AI Officer",         "Ballard Power",      "Burnaby",        220000, 300000, "yearly",  "organic",      "https://ballard.com/caio",         0, "Chief AI Officer"),
        ("Systems Analyst",          "EA Sports",          "Burnaby",         75000,  95000, "yearly",  "apply_direct", "",                                 2, "Data Scientist"),
        ("Investment Associate",     "Nicola Wealth",      "Vancouver",       85000, 110000, "yearly",  "organic",      "https://nicolawealth.com/15",      1, "Investment Associate"),
        ("Quantitative Analyst",     "Ritchie Bros.",      "Burnaby",         90000, 120000, "yearly",  "organic",      "https://rbauctioneer.com/16",      0, "Quantitative Analyst"),
        ("Research Scientist AI",    "D-Wave Systems",     "Burnaby",        115000, 155000, "yearly",  "organic",      "https://dwavesys.com/17",          1, "PhD Research"),
        ("CEO",                      "Fintech Startup BC", "Vancouver",      180000, 250000, "yearly",  "apply_direct", "",                                 2, "Chief Executive Officer CEO"),
        ("Capital Markets Analyst",  "CIBC Wood Gundy",    "Vancouver",       72000,  92000, "yearly",  "sponsored",    "https://cibc.com/18",              0, "Capital Markets Analyst"),
        ("Investment Manager",       "Family Office",      "West Vancouver",   None,   None, None,      "organic",      "https://familyoffice.com/19",      0, "Investment Management"),
        ("Data Analyst",             "BCAA",               "North Vancouver",  35.0,   45.0, "hourly",  "apply_direct", "",                                 1, "Data Scientist"),
        # Dưới ngưỡng lương → lọc ra
        ("Junior Analyst",           "Small Co",           "Vancouver",       40000,  55000, "yearly",  "organic",      "",                                 1, "Financial Analyst"),
        ("Data Entry Clerk",         "Small Office",       "Vancouver",        18.0,   22.0, "hourly",  "organic",      "",                                 1, "Financial Analyst"),
        # Ngoài địa điểm → lọc ra
        ("Financial Analyst",        "TD Bank Toronto",    "Toronto",         80000, 100000, "yearly",  "organic",      "",                                 0, "Financial Analyst"),
    ]
    records = []
    for title, co, city, mn, mx, intv, listing, url_d, days, kw in rows:
        records.append({
            "title":           title,
            "company_name":    co,
            "location":        f"{city}, BC, Canada",
            "min_amount":      mn,
            "max_amount":      mx,
            "interval":        intv,
            "listing_type":    listing,
            "job_url_direct":  url_d,
            "job_url":         f"https://ca.indeed.com/viewjob?jk=demo{abs(hash(title+co))%99999:05d}",
            "date_posted":     today - timedelta(days=days),
            "site":            "indeed" if days % 2 == 0 else "glassdoor",
            "is_remote":       False,
            "company_industry":"Finance & Technology",
            "job_type":        "fulltime",
            "search_keyword":  kw,
            "search_group":    "demo",
            "currency":        "CAD",
        })
    log.info(f"[DEMO] Tạo {len(records)} jobs mẫu (trước khi lọc)")
    return pd.DataFrame(records)

# ─────────────────────────────────────────────────────────────────────────────
# ④ CRAWL
# ─────────────────────────────────────────────────────────────────────────────

def crawl_jobs() -> pd.DataFrame:
    """Chạy tất cả keyword × location, gộp, bỏ duplicate."""
    all_frames: list[pd.DataFrame] = []
    total = sum(len(v) for v in KEYWORD_GROUPS.values()) * len(LOCATIONS)
    done  = 0

    for group_name, keywords in KEYWORD_GROUPS.items():
        for keyword in keywords:
            for location in LOCATIONS:
                done += 1
                log.info(f"[{done}/{total}] '{keyword}' @ {location}")
                try:
                    df = scrape_jobs(
                        site_name=SITES,
                        search_term=keyword,
                        location=location,
                        country_indeed="canada",
                        results_wanted=RESULTS_PER_SEARCH,
                        hours_old=DAYS_OLD * 24,
                        distance=DISTANCE_KM,
                        description_format="markdown",
                        proxies=PROXIES or None,
                        verbose=0,
                    )
                    if df is not None and not df.empty:
                        df["search_keyword"] = keyword
                        df["search_group"]   = group_name
                        all_frames.append(df)
                        log.info(f"  → {len(df)} jobs thô")
                    else:
                        log.warning("  → 0 jobs. Thêm PROXIES nếu thấy 403.")
                except Exception as e:
                    log.warning(f"  → Lỗi: {e}")
                time.sleep(2)   # tránh rate-limit

    if not all_frames:
        log.error("Không lấy được job nào. Kiểm tra PROXIES / VPN.")
        return pd.DataFrame()

    raw = pd.concat(all_frames, ignore_index=True)
    log.info(f"Tổng thô (có trùng): {len(raw)}")
    return raw

# ─────────────────────────────────────────────────────────────────────────────
# ⑤ CHUẨN HÓA
# ─────────────────────────────────────────────────────────────────────────────

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Đảm bảo cột cần thiết tồn tại
    for col in ["min_amount","max_amount","interval","currency","date_posted",
                "listing_type","job_url_direct","is_remote","location",
                "company_name","title","job_url","site","search_keyword",
                "description","salary_source","salary","salary_text"]:
        if col not in df.columns:
            df[col] = None

    df["location_str"]  = df["location"].apply(_location_to_str)
    salary_parts = df.apply(_resolve_salary_fields, axis=1, result_type="expand")
    salary_parts.columns = ["min_amount", "max_amount", "interval", "currency", "salary_source"]
    df[["min_amount", "max_amount", "interval", "currency", "salary_source"]] = salary_parts
    df["apply_method"]  = df.apply(_get_apply_method, axis=1)
    df["salary_display"]= df.apply(_format_salary, axis=1)
    df["date_posted"]   = pd.to_datetime(df["date_posted"], errors="coerce").dt.date
    return df


def _location_to_str(loc) -> str:
    if loc is None:
        return ""
    if isinstance(loc, str):
        return loc
    try:
        parts = []
        if getattr(loc, "city", None):
            parts.append(loc.city)
        if getattr(loc, "state", None):
            parts.append(loc.state)
        return ", ".join(parts)
    except Exception:
        return str(loc)


def _get_apply_method(row) -> str:
    """
    Apply Now    → job_url_direct trống (nộp thẳng trên Indeed)
    Apply on Company Site → có URL riêng dẫn sang ATS công ty
    """
    url_direct = str(row.get("job_url_direct") or "")
    listing    = str(row.get("listing_type")   or "").lower()
    if url_direct.startswith("http"):
        return "Apply on Company Site"
    if "apply_direct" in listing or "easy" in listing:
        return "Apply Now"
    return "Apply on Company Site"


def _resolve_salary_fields(row):
    mn = _to_number(row.get("min_amount"))
    mx = _to_number(row.get("max_amount"))
    intv = _clean_interval(row.get("interval"))
    curr = _clean_currency(row.get("currency"), row.get("location"), row.get("site"))
    src = row.get("salary_source")

    if mn is not None or mx is not None:
        return mn, mx, intv, curr, src or "direct_data"

    parsed = _extract_salary_from_row(row)
    if parsed:
        mn, mx, intv, parsed_curr, parsed_src = parsed
        return mn, mx, intv, parsed_curr or curr, parsed_src

    return None, None, intv, curr, src


def _extract_salary_from_row(row):
    text_candidates = [
        row.get("salary"),
        row.get("salary_text"),
        row.get("description"),
    ]
    currency_hint = _clean_currency(row.get("currency"), row.get("location"), row.get("site"))
    for value in text_candidates:
        parsed = _extract_salary_from_text(value, currency_hint=currency_hint)
        if parsed:
            return (*parsed, "description_parse")
    return None


def _extract_salary_from_text(value, currency_hint="CAD"):
    text = _to_text(value)
    if not text:
        return None

    normalized = (
        text.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2212", "-")
        .replace("\xa0", " ")
    )

    patterns = [
        r"(?P<currency>CAD|USD|C\$|US\$|\$)\s*(?P<min>\d[\d,]*(?:\.\d+)?)\s*(?P<min_k>[kK]?)\s*-\s*(?P<currency2>CAD|USD|C\$|US\$|\$)?\s*(?P<max>\d[\d,]*(?:\.\d+)?)\s*(?P<max_k>[kK]?)\s*(?P<interval>per year|a year|yearly|annually|per hour|an hour|hourly|per month|a month|monthly|per week|a week|weekly|per day|a day|daily)",
        r"(?P<currency>CAD|USD|C\$|US\$|\$)\s*(?P<amount>\d[\d,]*(?:\.\d+)?)\s*(?P<amount_k>[kK]?)\s*(?P<interval>per year|a year|yearly|annually|per hour|an hour|hourly|per month|a month|monthly|per week|a week|weekly|per day|a day|daily)",
    ]

    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue

        interval = _normalize_interval_text(match.group("interval"))
        currency = _normalize_currency_text(match.groupdict().get("currency")) or currency_hint

        if match.groupdict().get("min"):
            mn = _parse_amount(match.group("min"), match.group("min_k"))
            mx = _parse_amount(match.group("max"), match.group("max_k"))
        else:
            mn = _parse_amount(match.group("amount"), match.group("amount_k"))
            mx = None

        if mn is None and mx is None:
            continue
        return mn, mx, interval, currency

    return None


def _to_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, dict):
        return " ".join(str(v) for v in value.values() if v is not None)
    return str(value)


def _parse_amount(raw, suffix=""):
    if raw is None:
        return None
    amount = float(str(raw).replace(",", ""))
    if str(suffix or "").lower() == "k":
        amount *= 1000
    return int(amount) if amount >= 100 else round(amount, 2)


def _normalize_interval_text(text: str):
    label = str(text or "").strip().lower()
    mapping = {
        "per year": "yearly",
        "a year": "yearly",
        "yearly": "yearly",
        "annually": "yearly",
        "per hour": "hourly",
        "an hour": "hourly",
        "hourly": "hourly",
        "per month": "monthly",
        "a month": "monthly",
        "monthly": "monthly",
        "per week": "weekly",
        "a week": "weekly",
        "weekly": "weekly",
        "per day": "daily",
        "a day": "daily",
        "daily": "daily",
    }
    return mapping.get(label)


def _normalize_currency_text(text: str):
    label = str(text or "").strip().upper()
    mapping = {
        "$": None,
        "C$": "CAD",
        "CAD": "CAD",
        "US$": "USD",
        "USD": "USD",
    }
    return mapping.get(label, None)


def _clean_interval(value):
    label = str(value or "").strip().lower()
    if not label or label in {"none", "nan"}:
        return None
    mapping = {
        "year": "yearly",
        "yr": "yearly",
        "annual": "yearly",
        "hour": "hourly",
        "hr": "hourly",
        "month": "monthly",
        "week": "weekly",
        "day": "daily",
    }
    return mapping.get(label, label)


def _clean_currency(value, location=None, site=None):
    label = str(value or "").strip().upper()
    if label and label not in {"NONE", "NAN"}:
        return label

    location_text = str(location or "").lower()
    site_text = str(site or "").lower()
    if "canada" in location_text or ", bc" in location_text or site_text in {"indeed", "glassdoor"}:
        return "CAD"
    return "USD"


def _to_number(value):
    if value is None or pd.isna(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number >= 100 else round(number, 2)


def _format_salary(row) -> str:
    mn   = row.get("min_amount")
    mx   = row.get("max_amount")
    intv = str(row.get("interval") or "")
    curr = str(row.get("currency") or "CAD")
    if pd.isna(mn) and pd.isna(mx):
        return "N/A"
    def fmt(v):
        if pd.isna(v): return None
        return f"${int(v):,}" if v > 200 else f"${v:.2f}"
    parts = [p for p in [fmt(mn), fmt(mx)] if p]
    label = {"yearly":"/yr","hourly":"/hr","monthly":"/mo",
             "weekly":"/wk","daily":"/day"}.get(intv.lower(), "")
    return f"{curr} {' - '.join(parts)}{label}"

# ─────────────────────────────────────────────────────────────────────────────
# ⑥ LỌC
# ─────────────────────────────────────────────────────────────────────────────

def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    n0 = len(df)

    # 1. Dedup
    df = df.drop_duplicates(subset=["job_url"], keep="first")
    log.info(f"Sau dedup URL     : {len(df)} (bỏ {n0 - len(df)})")

    # 2. Địa điểm
    df = df[df["location_str"].apply(_is_allowed_location)]
    log.info(f"Sau lọc địa điểm  : {len(df)}")

    # 3. Ngày (giữ cả None — jobspy đôi khi không có date)
    cutoff = date.today() - timedelta(days=DAYS_OLD)
    df = df[df["date_posted"].apply(
        lambda d: d is None or (isinstance(d, date) and d >= cutoff)
    )]
    log.info(f"Sau lọc ngày      : {len(df)}")

    # 4. Lương (giữ nếu N/A — không loại job tốt thiếu info)
    df = df[df.apply(_salary_ok, axis=1)]
    log.info(f"Sau lọc lương     : {len(df)}")

    return df.reset_index(drop=True)


def _is_allowed_location(loc: str) -> bool:
    if not loc:
        return False
    loc_l = loc.lower()
    return any(c in loc_l for c in ALLOWED_CITIES)


def _salary_ok(row) -> bool:
    mn   = row.get("min_amount")
    mx   = row.get("max_amount")
    intv = str(row.get("interval") or "").lower()
    # Không có lương → giữ
    if pd.isna(mn) and pd.isna(mx):
        return True
    amount = mn if not pd.isna(mn) else mx
    thresholds = {
        "yearly":  MIN_ANNUAL,
        "hourly":  MIN_HOURLY,
        "monthly": MIN_MONTHLY,
        "weekly":  MIN_HOURLY * 40,
        "daily":   MIN_HOURLY * 8,
    }
    return amount >= thresholds.get(intv, MIN_ANNUAL)

# ─────────────────────────────────────────────────────────────────────────────
# ⑦ XUẤT CSV
# ─────────────────────────────────────────────────────────────────────────────

OUTPUT_COLS = [
    "title", "company_name", "location_str", "salary_display",
    "min_amount", "max_amount", "interval", "currency",
    "salary_source",
    "apply_method", "date_posted", "job_url", "job_url_direct",
    "site", "search_keyword", "search_group",
    "is_remote", "company_industry", "job_type",
]

def save_results(df: pd.DataFrame) -> Path:
    cols = [c for c in OUTPUT_COLS if c in df.columns]
    out  = df[cols].copy()
    if "min_amount" in out.columns:
        out = out.sort_values(["min_amount","date_posted"],
                              ascending=[False, False], na_position="last")
    out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    log.info(f"Saved {len(out)} jobs → {OUTPUT_FILE.resolve()}")
    return OUTPUT_FILE

# ─────────────────────────────────────────────────────────────────────────────
# ⑧ SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame):
    SEP = "═" * 60
    print(f"\n{SEP}")
    print(f"  VANCOUVER JOB CRAWLER — KẾT QUẢ{'  [DEMO]' if DEMO_MODE else ''}")
    print(SEP)
    if df.empty:
        print("  ⚠️  Không có kết quả.")
        print("  → Thêm PROXIES = ['user:pass@host:port'] và chạy lại.")
        print(SEP + "\n")
        return

    print(f"  Tổng jobs         : {len(df)}")

    if "site" in df.columns:
        for site, cnt in df["site"].value_counts().items():
            print(f"  {site.capitalize():<18}: {cnt}")

    if "apply_method" in df.columns:
        print()
        for method, cnt in df["apply_method"].value_counts().items():
            icon = "🟢" if "Now" in method else "🔵"
            print(f"  {icon} {method:<32}: {cnt}")

    if "search_keyword" in df.columns:
        print("\n  Top keywords:")
        for kw, cnt in df["search_keyword"].value_counts().head(8).items():
            print(f"    • {kw:<38}: {cnt}")

    has_salary = df["salary_display"].ne("N/A").sum() if "salary_display" in df.columns else 0
    print(f"\n  Có lương          : {has_salary}")
    print(f"  Không có lương    : {len(df) - has_salary}")

    if "min_amount" in df.columns and df["min_amount"].notna().any():
        print("\n  Top 3 lương cao nhất:")
        for _, r in df.nlargest(3, "min_amount")[["title","company_name","salary_display"]].iterrows():
            print(f"    {str(r['title'])[:35]:<36} {str(r['company_name'])[:22]:<23} {r['salary_display']}")

    print(f"\n  File CSV: {OUTPUT_FILE.resolve()}")
    print(SEP + "\n")

# ─────────────────────────────────────────────────────────────────────────────
# ⑨ MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═"*60)
    print(f"  Vancouver Job Crawler {'[DEMO MODE]' if DEMO_MODE else ''}")
    print(f"  Sites  : {', '.join(SITES)}")
    print(f"  Ngày   : {DAYS_OLD} ngày gần nhất")
    print(f"  Lương  : ≥ ${MIN_ANNUAL:,}/yr  hoặc  ${MIN_HOURLY}/hr")
    print(f"  Proxy  : {'✅ ' + str(len(PROXIES)) + ' proxy' if PROXIES else '❌ chưa cấu hình'}")
    print("═"*60 + "\n")

    # Lấy dữ liệu
    if DEMO_MODE:
        raw_df = make_demo_data()
    else:
        raw_df = crawl_jobs()

    if raw_df.empty:
        print_summary(raw_df)
        return

    norm_df     = normalize(raw_df)
    filtered_df = filter_jobs(norm_df)
    save_results(filtered_df)
    print_summary(filtered_df)


if __name__ == "__main__":
    main()
