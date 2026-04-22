import asyncio
import json
import os
import smtplib
import ssl
from collections import defaultdict
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import httpx
import gspread
from google.oauth2.service_account import Credentials

BASE_URL = "https://wrapper-func.vercel.app"
CACHE_FILE = Path("schools_cache.json")
CACHE_MAX_AGE_DAYS = 7
TOTAL_PAGES = 16904
CONCURRENCY = 30

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SINDH_DISTRICTS = [
    "Karachi South", "Karachi Central", "Karachi East", "Karachi West",
    "Karachi Korangi", "Karachi Malir", "Karachi Keamari",
    "Sukkur", "Ghotki", "Khairpur", "Shaheed Benazirabad", "Sanghar",
    "Naushahro Feroz", "Larkana", "Jacobabad", "Shikarpur",
    "Qamber Shahdatkot", "Kashmore Kandhkot", "Mirpurkhas", "Umerkot",
    "Tharparkar", "Hyderabad", "Badin", "Dadu", "Thatta", "Jamshoro",
    "Matiari", "Tando Allahyar", "Tando Muhammad Khan", "Sujawal",
]

# ── Parameters ────────────────────────────────────────────────────────────────

PROVINCE          = os.environ.get("PARAM_PROVINCE")          or "Sindh"
DISTRICT          = os.environ.get("PARAM_DISTRICT")          or ""
INSTITUTE         = os.environ.get("PARAM_INSTITUTE")         or "SELD"
DATE_FROM         = os.environ.get("PARAM_DATE_FROM")         or "12/01/2025"
DATE_TO           = os.environ.get("PARAM_DATE_TO")           or "01/31/2026"
ACTIVE_CRITERIA   = os.environ.get("PARAM_ACTIVE_CRITERIA")   or "Submitted Activities"
ACTIVE_THRESHOLD  = int(os.environ.get("PARAM_ACTIVE_THRESHOLD") or "2")
SHEET_ID_OVERRIDE = os.environ.get("PARAM_SHEET_ID")          or ""
DISTRICTS_INPUT   = os.environ.get("PARAM_DISTRICTS")          or ""


# ── Section A: Data Fetching ──────────────────────────────────────────────────

def fetch_posts(params: dict) -> list[dict]:
    with httpx.Client(timeout=180) as client:
        resp = client.get(f"{BASE_URL}/posts", params=params)
        resp.raise_for_status()
        return resp.json()


async def _fetch_page(client: httpx.AsyncClient, sem: asyncio.Semaphore, page: int) -> list[dict]:
    async with sem:
        for attempt in range(3):
            try:
                resp = await client.get(f"{BASE_URL}/schools", params={"page": page}, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                if attempt == 2:
                    print(f"  Page {page} failed after 3 attempts: {exc}")
                    return []
                await asyncio.sleep(1)
    return []


async def _fetch_all_schools_async() -> list[dict]:
    sem = asyncio.Semaphore(CONCURRENCY)
    schools = []
    batch_size = 500

    async with httpx.AsyncClient(http2=True, timeout=30) as client:
        for batch_start in range(1, TOTAL_PAGES + 1, batch_size):
            batch_end = min(batch_start + batch_size, TOTAL_PAGES + 1)
            tasks = [_fetch_page(client, sem, p) for p in range(batch_start, batch_end)]
            results = await asyncio.gather(*tasks)

            for page_data in results:
                for school in page_data:
                    name = str(school.get("SchoolName", "")).strip()
                    if not name:
                        continue
                    try:
                        level = int(school.get("level") or 0)
                    except (ValueError, TypeError):
                        level = 0
                    try:
                        cycle = int(school.get("cycle") or 0)
                    except (ValueError, TypeError):
                        cycle = 0
                    schools.append({
                        "SchoolName": name,
                        "District":   str(school.get("District",  "")).strip(),
                        "Province":   str(school.get("Province",  "")).strip(),
                        "institute":  str(school.get("institute", "")).strip(),
                        "Emiscode":   str(school.get("Emiscode",  "")).strip(),
                        "Level":      level,
                        "Cycle":      cycle,
                    })

            print(f"  Processed pages {batch_start}-{batch_end - 1} ({len(schools)} schools so far)")

    return schools


def _cache_is_fresh() -> bool:
    if not CACHE_FILE.exists():
        return False
    try:
        mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime, tz=timezone.utc)
        if (datetime.now(tz=timezone.utc) - mtime).days >= CACHE_MAX_AGE_DAYS:
            return False
        with CACHE_FILE.open() as f:
            data = json.load(f)
        # New cache format is a list; old format was a dict — treat old format as stale
        return isinstance(data, list) and len(data) > 0
    except Exception:
        return False


def fetch_all_schools() -> list[dict]:
    if _cache_is_fresh():
        print("Loading schools from cache.")
        with CACHE_FILE.open() as f:
            return json.load(f)

    print(f"Cache missing or stale. Fetching all {TOTAL_PAGES} pages of schools...")
    schools = asyncio.run(_fetch_all_schools_async())
    print(f"Fetched {len(schools)} schools total.")

    with CACHE_FILE.open("w") as f:
        json.dump(schools, f)
    print("Schools saved to cache.")
    return schools


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rate(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "N/A"
    return f"{numerator / denominator * 100:.1f}%"


def _is_active(submitted: int, approved: int) -> bool:
    if ACTIVE_CRITERIA == "Approved Activities":
        return approved >= ACTIVE_THRESHOLD
    return submitted >= ACTIVE_THRESHOLD


def _school_lookup(all_schools: list[dict]) -> dict[str, dict]:
    return {s["SchoolName"]: s for s in all_schools}


def _side_by_side(left_header: list, left_rows: list, right_header: list, right_rows: list, gap: int = 1) -> list[list]:
    """Merge two tables side-by-side with a gap column. Header row is the first row returned."""
    w = len(left_header)
    out = [left_header + [""] * gap + right_header]
    for i in range(max(len(left_rows), len(right_rows))):
        l = list(left_rows[i]) if i < len(left_rows) else [""] * w
        r = list(right_rows[i]) if i < len(right_rows) else [""] * len(right_header)
        out.append(l + [""] * gap + r)
    return out


def get_district_list(province_sub: list[dict], all_schools: list[dict]) -> list[str]:
    if DISTRICTS_INPUT:
        return [d.strip() for d in DISTRICTS_INPUT.split(",") if d.strip()]
    if PROVINCE.lower() == "sindh":
        return SINDH_DISTRICTS
    # Auto-detect from data for other provinces
    districts: set[str] = set()
    for p in province_sub:
        d = p.get("district", "").strip()
        if d:
            districts.add(d)
    for s in all_schools:
        if (s["Province"].lower() == PROVINCE.lower()
                and s["institute"].upper() == INSTITUTE.upper()):
            d = s["District"].strip()
            if d:
                districts.add(d)
    return sorted(districts)


# ── Section B: Report Calculations ───────────────────────────────────────────

def calc_reports_summary(overall_sub, overall_app, province_sub, province_app):
    header = ["", "Submitted", "Approved", "Not Approved", "Approval Rate"]
    prov_label = f"{PROVINCE}/{INSTITUTE}"

    rows = [
        ["Overall Program",
         len(overall_sub), len(overall_app),
         len(overall_sub) - len(overall_app),
         _rate(len(overall_app), len(overall_sub))],
        [prov_label,
         len(province_sub), len(province_app),
         len(province_sub) - len(province_app),
         _rate(len(province_app), len(province_sub))],
    ]

    if DISTRICT:
        dl = DISTRICT.strip().lower()
        dist_sub = [p for p in province_sub if p.get("district", "").strip().lower() == dl]
        dist_app = [p for p in province_app if p.get("district", "").strip().lower() == dl]
        rows.append([
            f"{prov_label}/{DISTRICT}",
            len(dist_sub), len(dist_app),
            len(dist_sub) - len(dist_app),
            _rate(len(dist_app), len(dist_sub)),
        ])

    return "Reports - Summary", header, rows


def calc_reports_by_district(province_sub, province_app, all_schools):
    sub_by_dist = defaultdict(int)
    app_by_dist = defaultdict(int)
    for p in province_sub:
        d = p.get("district", "").strip()
        if d:
            sub_by_dist[d] += 1
    for p in province_app:
        d = p.get("district", "").strip()
        if d:
            app_by_dist[d] += 1

    # Per-school counts to derive active schools per district
    sub_per_school = defaultdict(int)
    app_per_school = defaultdict(int)
    school_to_dist = {}
    for p in province_sub:
        name = p.get("schoolName", "").strip()
        dist = p.get("district", "").strip()
        if name:
            sub_per_school[name] += 1
            if dist:
                school_to_dist[name] = dist
    for p in province_app:
        name = p.get("schoolName", "").strip()
        if name:
            app_per_school[name] += 1

    active_by_dist = defaultdict(int)
    for name in set(sub_per_school) | set(app_per_school):
        if _is_active(sub_per_school[name], app_per_school[name]):
            d = school_to_dist.get(name, "")
            if d:
                active_by_dist[d] += 1

    # Registered SELD schools per district
    filtered = [s for s in all_schools
                if s["Province"].lower() == PROVINCE.lower()
                and s["institute"].upper() == INSTITUTE.upper()]
    reg_by_dist = defaultdict(int)
    for s in filtered:
        d = s["District"].strip()
        if d:
            reg_by_dist[d] += 1

    district_list = get_district_list(province_sub, all_schools)
    header = ["District", "Submitted", "Approved", "Not Approved", "Approval Rate",
              "Active Schools", "Registered Schools", "Active as % of Registered"]
    rows = []
    for dist in district_list:
        sub  = sub_by_dist.get(dist, 0)
        app  = app_by_dist.get(dist, 0)
        act  = active_by_dist.get(dist, 0)
        reg  = reg_by_dist.get(dist, 0)
        rows.append([dist, sub, app, sub - app, _rate(app, sub), act, reg, _rate(act, reg)])

    t_sub = sum(r[1] for r in rows)
    t_app = sum(r[2] for r in rows)
    t_act = sum(r[5] for r in rows)
    t_reg = sum(r[6] for r in rows)
    rows.append(["TOTAL", t_sub, t_app, t_sub - t_app,
                 _rate(t_app, t_sub), t_act, t_reg, _rate(t_act, t_reg)])

    return "Reports - By District", header, rows


def calc_active_schools(province_sub, province_app, all_schools):
    lookup = _school_lookup(all_schools)

    sub_per_school = defaultdict(int)
    app_per_school = defaultdict(int)
    for p in province_sub:
        name = p.get("schoolName", "").strip()
        if name:
            sub_per_school[name] += 1
    for p in province_app:
        name = p.get("schoolName", "").strip()
        if name:
            app_per_school[name] += 1

    dl = DISTRICT.strip().lower() if DISTRICT else ""
    rows = []
    for name in set(sub_per_school) | set(app_per_school):
        sub = sub_per_school[name]
        app = app_per_school[name]
        if not _is_active(sub, app):
            continue
        school = lookup.get(name, {})
        if dl and school.get("District", "").strip().lower() != dl:
            continue
        rows.append([
            name,
            school.get("Emiscode", "N/A"),
            school.get("District", "N/A"),
            school.get("institute", "N/A"),
            school.get("Province", "N/A"),
            sub, app, _rate(app, sub),
        ])

    sort_col = 6 if ACTIVE_CRITERIA == "Approved Activities" else 5
    rows.sort(key=lambda r: r[sort_col], reverse=True)

    header = ["School Name", "EMIS Code", "District", "Institute", "Province",
              "Submitted", "Approved", "Approval Rate"]
    criteria_note = [f"Criteria: {ACTIVE_CRITERIA} >= {ACTIVE_THRESHOLD}",
                     "", "", "", "", "", "", ""]
    return "Active Schools", header, [criteria_note] + rows


# ── Raw Data Tabs ─────────────────────────────────────────────────────────────

def calc_posts_data(province_sub, province_app, all_schools):
    left_header = ["School Name", "District", "Province", "Institute"]
    left_rows = [
        [p.get("schoolName", ""), p.get("district", ""),
         p.get("province", ""),  p.get("institute", "")]
        for p in province_sub
    ]

    # Compute active school count for summary
    sub_per = defaultdict(int)
    app_per = defaultdict(int)
    for p in province_sub:
        name = p.get("schoolName", "").strip()
        if name:
            sub_per[name] += 1
    for p in province_app:
        name = p.get("schoolName", "").strip()
        if name:
            app_per[name] += 1
    active_count = sum(1 for n in set(sub_per) | set(app_per)
                       if _is_active(sub_per[n], app_per[n]))

    tot_sub = len(province_sub)
    tot_app = len(province_app)
    right_header = ["Metric", "Value"]
    right_rows = [
        ["Total Submitted",  tot_sub],
        ["Total Approved",   tot_app],
        ["Not Approved",     tot_sub - tot_app],
        ["Approval Rate",    _rate(tot_app, tot_sub)],
        ["Active Schools",   active_count],
    ]

    merged = _side_by_side(left_header, left_rows, right_header, right_rows)
    return "Posts Data", [], merged


def calc_schools_data(all_schools):
    filtered = [s for s in all_schools
                if s["Province"].lower() == PROVINCE.lower()
                and s["institute"].upper() == INSTITUTE.upper()]

    left_header = ["School Name", "EMIS Code", "District", "Province", "Institute", "Level", "Cycle"]
    left_rows = [
        [s["SchoolName"], s["Emiscode"], s["District"],
         s["Province"],   s["institute"], s["Level"],  s["Cycle"]]
        for s in filtered
    ]

    by_inst = defaultdict(int)
    for s in filtered:
        by_inst[s["institute"].strip() or "Unknown"] += 1

    right_header = ["Metric", "Value"]
    right_rows = [["Total Registered", len(filtered)], ["", ""]]
    right_rows.append(["By Institute", "Count"])
    for inst, cnt in sorted(by_inst.items(), key=lambda x: -x[1]):
        right_rows.append([inst, cnt])

    merged = _side_by_side(left_header, left_rows, right_header, right_rows)
    return "Schools - SELD Data", [], merged


# ── Section C: Schools Calculations ──────────────────────────────────────────

def calc_schools_summary(all_schools):
    by_inst = defaultdict(int)
    by_prov = defaultdict(int)
    for s in all_schools:
        by_inst[s["institute"].strip() or "Unknown"] += 1
        by_prov[s["Province"].strip()  or "Unknown"] += 1

    rows = [
        ["Total Schools Registered", len(all_schools)],
        ["", ""],
        ["By Institute", "Count"],
    ]
    for inst, cnt in sorted(by_inst.items(), key=lambda x: -x[1]):
        rows.append([inst, cnt])
    rows += [["", ""], ["By Province", "Count"]]
    for prov, cnt in sorted(by_prov.items(), key=lambda x: -x[1]):
        rows.append([prov, cnt])

    return "Schools - Summary", ["Category", "Count"], rows


def calc_schools_by_district(all_schools):
    filtered = [s for s in all_schools
                if s["Province"].lower() == PROVINCE.lower()
                and s["institute"].upper() == INSTITUTE.upper()]

    all_dist  = defaultdict(int)
    filt_dist = defaultdict(int)
    for s in all_schools:
        d = s["District"].strip()
        if d:
            all_dist[d] += 1
    for s in filtered:
        d = s["District"].strip()
        if d:
            filt_dist[d] += 1

    district_list = get_district_list([], all_schools)
    header = ["District", "Total Registered (All Institutes)",
              f"{PROVINCE}/{INSTITUTE} Registered"]
    rows = [[d, all_dist.get(d, 0), filt_dist.get(d, 0)] for d in district_list]
    rows.append(["TOTAL", sum(r[1] for r in rows), sum(r[2] for r in rows)])
    return "Schools - By District", header, rows


def _matrix_section(schools: list[dict], label: str, level_x_cycle: bool) -> list[list]:
    cycle_vals = set()
    for s in schools:
        c = s["Cycle"]
        if c > 0:
            cycle_vals.add(c if c < 4 else "4+")
    cycles = sorted([c for c in cycle_vals if isinstance(c, int)]) + \
             (["4+"] if "4+" in cycle_vals else [])
    if not cycles:
        cycles = [1, 2, 3, "4+"]

    levels = sorted(set(s["Level"] for s in schools if s["Level"] > 0))
    if not levels:
        levels = list(range(1, 16))

    counts: dict = defaultdict(int)
    for s in schools:
        lv, cy = s["Level"], s["Cycle"]
        if lv <= 0 or cy <= 0:
            continue
        counts[(lv, cy if cy < 4 else "4+")] += 1

    width = (len(levels) if not level_x_cycle else len(cycles)) + 1
    section = [[f"=== {label} ==="] + [""] * (width - 1)]

    if level_x_cycle:
        section.append(["Level \\ Cycle"] + [f"Cycle {c}" for c in cycles])
        for lv in levels:
            section.append([f"Level {lv}"] + [counts.get((lv, c), 0) for c in cycles])
    else:
        section.append(["Cycle \\ Level"] + [f"Level {lv}" for lv in levels])
        for cy in cycles:
            section.append([f"Cycle {cy}"] + [counts.get((lv, cy), 0) for lv in levels])

    section.append([""] * width)
    return section


def calc_schools_level_x_cycle(all_schools):
    filtered = [s for s in all_schools
                if s["Province"].lower() == PROVINCE.lower()
                and s["institute"].upper() == INSTITUTE.upper()]
    rows = _matrix_section(all_schools, "Overall Program", True)
    rows += _matrix_section(filtered, f"{PROVINCE}/{INSTITUTE}", True)
    if DISTRICT:
        dl = DISTRICT.strip().lower()
        rows += _matrix_section(
            [s for s in filtered if s["District"].strip().lower() == dl], DISTRICT, True)
    return "Schools - Level x Cycle", [], rows


def calc_schools_cycle_x_level(all_schools):
    filtered = [s for s in all_schools
                if s["Province"].lower() == PROVINCE.lower()
                and s["institute"].upper() == INSTITUTE.upper()]
    rows = _matrix_section(all_schools, "Overall Program", False)
    rows += _matrix_section(filtered, f"{PROVINCE}/{INSTITUTE}", False)
    if DISTRICT:
        dl = DISTRICT.strip().lower()
        rows += _matrix_section(
            [s for s in filtered if s["District"].strip().lower() == dl], DISTRICT, False)
    return "Schools - Cycle x Level", [], rows


# ── Section D: Upload ─────────────────────────────────────────────────────────

def _get_or_create_ws(sh, title: str):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=2000, cols=60)


def upload_all_tabs(tab_data: list[tuple]) -> str:
    sa_json  = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    sheet_id = SHEET_ID_OVERRIDE or os.environ["SHEET_ID"]

    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    for tab_name, header, rows in tab_data:
        ws = _get_or_create_ws(sh, tab_name)
        ws.clear()
        data = ([header] if header else []) + rows
        if data:
            ws.update(range_name="A1", values=data)
        print(f"  Written: {tab_name} ({len(rows)} rows)")

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    print(f"All tabs uploaded: {url}")
    return url


# ── Section E: Email ──────────────────────────────────────────────────────────

def send_email(tab_names: list[str], sheet_url: str) -> None:
    gmail_user     = os.environ["GMAIL_USER"]
    gmail_password = os.environ["GMAIL_APP_PASSWORD"]
    recipient      = os.environ["RECIPIENT_EMAIL"]

    filters = (f"province={PROVINCE}, institute={INSTITUTE}, "
               f"district={DISTRICT or 'all'}, "
               f"dateFrom={DATE_FROM}, dateTo={DATE_TO}")

    body = (
        f"School Reports Automation completed.\n\n"
        f"Filters: {filters}\n"
        f"Active Schools: {ACTIVE_CRITERIA} >= {ACTIVE_THRESHOLD}\n\n"
        f"Tabs written:\n" + "\n".join(f"  - {t}" for t in tab_names) +
        f"\n\nSheet: {sheet_url}"
    )
    msg = MIMEText(body)
    msg["Subject"] = "School Reports Automation: Done"
    msg["From"]    = gmail_user
    msg["To"]      = recipient

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as server:
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, recipient, msg.as_string())
    print(f"Email sent to {recipient}.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    date_params = {"dateFrom": DATE_FROM, "dateTo": DATE_TO}
    prov_params = {"province": PROVINCE, "institute": INSTITUTE, **date_params}

    print("=== Step 1: Fetching posts (4 calls) ===")
    print("  Fetching overall submitted...")
    overall_sub = fetch_posts(date_params)
    print(f"  -> {len(overall_sub)} posts")

    print("  Fetching overall approved...")
    overall_app = fetch_posts({**date_params, "status": "approved"})
    print(f"  ->{len(overall_app)} posts")

    print(f"  Fetching {PROVINCE}/{INSTITUTE} submitted...")
    province_sub = fetch_posts(prov_params)
    print(f"  ->{len(province_sub)} posts")

    print(f"  Fetching {PROVINCE}/{INSTITUTE} approved...")
    province_app = fetch_posts({**prov_params, "status": "approved"})
    print(f"  ->{len(province_app)} posts")

    print("\n=== Step 2: Fetching schools ===")
    all_schools = fetch_all_schools()
    print(f"  ->{len(all_schools)} schools total")

    print("\n=== Step 3: Running calculations ===")
    tab_data = [
        calc_posts_data(province_sub, province_app, all_schools),
        calc_reports_summary(overall_sub, overall_app, province_sub, province_app),
        calc_reports_by_district(province_sub, province_app, all_schools),
        calc_active_schools(province_sub, province_app, all_schools),
        calc_schools_data(all_schools),
        calc_schools_summary(all_schools),
        calc_schools_by_district(all_schools),
        calc_schools_level_x_cycle(all_schools),
        calc_schools_cycle_x_level(all_schools),
    ]

    print("\n=== Step 4: Uploading to Google Sheets ===")
    sheet_url = upload_all_tabs(tab_data)

    print("\n=== Step 5: Sending email ===")
    send_email([t[0] for t in tab_data], sheet_url)

    print("\nDone.")


if __name__ == "__main__":
    main()
