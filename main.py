import asyncio
import json
import os
import smtplib
import ssl
from collections import Counter
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

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ── Section A: Fetch Posts ────────────────────────────────────────────────────

def fetch_posts() -> list[dict]:
    params = {
        "province": "Sindh",
        "institute": "SELD",
        "dateFrom": "12/01/2025",
        "dateTo": "01/31/2026",
    }
    with httpx.Client(timeout=60) as client:
        resp = client.get(f"{BASE_URL}/posts", params=params)
        resp.raise_for_status()
        data = resp.json()

    print(f"Fetched {len(data)} posts.")
    return data


# ── Section B: Fetch Schools (async, cached) ─────────────────────────────────

TOTAL_PAGES = 16904
CONCURRENCY = 30


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


async def _fetch_all_schools_async() -> dict[str, str]:
    sem = asyncio.Semaphore(CONCURRENCY)
    school_map: dict[str, str] = {}
    batch_size = 500

    async with httpx.AsyncClient(http2=True, timeout=30) as client:
        for batch_start in range(1, TOTAL_PAGES + 1, batch_size):
            batch_end = min(batch_start + batch_size, TOTAL_PAGES + 1)
            tasks = [_fetch_page(client, sem, p) for p in range(batch_start, batch_end)]
            results = await asyncio.gather(*tasks)

            for page_data in results:
                for school in page_data:
                    # Actual API field names: Province (capital P), institute (lowercase)
                    province = str(school.get("Province", "")).strip().lower()
                    institute = str(school.get("institute", "")).strip().upper()
                    if province == "sindh" and institute == "SELD":
                        name = str(school.get("SchoolName", "")).strip()
                        emis = str(school.get("Emiscode", "")).strip()
                        if name:
                            school_map[name] = emis

            print(f"  Processed pages {batch_start}-{batch_end - 1} ({len(school_map)} schools so far)")

    return school_map


def _cache_is_fresh() -> bool:
    if not CACHE_FILE.exists():
        return False
    mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime, tz=timezone.utc)
    age_days = (datetime.now(tz=timezone.utc) - mtime).days
    return age_days < CACHE_MAX_AGE_DAYS


def fetch_schools() -> dict[str, str]:
    if _cache_is_fresh():
        print("Loading schools from cache.")
        with CACHE_FILE.open() as f:
            return json.load(f)

    print(f"Cache missing or stale. Fetching all {TOTAL_PAGES} pages of schools...")
    school_map = asyncio.run(_fetch_all_schools_async())
    print(f"Fetched {len(school_map)} Sindh/SELD schools.")

    with CACHE_FILE.open("w") as f:
        json.dump(school_map, f)
    print("Schools saved to cache.")
    return school_map


# ── Section C: Merge & Calculate Active Schools ───────────────────────────────

def merge_and_calculate(posts: list[dict], school_map: dict[str, str]) -> list[list]:
    counts: Counter = Counter()
    for post in posts:
        # Actual posts field name is schoolName (camelCase)
        name = str(post.get("schoolName", "")).strip()
        if name:
            counts[name] += 1

    rows = []
    for name, count in counts.items():
        if count >= 2:
            emis = school_map.get(name, "N/A")
            rows.append([name, emis, count])

    rows.sort(key=lambda r: r[2], reverse=True)
    print(f"Found {len(rows)} active schools (appeared 2+ times).")
    return rows


# ── Section D: Upload to Google Sheets ───────────────────────────────────────

def upload_to_sheets(rows: list[list]) -> str:
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    sheet_id = os.environ["SHEET_ID"]

    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1

    ws.clear()
    header = [["School Name", "EMIS Code", "Appearance Count"]]
    ws.update(range_name="A1", values=header + rows)

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    print(f"Uploaded {len(rows)} rows to Google Sheets: {url}")
    return url


# ── Section E: Send Email ─────────────────────────────────────────────────────

def send_email(n_active: int, sheet_url: str) -> None:
    gmail_user = os.environ["GMAIL_USER"]
    gmail_password = os.environ["GMAIL_APP_PASSWORD"]
    recipient = os.environ["RECIPIENT_EMAIL"]

    msg = MIMEText(
        f"Active schools upload completed.\n\n"
        f"{n_active} active schools written to Google Sheets.\n\n"
        f"Sheet: {sheet_url}"
    )
    msg["Subject"] = "School Reports Automation: Done"
    msg["From"] = gmail_user
    msg["To"] = recipient

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as server:
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, recipient, msg.as_string())
    print(f"Email sent to {recipient}.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=== Step 1: Fetching posts ===")
    posts = fetch_posts()

    print("\n=== Step 2: Fetching schools ===")
    school_map = fetch_schools()

    print("\n=== Step 3: Merging & calculating active schools ===")
    rows = merge_and_calculate(posts, school_map)

    print("\n=== Step 4: Uploading to Google Sheets ===")
    sheet_url = upload_to_sheets(rows)

    print("\n=== Step 5: Sending email ===")
    send_email(len(rows), sheet_url)

    print("\nDone.")


if __name__ == "__main__":
    main()
