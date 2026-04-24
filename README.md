# Steam Automation — School Reports Pipeline

Fetches school activity reports from the Wrapper-Func API, calculates active schools, analyzes school registrations by district and classification, and uploads results to Google Sheets with an email notification.

---

## How to Run

Go to **Actions → School Reports Automation → Run workflow** and fill in the form:

| Field | Description | Default |
|---|---|---|
| Province | Province to filter by | Sindh |
| District(s) | Leave blank for all districts, or enter one district to drill down, or comma-separated for breakdown tables only | *(all)* |
| Institute | Leave blank for all institutes in the selected province | *(all)* |
| Date From | Start date `MM/DD/YYYY` | 12/01/2025 |
| Date To | End date `MM/DD/YYYY` | 01/31/2026 |
| Active Schools: count by | Submitted Activities or Approved Activities | Submitted Activities |
| Active Schools: minimum count | Minimum activity count to mark a school active | 2 |
| Google Sheet ID | Sheet to write results to, or leave blank for default | *(default)* |

Click **Run workflow**. You will receive an email when complete. The selected date range appears in the top-right of every sheet.

> The pipeline also runs automatically on the **1st of every 2 months at 8am** using the default filters.

---

## Output Sheets (7 tabs)

1. **Reports - Summary** — Aggregate submission and approval counts by filter level (Overall, Province, Institute, and optionally District). When both Province and Institute are selected, they appear as separate rows.
2. **Reports - By District** — Per-district submission, approval, and active school counts.
3. **Active Schools** — Schools meeting the active schools criteria (by count of submitted or approved activities).
4. **Schools - Summary** — Total schools registered, grouped by Institute and by Province.
5. **Schools - By District** — Registered schools per district.
6. **Schools - Level x Cycle** — Matrix of registered schools by Level (rows) and Cycle (columns).
7. **Schools - Cycle x Level** — Matrix of registered schools by Cycle (rows) and Level (columns).

---

## Google Sheets Setup

For any sheet you want results written to, share it with the service account:

1. Open the Google Sheet
2. Click **Share**
3. Add `steam-account@steam-494021.iam.gserviceaccount.com` as **Editor**
4. Click Send

---

## Required GitHub Secrets

Go to **Settings → Secrets and variables → Actions** to add:

| Secret | Description |
|---|---|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Full contents of the Google service account JSON key file (minified to one line) |
| `SHEET_ID` | Default Google Sheet ID (from the sheet URL) |
| `GMAIL_USER` | Gmail address used to send notifications |
| `GMAIL_APP_PASSWORD` | Gmail App Password (Google Account → Security → App Passwords) |
| `RECIPIENT_EMAIL` | Email address that receives the completion notification |

---

## Adding Team Members

To allow someone to trigger the workflow:

**Settings → Collaborators → Add people** → enter their GitHub username → role: **Write**
