# Steam Automation — School Reports Pipeline

Fetches school activity reports from the Wrapper-Func API, calculates active schools, and uploads results to Google Sheets with an email notification.

---

## How to Run

Go to **Actions → School Reports Automation → Run workflow** and fill in the form:

| Field | Description | Default |
|---|---|---|
| Province | Province to filter by | Sindh |
| District | Specific district, or leave blank for all | *(all)* |
| Institute | Institute to filter by | SELD |
| Date From | Start date `MM/DD/YYYY` | 12/01/2025 |
| Date To | End date `MM/DD/YYYY` | 01/31/2026 |
| Status filter | `all`, `approved`, `pending`, or `rejected` | all |
| Calculation to run | Which calculation to perform | Active Schools |
| Google Sheet ID | Sheet to write results to, or leave blank for default | *(default)* |

Click **Run workflow**. You will receive an email when complete.

> The pipeline also runs automatically on the **1st of every 2 months at 8am** using the default filters.

---

## Calculations

### Active Schools
Counts how many times each school appears in the filtered reports. Schools that appear **2 or more times** are considered active. Results are written to Google Sheets with columns:

| School Name | EMIS Code | Appearance Count |
|---|---|---|

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
