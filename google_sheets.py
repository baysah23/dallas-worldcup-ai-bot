import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS_FILE = "google_creds.json"
SHEET_NAME = "World Cup AI Reservations"

def append_lead(name, phone, date, time, party_size):
    creds = Credentials.from_service_account_file(
        CREDS_FILE, scopes=SCOPES
    )
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).sheet1

    sheet.append_row([
        datetime.now().isoformat(timespec="seconds"),
        name,
        phone,
        date,
        time,
        party_size
    ])
