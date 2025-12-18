from dotenv import load_dotenv
load_dotenv()

import csv
import os
import json
from datetime import datetime

from flask import Flask, request, jsonify, send_from_directory
from openai import OpenAI
from pydantic import BaseModel, ValidationError

app = Flask(__name__)
client = OpenAI()

# -----------------------------
# Load business profile once
# -----------------------------
BUSINESS_PROFILE_PATH = "business_profile.txt"
if not os.path.exists(BUSINESS_PROFILE_PATH):
    raise FileNotFoundError(
        "business_profile.txt not found. Create it in the same folder as app.py."
    )

with open(BUSINESS_PROFILE_PATH, "r", encoding="utf-8") as f:
    business_profile = f.read()

# -----------------------------
# Lead capture CSV file
# -----------------------------
LEADS_FILE = "leads.csv"


class ReservationLead(BaseModel):
    name: str
    phone: str
    date: str
    time: str
    party_size: int


def save_lead(lead: ReservationLead):
    """Append a reservation lead to leads.csv (local/dev)."""
    file_exists = os.path.exists(LEADS_FILE)
    with open(LEADS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "name", "phone", "date", "time", "party_size"])
        writer.writerow([
            datetime.now().isoformat(timespec="seconds"),
            lead.name,
            lead.phone,
            lead.date,
            lead.time,
            lead.party_size
        ])


# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def home():
    # Serve your index.html from the same folder
    return send_from_directory(".", "index.html")


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(force=True)
    user_message = (data.get("message") or "").strip()

    if not user_message:
        return jsonify({"reply": "Please type a message."})

    system_msg = f"""
You are a World Cup 2026 AI Concierge for a Dallas-area business.

Use the following business profile as your source of truth:
{business_profile}

Goals:
1) Answer customer questions about the business (hours, address, specials)
2) Help customers make reservations or inquiries
3) Be friendly, fast, and concise
4) Auto-detect language and respond in it (English, Spanish, Portuguese, French)

Reservation workflow (IMPORTANT):
- If the user wants a reservation, collect ONLY: date, time, party size, name, phone number.
- Once you have all 5, output a FINAL line exactly in this format on ONE line:
  RESERVATION_JSON={{"name":"...","phone":"...","date":"...","time":"...","party_size":4}}
- After that, confirm the reservation details to the user.

Rules:
- If something is unknown, say you’re not sure and offer a callback to the business phone.
- Never mention being an AI unless the user asks directly.
"""

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_message},
        ],
    )

    reply = (resp.output_text or "").strip()

    # -----------------------------
    # Try to extract the reservation JSON
    # -----------------------------
    if "RESERVATION_JSON=" in reply:
        before, after = reply.split("RESERVATION_JSON=", 1)
        json_text = after.strip()

        try:
            lead_dict = json.loads(json_text)
            lead = ReservationLead(**lead_dict)
            save_lead(lead)

            # Clean user-facing reply (hide the JSON line)
            visible_reply = before.strip()
            if not visible_reply:
                visible_reply = (
                    f"✅ Reservation saved!\n"
                    f"Name: {lead.name}\n"
                    f"Phone: {lead.phone}\n"
                    f"Date: {lead.date}\n"
                    f"Time: {lead.time}\n"
                    f"Party size: {lead.party_size}\n\n"
                    f"We’ll confirm shortly."
                )
            else:
                visible_reply += (
                    f"\n\n✅ Reservation saved!\n"
                    f"Name: {lead.name}\n"
                    f"Phone: {lead.phone}\n"
                    f"Date: {lead.date}\n"
                    f"Time: {lead.time}\n"
                    f"Party size: {lead.party_size}\n\n"
                    f"We’ll confirm shortly."
                )

            return jsonify({"reply": visible_reply})

        except (json.JSONDecodeError, ValidationError):
            # If parsing fails, just return the original AI reply
            return jsonify({"reply": reply})

        except Exception:
            return jsonify({"reply": reply})

    return jsonify({"reply": reply})


# -----------------------------
# Run locally OR on hosted platforms
# -----------------------------
if __name__ == "__main__":
    # Render/hosting sets PORT; local defaults to 5050
    port = int(os.environ.get("PORT", 5050))
    # 0.0.0.0 allows external access when hosted
    app.run(host="0.0.0.0", port=port, debug=False)
