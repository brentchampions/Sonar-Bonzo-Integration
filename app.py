from fastapi import FastAPI, Request, HTTPException
import requests
import os

app = FastAPI()

BONZO_URL = os.getenv("BONZO_URL")


@app.get("/")
def home():
    return {"status": "running"}


@app.get("/health")
def health():
    return {"ok": True}


def clean(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def map_sonar_to_bonzo(data):
    tags = ["Sonar"]

    for key in ["LeadStage", "LoanPurpose", "LoanStatus"]:
        val = clean(data.get(key))
        if val:
            tags.append(f"{key}:{val}")

    payload = {
        "first_name": clean(data.get("FirstName")) or "Unknown",
        "last_name": clean(data.get("LastName")) or "Unknown",
        "email": clean(data.get("Email")),
        "phone": clean(data.get("DayPhone")),
        "address": clean(data.get("Street")),
        "city": clean(data.get("City")),
        "prospect_state": clean(data.get("State")),
        "prospect_zip": clean(data.get("ZipCode")),
        "external_id": clean(data.get("LoanId")),
        "tags": ", ".join(tags),
    }

    return {k: v for k, v in payload.items() if v is not None}


@app.post("/sonar")
async def receive_sonar(request: Request):
    if not BONZO_URL:
        raise HTTPException(status_code=500, detail="BONZO_URL is not configured")

    data = await request.json()

    bonzo_payload = map_sonar_to_bonzo(data)

    response = requests.post(BONZO_URL, json=bonzo_payload)

    return {
        "status": "sent_to_bonzo",
        "bonzo_status_code": response.status_code,
        "bonzo_response": response.text,
        "sent_payload": bonzo_payload
    }
