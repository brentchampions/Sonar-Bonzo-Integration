from fastapi import FastAPI, Request, HTTPException, Response
import requests
import os
import json
import time
import hashlib

app = FastAPI()

BONZO_URL = os.getenv("BONZO_URL")

# in-memory dedupe cache
# fingerprint -> last_seen_unix
recent_payloads = {}

# how long to suppress identical payloads (seconds)
DEDUP_WINDOW_SECONDS = 60 * 30  # 30 minutes


@app.get("/")
def home():
    return {"status": "running"}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/sonar")
def sonar_get():
    return {"status": "sonar endpoint ready"}


@app.head("/sonar")
def sonar_head():
    return Response(status_code=200)


def clean(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def clean_phone(phone):
    if not phone:
        return None
    digits = "".join(filter(str.isdigit, str(phone)))
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return clean(phone)


def to_number(value):
    if value in (None, ""):
        return None
    try:
        s = str(value).replace("$", "").replace(",", "").strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def to_int(value):
    n = to_number(value)
    return int(n) if n is not None else None


def remove_empty(obj):
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            v2 = remove_empty(v)
            if v2 not in (None, "", [], {}):
                cleaned[k] = v2
        return cleaned
    if isinstance(obj, list):
        cleaned = [remove_empty(v) for v in obj]
        return [v for v in cleaned if v not in (None, "", [], {})]
    return obj


def fingerprint_payload(data):
    """
    Create a stable fingerprint of the incoming Sonar payload.
    Empty values are removed so trivial blanks do not create false differences.
    """
    normalized = remove_empty(data)
    stable_json = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(stable_json.encode("utf-8")).hexdigest(), stable_json


def purge_old_fingerprints(now_ts):
    expired = [
        fp for fp, ts in recent_payloads.items()
        if now_ts - ts > DEDUP_WINDOW_SECONDS
    ]
    for fp in expired:
        del recent_payloads[fp]


def map_sonar_to_bonzo(data):
    tags = ["sonar"]

    for key in ["LeadStage", "LoanPurpose", "LoanStatus"]:
        val = clean(data.get(key))
        if val:
            tags.append(f"{key.lower()}:{val.lower()}")

    payload = {
        # basic prospect fields Bonzo is already accepting
        "first_name": clean(data.get("FirstName")) or "Unknown",
        "last_name": clean(data.get("LastName")) or "Unknown",
        "email": clean(data.get("Email")),
        "phone": clean_phone(data.get("DayPhone")),
        "address": clean(data.get("Street")),
        "city": clean(data.get("City")),
        "prospect_state": clean(data.get("State")),
        "prospect_zip": clean(data.get("ZipCode")),
        "external_id": clean(data.get("LoanId")) or clean(data.get("RefId")),
        "custom_id": clean(data.get("RefId")),
        "tags": ", ".join(tags),

        # extra fields for Bonzo webhook mapping tab
        "loan_amount": to_number(data.get("LoanAmount")),
        "purchase_price": to_number(data.get("PurchasePrice")),
        "down_payment": to_number(data.get("DownPaymentAmount")),
        "down_payment_percent": to_number(data.get("DownPayment(%)")),
        "loan_purpose": clean(data.get("LoanPurpose")),
        "property_type": clean(data.get("PropertyType")),
        "property_use": clean(data.get("IntendedPropertyUse")),
        "property_address": clean(data.get("PropertyStreet")),
        "property_city": clean(data.get("PropertyCity")),
        "property_state": clean(data.get("PropertyState")),
        "property_zip": clean(data.get("PropertyZipCode")),
        "credit_score": to_int(data.get("CreditScore")),
        "household_income": to_number(data.get("TotalHouseholdIncome")),
        "monthly_mortgage_payment": to_number(data.get("MonthlyMortgagePayment")),
        "lead_source": "Sonar",
        "lead_id": clean(data.get("LoanId")),
        "loan_status": clean(data.get("LoanStatus")),
        "milestone": clean(data.get("Milestone")),
        "purchase_intent": clean(data.get("PurchaseIntent")),
        "originator_name": clean(data.get("OriginatorName")),
        "originator_email": clean(data.get("OriginatorBusinessEmail")),
        "loan_url": clean(data.get("LoanUrl")),
        "utm_source": clean(data.get("UtmSource")),
        "utm_campaign": clean(data.get("UtmCampaign")),
        "utm_medium": clean(data.get("UtmMedium")),
    }

    return {k: v for k, v in payload.items() if v not in (None, "", [])}


@app.post("/sonar")
async def receive_sonar(request: Request):
    if not BONZO_URL:
        raise HTTPException(status_code=500, detail="BONZO_URL is not configured")

    try:
        data = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    print("=== INCOMING FROM SONAR ===", flush=True)
    print(json.dumps(data, indent=2, default=str), flush=True)

    # basic quality gates
    first_name = clean(data.get("FirstName"))
    last_name = clean(data.get("LastName"))
    email = clean(data.get("Email"))
    phone = clean_phone(data.get("DayPhone"))
    loan_id = clean(data.get("LoanId")) or clean(data.get("RefId"))

    if not first_name or not last_name:
        print("=== IGNORED === missing first/last name", flush=True)
        return {"status": "ignored", "reason": "missing first/last name"}

    if not email and not phone:
        print("=== IGNORED === missing both email and phone", flush=True)
        return {"status": "ignored", "reason": "missing both email and phone"}

    if not loan_id:
        print("=== IGNORED === missing loan/ref id", flush=True)
        return {"status": "ignored", "reason": "missing loan/ref id"}

    # dedupe exact same payload
    now_ts = time.time()
    purge_old_fingerprints(now_ts)

    fingerprint, stable_json = fingerprint_payload(data)

    if fingerprint in recent_payloads:
        print("=== IGNORED DUPLICATE PAYLOAD ===", flush=True)
        print(f"Fingerprint: {fingerprint}", flush=True)
        print(stable_json, flush=True)
        return {
            "status": "ignored",
            "reason": "duplicate identical payload",
            "loan_id": loan_id,
            "fingerprint": fingerprint
        }

    recent_payloads[fingerprint] = now_ts

    bonzo_payload = map_sonar_to_bonzo(data)

    print("=== SENDING TO BONZO ===", flush=True)
    print(json.dumps(bonzo_payload, indent=2, default=str), flush=True)

    try:
        response = requests.post(BONZO_URL, json=bonzo_payload, timeout=30)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error sending to Bonzo: {e}")

    print("=== BONZO RESPONSE ===", flush=True)
    print(f"Status: {response.status_code}", flush=True)
    print(f"Body: {response.text}", flush=True)

    return {
        "status": "sent_to_bonzo",
        "bonzo_status_code": response.status_code,
        "bonzo_response": response.text[:1000],
        "sent_payload": bonzo_payload
    }
