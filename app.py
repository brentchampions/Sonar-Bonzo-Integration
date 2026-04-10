from fastapi import FastAPI, Request, HTTPException, Response
import requests
import os
import json

app = FastAPI()

BONZO_URL = os.getenv("BONZO_URL")


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
