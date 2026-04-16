from fastapi import FastAPI, Request, HTTPException, Response
import requests
import os
import json
import time

app = FastAPI()

BONZO_URL = os.getenv("BONZO_URL")

# In-memory loan state
# loan_id -> {
#   "first_sent": bool,
#   "submitted_seen": bool,
#   "last_post_submit_signature": str | None,
#   "last_seen_ts": float,
# }
loan_state = {}

# Clean up stale in-memory state after this many seconds
STATE_TTL_SECONDS = 60 * 60 * 24 * 7  # 7 days


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


def purge_old_state(now_ts: float):
    expired_ids = [
        loan_id
        for loan_id, state in loan_state.items()
        if now_ts - state.get("last_seen_ts", now_ts) > STATE_TTL_SECONDS
    ]
    for loan_id in expired_ids:
        del loan_state[loan_id]


def build_post_submit_signature(data):
    """
    Only track fields that matter for post-submit Bonzo pipeline movement.
    If these do not change, we suppress the update.
    """
    tracked = {
        "LeadStage": clean(data.get("LeadStage")),
        "LoanStatus": clean(data.get("LoanStatus")),
        "Milestone": clean(data.get("Milestone")),
    }
    return json.dumps(tracked, sort_keys=True, separators=(",", ":"), default=str)


def map_sonar_to_bonzo(data):
    tags = ["sonar"]

    for key in ["LeadStage", "LoanPurpose", "LoanStatus"]:
        val = clean(data.get(key))
        if val:
            tags.append(f"{key.lower()}:{val.lower()}")

    payload = {
        # core prospect fields
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

        # Bonzo mapping fields
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
        "lead_source": clean(data.get("Source")) or "Sonar",
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


def should_send_to_bonzo(data, now_ts):
    loan_id = clean(data.get("LoanId")) or clean(data.get("RefId"))
    lead_stage = clean(data.get("LeadStage"))
    loan_status = clean(data.get("LoanStatus"))
    milestone = clean(data.get("Milestone"))

    submitted_now = (
        milestone == "ApplicationSubmitted"
        or lead_stage == "Submit Application"
        or loan_status == "Active"
    )

    state = loan_state.get(
        loan_id,
        {
            "first_sent": False,
            "submitted_seen": False,
            "last_post_submit_signature": None,
            "last_seen_ts": now_ts,
        },
    )
    state["last_seen_ts"] = now_ts

    # 1) Always allow the very first valid payload for a loan
    if not state["first_sent"]:
        state["first_sent"] = True
        if submitted_now:
            state["submitted_seen"] = True
            state["last_post_submit_signature"] = build_post_submit_signature(data)
        loan_state[loan_id] = state
        return True, "first_valid_payload"

    # 2) Before submission, block noisy intermediate updates
    if not state["submitted_seen"]:
        if submitted_now:
            state["submitted_seen"] = True
            state["last_post_submit_signature"] = build_post_submit_signature(data)
            loan_state[loan_id] = state
            return True, "application_submitted_transition"

        loan_state[loan_id] = state
        return False, "pre_submit_noise"

    # 3) After submission, allow only meaningful pipeline-driving changes
    current_signature = build_post_submit_signature(data)
    if current_signature != state.get("last_post_submit_signature"):
        state["last_post_submit_signature"] = current_signature
        loan_state[loan_id] = state
        return True, "post_submit_meaningful_change"

    loan_state[loan_id] = state
    return False, "post_submit_duplicate"


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

    now_ts = time.time()
    purge_old_state(now_ts)

    should_send, reason = should_send_to_bonzo(data, now_ts)

    if not should_send:
        print("=== IGNORED EVENT ===", flush=True)
        print(f"LoanId: {loan_id}", flush=True)
        print(f"Reason: {reason}", flush=True)
        return {
            "status": "ignored",
            "loan_id": loan_id,
            "reason": reason,
        }

    print("=== ACCEPTED EVENT ===", flush=True)
    print(f"LoanId: {loan_id}", flush=True)
    print(f"Reason: {reason}", flush=True)

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
        "reason": reason,
        "bonzo_status_code": response.status_code,
        "bonzo_response": response.text[:1000],
        "sent_payload": bonzo_payload,
    }
