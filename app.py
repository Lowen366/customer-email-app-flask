import os, json, base64, logging, io, csv, time
from email.message import EmailMessage
from typing import List, Dict, Any, Optional

import pandas as pd
import requests  # call AI worker
from flask import Flask, request, render_template, flash, redirect, url_for, session, jsonify

# ---- Your modules (existing) ----
from product_parser import parse_products_from_pdf
from matcher import match_products_to_customers
from email_templates import build_email_for_customer

# ---- Google OAuth & Gmail API ----
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# ---- Optional OpenAI (kept as a fallback) ----
OPENAI_ENABLED = False
client = None
AI_MODEL = os.environ.get("AI_MODEL", "gpt-4o-mini")
try:
    from openai import OpenAI
    if os.environ.get("OPENAI_API_KEY"):
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        OPENAI_ENABLED = True
except Exception:
    OPENAI_ENABLED = False
    client = None

# ============================================================
# Flask setup
# ============================================================
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "devkey")
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024  # 25 MB
logging.basicConfig(level=logging.INFO)

# ============================================================
# Worker config
# ============================================================
WORKER_API = os.getenv("AI_EMAIL_API_URL")       # e.g., https://ai-email-worker.onrender.com
USE_WORKER = os.getenv("USE_WORKER", "true").lower() == "true"

# Simple health check / info
@app.get("/healthz")
def health():
    return {"status": "ok", "worker": bool(WORKER_API), "use_worker": USE_WORKER}, 200

# JSON proxy for quick manual tests (optional)
@app.post("/generate-email")
def generate_email():
    """
    Proxy: website → Flask → AI worker → JSON back
    """
    if not WORKER_API:
        return {"error": "AI_EMAIL_API_URL not set"}, 500

    data = request.get_json(force=True)
    try:
        resp = requests.post(
            f"{WORKER_API}/write-email",
            json=data,
            timeout=30,
            headers={"Content-Type": "application/json"},
        )
        return resp.json(), resp.status_code
    except Exception as e:
        return {"error": f"Failed to reach AI worker: {e}"}, 502

# ============================================================
# File types / schema
# ============================================================
ALLOWED_CSV   = {"csv"}
ALLOWED_PDF   = {"pdf"}
ALLOWED_EXCEL = {"xlsx", "xls"}

REQUIRED_CUSTOMER_COLS = ["email", "name"]
SUGGESTED_PRODUCT_COLS = ["name", "price"]  # other cols optional

def allowed(filename: str, exts: set[str]) -> bool:
    return "." in (filename or "") and filename.rsplit(".", 1)[1].lower() in exts

def assert_required_cols(df: pd.DataFrame, required: List[str], label: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label} file is missing required column(s): {', '.join(missing)}")

def read_table_upload(file_storage) -> pd.DataFrame:
    """Return a pandas DataFrame from an uploaded CSV/XLSX/XLS file."""
    fname = (file_storage.filename or "").lower()
    ext = fname.rsplit(".", 1)[-1]
    if ext in ALLOWED_CSV:
        return pd.read_csv(file_storage)
    if ext in ALLOWED_EXCEL:
        # prefer openpyxl for xlsx
        if ext == "xlsx":
            return pd.read_excel(file_storage, engine="openpyxl")
        return pd.read_excel(file_storage)
    raise ValueError("Unsupported file type. Please upload .csv, .xlsx, or .xls")

# ============================================================
# Gmail OAuth (includes SEND)
# ============================================================
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.compose",
]

def get_google_flow():
    client_config = {
        "web": {
            "client_id": os.environ["GOOGLE_CLIENT_ID"],
            "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
            "redirect_uris": [os.environ["GOOGLE_REDIRECT_URI"]],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }
    return Flow.from_client_config(
        client_config=client_config,
        scopes=GMAIL_SCOPES,
        redirect_uri=os.environ["GOOGLE_REDIRECT_URI"],
    )

def gmail_is_connected() -> bool:
    return bool(session.get("gmail_creds"))

def gmail_connected_email() -> Optional[str]:
    data = session.get("gmail_creds")
    if not data:
        return None
    try:
        creds = Credentials(**data)
        service = build("gmail", "v1", credentials=creds)
        prof = service.users().getProfile(userId="me").execute()
        return prof.get("emailAddress")
    except Exception:
        return None

def get_gmail_service():
    data = session.get("gmail_creds")
    if not data:
        return None
    creds = Credentials(**data)
    return build("gmail", "v1", credentials=creds)

def gmail_send(to_addr: str, subject: str, body: str) -> dict:
    service = get_gmail_service()
    if not service:
        raise RuntimeError("Gmail not connected")
    msg = EmailMessage()
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    return service.users().messages().send(userId="me", body={"raw": raw}).execute()

# ============================================================
# AI helpers (fallbacks + worker integration)
# ============================================================
DEFAULT_SUBJECT = "Your picks from our latest catalogue"
DEFAULT_GREETING = "Hi {name},"
DEFAULT_INTRO = "We picked a few things we think you'll like:"
DEFAULT_FOOTER = "If you have any questions, just hit reply.\n\nBest,\n{sender_name}"

def summarize_history_to_profile(raw_history_text: str) -> str:
    """Optional: condense past emails into a brief tone/profile hint."""
    if not raw_history_text.strip():
        return ""
    if not OPENAI_ENABLED:
        # lightweight default hint
        return ("- Tone: friendly, concise\n"
                "- Do: suggest 2–3 items\n"
                "- Don't: overpromise or invent discounts\n")
    try:
        r = client.responses.create(
            model=AI_MODEL,
            input=f"Summarise this email history into a short profile (tone, do/don't, style):\n\n{raw_history_text}"
        )
        return (getattr(r, "output_text", "") or "").strip()[:1200]
    except Exception:
        return "- Tone: friendly, concise; Do: be specific; Don't: overpromise."

def build_product_summary(products_df: Optional[pd.DataFrame], preferred: Optional[str], max_recs: int) -> Optional[str]:
    """Create a compact string like 'Lamp £39.95; Timer £9.99; Cable £6.50' for the worker prompt."""
    if products_df is None or products_df.empty:
        return None
    df = products_df
    if preferred and "category" in df.columns:
        try:
            sub = df[df["category"].astype(str).str.contains(preferred, case=False, na=False)]
            if not sub.empty:
                df = sub
        except Exception:
            pass
    sample = df.head(max_recs)
    parts = []
    for _, r in sample.iterrows():
        name = str(r.get("name") or "item").strip()
        price = r.get("price")
        try:
            price_str = f"£{float(price):.2f}" if pd.notna(price) else ""
        except Exception:
            price_str = ""
        parts.append(f"{name}{(' ' + price_str) if price_str else ''}")
    return "; ".join(parts) if parts else None

def call_worker(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Robust call with tiny retry/backoff. Returns dict with keys like subject/body_* OR {'error':...}"""
    if not (USE_WORKER and WORKER_API):
        return {"error": "Worker disabled or URL missing"}
    last_err = "unknown"
    for attempt in range(3):
        try:
            r = requests.post(
                f"{WORKER_API}/write-email",
                json=payload,
                timeout=30,
                headers={"Content-Type": "application/json"},
            )
            if r.status_code == 200:
                return r.json()
            if 500 <= r.status_code < 600:
                time.sleep(0.6 * (attempt + 1))
                continue
            return {"error": f"Worker {r.status_code}: {r.text[:300]}"}
        except Exception as e:
            last_err = str(e)
            time.sleep(0.6 * (attempt + 1))
    return {"error": f"Worker unreachable: {last_err}"}

def generate_personalized_email_fallback(profile: str, customer: dict, recommendations: list,
                                         subject_tpl: str, greeting_tpl: str, intro_tpl: str,
                                         footer_tpl: str, sender_name: str) -> tuple[str, str]:
    """Your existing fallback (template or OpenAI)."""
    if not OPENAI_ENABLED:
        subject = subject_tpl.format(name=customer.get("name",""))
        body = build_email_for_customer(
            customer=customer, recommendations=recommendations,
            greeting_tpl=greeting_tpl, intro_tpl=intro_tpl,
            footer_tpl=footer_tpl, sender_name=sender_name
        )
        return subject, body

    # Minimal OpenAI fallback
    bullets = []
    for p in recommendations:
        name = p.get("name")
        price = p.get("price")
        url = p.get("url")
        line = f"- {name}"
        if isinstance(price, (int, float)):
            line += f" — £{price:.2f}"
        if url:
            line += f" ({url})"
        bullets.append(line)
    prod_text = "\n".join(bullets) if bullets else "- (no items)"
    prompt = f"""
CUSTOMER: {customer}
PROFILE HINT: {profile}
PRODUCTS:
{prod_text}

Return:
SUBJECT: <60 chars
BODY:
<email body with greeting, 2–3 bullets, closing>
"""
    try:
        r = client.responses.create(model=AI_MODEL, input=prompt)
        txt = getattr(r, "output_text", "") or ""
        subject = subject_tpl
        body = txt
        if "BODY:" in txt:
            head, body = txt.split("BODY:", 1)
            head = head.replace("SUBJECT:", "").strip()
            if head:
                subject = head
        subject = subject.replace("{name}", customer.get("name",""))
        body = body.replace("{name}", customer.get("name","")).replace("{sender_name}", sender_name)
        return subject, body
    except Exception:
        subject = subject_tpl.format(name=customer.get("name",""))
        body = build_email_for_customer(
            customer=customer, recommendations=recommendations,
            greeting_tpl=greeting_tpl, intro_tpl=intro_tpl,
            footer_tpl=footer_tpl, sender_name=sender_name
        )
        return subject, body

# ============================================================
# Errors / no-cache
# ============================================================
@app.errorhandler(413)
def too_large(_e):
    flash("Upload is too large. Please keep files under 25 MB.", "error")
    return redirect(url_for("index"))

@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# ============================================================
# Main page (GET shows form; POST handles uploads)
# ============================================================
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template(
            "index.html",
            gmail_connected=gmail_is_connected(),
            gmail_email=gmail_connected_email(),
        )

    # ---------- POST: handle upload ----------
    sender_name = request.form.get("sender_name", "Customer Success")
    try:
        max_recs = int(request.form.get("max_recs", "3"))
    except ValueError:
        max_recs = 3

    prod_pdf = request.files.get("prod_pdf")    # optional
    prod_csv = request.files.get("prod_csv")    # CSV/XLSX/XLS
    cust_csv = request.files.get("cust_csv")    # CSV/XLSX/XLS (required)
    history  = request.files.get("history_file")

    # Customers
    if not cust_csv:
        flash("Please upload a Customers CSV/Excel file.", "error")
        return redirect(url_for("index"))
    try:
        customers_df = read_table_upload(cust_csv)
        customers_df.rename(columns=lambda c: c.strip().lower(), inplace=True)
        assert_required_cols(customers_df, REQUIRED_CUSTOMER_COLS, "Customers")
    except Exception as e:
        flash(f"Could not read Customers file: {e}", "error")
        return redirect(url_for("index"))

    # Products (CSV/Excel or PDF)
    try:
        if prod_csv and allowed(prod_csv.filename, ALLOWED_CSV | ALLOWED_EXCEL):
            products_df = read_table_upload(prod_csv)
        elif prod_pdf and allowed(prod_pdf.filename, ALLOWED_PDF):
            products_df, _ = parse_products_from_pdf(prod_pdf)
        else:
            flash("Please upload either a Products CSV/Excel or a Product PDF.", "error")
            return redirect(url_for("index"))
        products_df.rename(columns=lambda c: c.strip().lower(), inplace=True)
        # ensure columns exist
        for c in ["name", "price", "category", "sku", "url"]:
            if c not in products_df.columns:
                products_df[c] = None
        products_df["price"] = pd.to_numeric(products_df["price"], errors="coerce")
        products_df = products_df.dropna(subset=["name"]).copy()
        assert_required_cols(products_df, SUGGESTED_PRODUCT_COLS, "Products")
    except Exception as e:
        flash(f"Problem reading products file: {e}", "error")
        return redirect(url_for("index"))

    # Optional history → profile hint
    history_blob = ""
    if history:
        try:
            history_blob = history.read().decode("utf-8", errors="ignore")
        except Exception:
            history_blob = ""
    profile_text = summarize_history_to_profile(history_blob) if history_blob else ""

    # Match products to customers (you already have this logic)
    matched = match_products_to_customers(products_df, customers_df, max_recs=max_recs)

        # --- NEW: build a per-customer summary from recs ---
        def price_str(v):
            try:
                return f"£{float(v):.2f}"
            except Exception:
                return ""

        rec_summary = "; ".join(
            f"{(r.get('name') or 'item').strip()} {price_str(r.get('price'))}".strip()
            + (f" ({r.get('url')})" if r.get('url') else "")
            for r in (recs or [])
        ) or "(no items)"

        # Payload for worker (now includes *recommendations*)
        payload = {
            "goal": "win-back",
            "offer": "Free shipping this week",
            "cta_url": "https://yourwebsite.com/shop",
            "constraints": {
                "sender_name": sender_name,
                "tone_hint": (profile_text[:800] if profile_text else None),
                "product_summary": rec_summary  # human-readable
            },
            "customer": {
                "id": f"cust_{email or name}",
                "name": name,
                "email": email,
                "locale": "en-GB",
                "segment": [preferred] if preferred else []
            },
            "recommendations": recs  # <-- critical: pass structured items
        }

        ai = call_worker(payload)

        if "error" not in ai:
            # Prefer HTML/text from worker, but keep your existing review pipeline (plain text body)
            subject = ai.get("subject", DEFAULT_SUBJECT).replace("{name}", name)
            body_text = ai.get("body_text") or ""
            body_html = ai.get("body_html") or ""
            # Use body_text for Gmail send (safe) but stash HTML for the review template later if you wish
            rows.append({
                "email": email,
                "name":  name,
                "subject": subject,
                "body": body_text or body_html or "",     # keep existing key
                "body_html": body_html,                   # extra (non-breaking)
                "preheader": ai.get("preheader", ""),
                "cta_text": ai.get("cta_text", ""),
                "cta_url": ai.get("cta_url", ""),
                "notes": ai.get("notes", "")
            })
            continue

        # Worker failed -> fallback to your template/OpenAI path
        subject, body = generate_personalized_email_fallback(
            profile_text, cust, recs,
            DEFAULT_SUBJECT, DEFAULT_GREETING, DEFAULT_INTRO, DEFAULT_FOOTER, sender_name
        )
        rows.append({
            "email": email,
            "name":  name,
            "subject": subject,
            "body": body,
            "notes": f"[fallback] {ai.get('error','')}"
        })

    # Save for review
    session["review_rows"] = rows
    session["review_status"] = ["pending"] * len(rows)
    return redirect(url_for("review"))

# ============================================================
# Review & Send workflow
# ============================================================
@app.route("/review")
def review():
    rows = session.get("review_rows", [])
    status = session.get("review_status", [])
    return render_template(
        "review.html",
        rows=rows, status=status,
        gmail_connected=gmail_is_connected(),
        gmail_email=gmail_connected_email()
    )

@app.post("/review/approve")
def review_approve():
    i = int(request.form["index"])
    status = session.get("review_status", [])
    if 0 <= i < len(status):
        status[i] = "approved"
        session["review_status"] = status
    return redirect(url_for("review"))

@app.post("/review/unapprove")
def review_unapprove():
    i = int(request.form["index"])
    status = session.get("review_status", [])
    if 0 <= i < len(status):
        status[i] = "pending"
        session["review_status"] = status
    return redirect(url_for("review"))

@app.post("/review/skip_one")
def review_skip_one():
    i = int(request.form["index"])
    status = session.get("review_status", [])
    if 0 <= i < len(status):
        status[i] = "skipped"
        session["review_status"] = status
    return redirect(url_for("review"))

@app.post("/review/test_one")
def review_test_one():
    i = int(request.form["index"])
    rows = session.get("review_rows", [])
    if not (0 <= i < len(rows)):
        flash("Invalid item.", "error")
        return redirect(url_for("review"))
    me = gmail_connected_email()
    if not me:
        flash("Connect Google first.", "error")
        return redirect(url_for("review"))
    subject = request.form.get("subject", rows[i].get("subject","")) + " [TEST]"
    body    = request.form.get("body", rows[i].get("body",""))
    try:
        gmail_send(me, subject, body)
        flash(f"Test sent to {me}", "success")
    except Exception as e:
        flash(f"Test send failed: {e}", "error")
    return redirect(url_for("review"))

@app.post("/review/send_one")
def review_send_one():
    i = int(request.form["index"])
    rows = session.get("review_rows", [])
    status = session.get("review_status", [])
    if not (0 <= i < len(rows)):
        flash("Invalid item.", "error")
        return redirect(url_for("review"))

    # persist edits
    subject = request.form.get("subject", rows[i].get("subject",""))
    body    = request.form.get("body", rows[i].get("body",""))
    rows[i]["subject"] = subject
    rows[i]["body"] = body
    session["review_rows"] = rows

    try:
        gmail_send(rows[i]["email"], subject, body)
        status[i] = "sent"
        session["review_status"] = status
        flash(f"Sent to {rows[i]['email']}", "success")
    except Exception as e:
        flash(f"Send failed: {e}", "error")
    return redirect(url_for("review"))

@app.post("/review/send_all")
def review_send_all():
    rows = session.get("review_rows", [])
    status = session.get("review_status", [])
    sent = 0
    for i, r in enumerate(rows):
        if status[i] != "approved":
            continue
        try:
            gmail_send(r["email"], r["subject"], r["body"])
            status[i] = "sent"
            sent += 1
        except Exception as e:
            status[i] = f"error: {e}"
    session["review_status"] = status
    flash(f"Sent {sent} approved emails.", "success")
    return redirect(url_for("review"))

# ============================================================
# Google OAuth routes
# ============================================================
@app.get("/google/login")
def google_login():
    flow = get_google_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline", include_granted_scopes="true", prompt="consent"
    )
    session["oauth_state"] = state
    return redirect(auth_url)

@app.get("/oauth2callback")
def oauth2callback():
    flow = get_google_flow()
    flow.fetch_token(authorization_response=request.url)
    creds: Credentials = flow.credentials
    session["gmail_creds"] = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }
    flash("Google connected! You can now review & send.", "success")
    return redirect(url_for("index"))

@app.get("/google/logout")
def google_logout():
    session.pop("gmail_creds", None)
    flash("Google disconnected.", "success")
    return redirect(url_for("index"))

# Optional debug (remove later)
@app.get("/oauth-debug")
def oauth_debug():
    return {
        "GOOGLE_CLIENT_ID": os.environ.get("GOOGLE_CLIENT_ID", ""),
        "GOOGLE_REDIRECT_URI": os.environ.get("GOOGLE_REDIRECT_URI", "")
    }

@app.get("/oauth-authurl")
def oauth_authurl():
    flow = get_google_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline", include_granted_scopes="true", prompt="consent"
    )
    return {"auth_url": auth_url, "state": state}

# ============================================================
# Local runner (Render uses gunicorn)
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=True)
