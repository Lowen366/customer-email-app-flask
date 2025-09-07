import os, io, json, base64, logging
from datetime import datetime
from email.message import EmailMessage

import pandas as pd
from flask import (
    Flask, request, render_template, send_file,
    flash, redirect, url_for, session
)

# ---- Your app modules ----
from product_parser import parse_products_from_pdf
from matcher import match_products_to_customers
from email_templates import build_email_for_customer

# ---- Google OAuth & Gmail API ----
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---- Optional AI (safe: won’t crash if not configured) ----
OPENAI_ENABLED = False
client = None
AI_MODEL = os.environ.get("AI_MODEL", "gpt-4o-mini")
try:
    from openai import OpenAI
    _key = os.environ.get("OPENAI_API_KEY")
    if _key:
        client = OpenAI(api_key=_key)
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

ALLOWED_CSV   = {"csv"}
ALLOWED_PDF   = {"pdf"}
ALLOWED_EXCEL = {"xlsx", "xls"}
  
def read_table_upload(file_storage):
    """Return a pandas DataFrame from an uploaded CSV/XLSX/XLS file."""
    fname = file_storage.filename or ""
    ext = fname.rsplit(".", 1)[-1].lower()
    if ext in ALLOWED_CSV:
        return pd.read_csv(file_storage)
    if ext in ALLOWED_EXCEL:
        if ext == "xlsx":
            return pd.read_excel(file_storage, engine="openpyxl")  # first sheet
        else:  # xls
            return pd.read_excel(file_storage, engine="xlrd")
    raise ValueError("Unsupported file type. Please upload .csv, .xlsx, or .xls")


def allowed(filename, exts):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in exts
    



REQUIRED_CUSTOMER_COLS = ["email", "name"]
SUGGESTED_PRODUCT_COLS = ["name", "price"]  # others optional

def assert_required_cols(df, required, label):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label} file is missing required column(s): {', '.join(missing)}")

# ============================================================
# Gmail OAuth (now includes SEND permission)
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

def gmail_connected_email() -> str | None:
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
# AI helpers (safe fallbacks)
# ============================================================
DEFAULT_SUBJECT = "Your picks from our latest catalogue"
DEFAULT_GREETING = "Hi {name},"
DEFAULT_INTRO = "We picked a few things we think you'll like:"
DEFAULT_FOOTER = "If you have any questions, just hit reply.\n\nBest,\n{sender_name}"

def summarize_history_to_profile(raw_history_text: str) -> str:
    if not OPENAI_ENABLED or not raw_history_text.strip():
        return ("- Tone: friendly, concise\n"
                "- Do: keep emails short, suggest 2–3 items, be helpful\n"
                "- Don't: promise discounts or availability not provided\n"
                "- Interests: infer from preferences if present\n")
    prompt = f"""
You are an email personalization assistant. Read the raw email history below and produce a compact profile.

Return sections with bullets:
- Tone
- Do
- Don't
- Interests / categories
- Price comfort (if evident)
- Phrases they use
- Risk flags (complaints, returns, sensitive topics)
- Compliance/opt-out notes (if any)

Only infer what’s clearly supported; do not fabricate.

--- HISTORY START ---
{raw_history_text}
--- HISTORY END ---
"""
    try:
        r = client.responses.create(model=AI_MODEL, input=prompt)
        return (r.output_text or "").strip() or "Tone: friendly; Do: be concise; Don't: overpromise."
    except Exception as e:
        app.logger.warning(f"AI summarize failed: {e}")
        return "Tone: friendly; Do: be concise; Don't: overpromise."

def generate_personalized_email(profile: str, customer: dict, recommendations: list,
                                subject_tpl: str, greeting_tpl: str, intro_tpl: str,
                                footer_tpl: str, sender_name: str) -> tuple[str, str]:
    if not OPENAI_ENABLED:
        subject = subject_tpl.format(name=customer.get("name",""))
        body = build_email_for_customer(
            customer=customer, recommendations=recommendations,
            greeting_tpl=greeting_tpl, intro_tpl=intro_tpl,
            footer_tpl=footer_tpl, sender_name=sender_name
        )
        return subject, body

    # Prepare product bullets
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

    sys_rules = """You write short, clear marketing emails that feel human and helpful.
Follow the provided customer profile strictly; do not invent facts or make promises not given.
Never imply discounts or availability beyond the product list. Use UK spelling if ambiguous.
Include a natural CTA and keep 90–140 words unless profile suggests otherwise."""

    user_prompt = f"""
CUSTOMER:
- name: {customer.get('name','')}
- email: {customer.get('email','')}

PROFILE:
{profile}

PRODUCT PICKS:
{prod_text}

TEMPLATES (structure guide; adapt tone):
- Greeting: "{greeting_tpl}"
- Intro: "{intro_tpl}"
- Footer: "{footer_tpl}" (sender_name = "{sender_name}")

Write:
1) SUBJECT (<= 60 chars), tailored to the profile.
2) BODY with greeting, short intro, 2–3 bullets for products, friendly close.

Return in this format:

SUBJECT: <subject line>
BODY:
<final body text>
"""
    try:
        r = client.responses.create(
            model=AI_MODEL,
            input=[{"role": "system", "content": sys_rules},
                   {"role": "user", "content": user_prompt}]
        )
        txt = r.output_text or ""
        subject = subject_tpl
        body = txt
        if "BODY:" in txt:
            parts = txt.split("BODY:", 1)
            subject_line = parts[0].replace("SUBJECT:", "").strip()
            body = parts[1].strip()
            if subject_line:
                subject = subject_line
        subject = subject.replace("{name}", customer.get("name",""))
        body = body.replace("{name}", customer.get("name","")).replace("{sender_name}", sender_name)
        return subject, body
    except Exception as e:
        app.logger.warning(f"AI generate failed: {e}")
        subject = subject_tpl.format(name=customer.get("name",""))
        body = build_email_for_customer(
            customer=customer, recommendations=recommendations,
            greeting_tpl=greeting_tpl, intro_tpl=intro_tpl,
            footer_tpl=footer_tpl, sender_name=sender_name
        )
        return subject, body

# ============================================================
# Errors, Health, No-cache
# ============================================================
@app.errorhandler(413)
def too_large(_e):
    flash("Upload is too large. Please keep files under 25 MB.", "error")
    return redirect(url_for("index"))

@app.route("/healthz")
def health():
    return {"status": "ok"}, 200

@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# ============================================================
# Main page: simplified UI (AI writes copy; no template fields)
# ============================================================
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template(
            "index.html",
            gmail_connected=gmail_is_connected(),
            gmail_email=gmail_connected_email(),
        )

    # ---- Minimal inputs (AI handles subject/body) ----
    sender_name = request.form.get("sender_name", "Customer Success")
    try:
        max_recs = int(request.form.get("max_recs", "3"))
    except ValueError:
        max_recs = 3

    # ---- Files ----
    prod_pdf = request.files.get("prod_pdf")
    prod_csv = request.files.get("prod_csv")
    cust_csv = request.files.get("cust_csv")
    history  = request.files.get("history_file")  # optional

    if not cust_csv or not allowed(cust_csv.filename, ALLOWED_CSV):
        flash("Please upload a valid Customers CSV.", "error")
        return redirect(url_for("index"))

    # Customers
    try:
        customers_df = pd.read_csv(cust_csv)
        assert_required_cols(customers_df, REQUIRED_CUSTOMER_COLS, "Customers")
    except Exception as e:
        flash(f"Could not read Customers CSV: {e}", "error")
        return redirect(url_for("index"))

    # Products
    try:
        if prod_csv and allowed(prod_csv.filename, ALLOWED_CSV):
            products_df = pd.read_csv(prod_csv)
        elif prod_pdf and allowed(prod_pdf.filename, ALLOWED_PDF):
            products_df, _ = parse_products_from_pdf(prod_pdf)
        else:
            flash("Please upload either a Products CSV or a Product PDF.", "error")
            return redirect(url_for("index"))
        assert_required_cols(products_df, SUGGESTED_PRODUCT_COLS, "Products")
    except Exception as e:
        flash(f"Problem reading products file: {e}", "error")
        return redirect(url_for("index"))

    # Normalize product columns
    for c in ["name", "price", "category", "sku", "url"]:
        if c not in products_df.columns:
            products_df[c] = None
    products_df["price"] = pd.to_numeric(products_df["price"], errors="coerce")
    products_df = products_df.dropna(subset=["name"]).copy()

    # Optional: build ONE profile from uploaded history text (v1)
    history_blob = ""
    if history:
        try:
            history_blob = history.read().decode("utf-8", errors="ignore")
        except Exception:
            history_blob = ""
    profile_text = summarize_history_to_profile(history_blob) if history_blob else ""

    # Match products → recommendations per customer
    matched = match_products_to_customers(products_df, customers_df, max_recs=max_recs)

    # Build email drafts
    rows = []
    for _, row in matched.iterrows():
        cust = row["customer"]
        recs = row["recommendations"]
        if profile_text:
            subject, body = generate_personalized_email(
                profile_text, cust, recs,
                DEFAULT_SUBJECT, DEFAULT_GREETING, DEFAULT_INTRO, DEFAULT_FOOTER, sender_name
            )
        else:
            subject = DEFAULT_SUBJECT.format(name=cust.get("name",""))
            body = build_email_for_customer(
                customer=cust, recommendations=recs,
                greeting_tpl=DEFAULT_GREETING,
                intro_tpl=DEFAULT_INTRO,
                footer_tpl=DEFAULT_FOOTER,
                sender_name=sender_name
            )
        rows.append({
            "email": (cust.get("email") or "").strip(),
            "name": (cust.get("name") or "").strip(),
            "subject": subject,
            "body": body
        })

    # Save drafts in session → go to Review page
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

@app.route("/review/approve", methods=["POST"])
def review_approve():
    i = int(request.form["index"])
    status = session.get("review_status", [])
    if 0 <= i < len(status):
        status[i] = "approved"
        session["review_status"] = status
    return redirect(url_for("review"))

@app.route("/review/unapprove", methods=["POST"])
def review_unapprove():
    i = int(request.form["index"])
    status = session.get("review_status", [])
    if 0 <= i < len(status):
        status[i] = "pending"
        session["review_status"] = status
    return redirect(url_for("review"))

@app.route("/review/skip_one", methods=["POST"])
def review_skip_one():
    i = int(request.form["index"])
    status = session.get("review_status", [])
    if 0 <= i < len(status):
        status[i] = "skipped"
        session["review_status"] = status
    return redirect(url_for("review"))

@app.route("/review/test_one", methods=["POST"])
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
    subject = request.form.get("subject", rows[i]["subject"]) + " [TEST]"
    body    = request.form.get("body", rows[i]["body"])
    try:
        gmail_send(me, subject, body)
        flash(f"Test sent to {me}", "success")
    except Exception as e:
        flash(f"Test send failed: {e}", "error")
    return redirect(url_for("review"))

@app.route("/review/send_one", methods=["POST"])
def review_send_one():
    i = int(request.form["index"])
    rows = session.get("review_rows", [])
    status = session.get("review_status", [])
    if not (0 <= i < len(rows)):
        flash("Invalid item.", "error")
        return redirect(url_for("review"))

    # capture edits
    subject = request.form.get("subject", rows[i]["subject"])
    body    = request.form.get("body", rows[i]["body"])
    to_addr = rows[i]["email"]

    # persist edits in session
    rows[i]["subject"] = subject
    rows[i]["body"] = body
    session["review_rows"] = rows

    try:
        gmail_send(to_addr, subject, body)
        status[i] = "sent"
        session["review_status"] = status
        flash(f"Sent to {to_addr}", "success")
    except Exception as e:
        flash(f"Send failed: {e}", "error")
    return redirect(url_for("review"))

@app.route("/review/send_all", methods=["POST"])
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
# Google OAuth routes (+ sample fetch)
# ============================================================
# Shows what values your app will use
@app.route("/oauth-debug")
def oauth_debug():
    return {
        "GOOGLE_CLIENT_ID": os.environ.get("GOOGLE_CLIENT_ID", ""),
        "GOOGLE_REDIRECT_URI": os.environ.get("GOOGLE_REDIRECT_URI", "")
    }

# Shows the exact Google auth URL your app generates (so we can inspect redirect_uri param)
@app.route("/oauth-authurl")
def oauth_authurl():
    flow = get_google_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )
    return {"auth_url": auth_url, "state": state}



@app.route("/google/login")
def google_login():
    flow = get_google_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline", include_granted_scopes="true", prompt="consent"
    )
    session["oauth_state"] = state
    return redirect(auth_url)

@app.route("/oauth2callback")
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

@app.route("/google/logout")
def google_logout():
    session.pop("gmail_creds", None)
    flash("Google disconnected.", "success")
    return redirect(url_for("index"))

@app.route("/gmail/sample")
def gmail_sample():
    data = session.get("gmail_creds")
    if not data:
        flash("Please connect Google first.", "error")
        return redirect(url_for("index"))
    try:
        creds = Credentials(**data)
        service = build("gmail", "v1", credentials=creds)
        resp = service.users().messages().list(userId="me", maxResults=5, labelIds=["INBOX"]).execute()
        ids = [m["id"] for m in resp.get("messages", [])]
        results = []
        for mid in ids:
            msg = service.users().messages().get(
                userId="me", id=mid, format="metadata", metadataHeaders=["Subject","From"]
            ).execute()
            headers = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
            results.append({
                "id": mid,
                "from": headers.get("From",""),
                "subject": headers.get("Subject",""),
                "snippet": msg.get("snippet","")
            })
        return {"messages": results}
    except Exception as e:
        return {"error": str(e)}, 500

# ============================================================
# Local dev runner (Render uses gunicorn)
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=True)
