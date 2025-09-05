import os, io, json, base64, zipfile, logging
from datetime import datetime
import pandas as pd
from flask import Flask, request, render_template, send_file, flash, redirect, url_for

# --- core app ---
from product_parser import parse_products_from_pdf
from matcher import match_products_to_customers
from email_templates import build_email_for_customer

# --- Gmail OAuth (read-only) ---
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# --- optional AI (safe: won’t crash if not configured) ---
OPENAI_ENABLED = False
client = None
try:
    from openai import OpenAI
    _key = os.environ.get("OPENAI_API_KEY")
    if _key:
        client = OpenAI(api_key=_key)
        OPENAI_ENABLED = True
except Exception:
    OPENAI_ENABLED = False
    client = None

from flask import session
from googleapiclient.errors import HttpError

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
    except HttpError:
        return None
    except Exception:
        return None


# Flask
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "devkey")
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024  # 25MB
logging.basicConfig(level=logging.INFO)

ALLOWED_CSV = {"csv"}
ALLOWED_PDF = {"pdf"}

def allowed(filename, exts):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in exts

# ---- Gmail OAuth helpers ----
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.compose"
]


def get_google_flow():
    client_config = {
        "web": {
            "client_id": os.environ["GOOGLE_CLIENT_ID"],
            "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
            "redirect_uris": [os.environ["GOOGLE_REDIRECT_URI"]],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token"
        }
    }
    return Flow.from_client_config(
        client_config=client_config,
        scopes=GMAIL_SCOPES,
        redirect_uri=os.environ["GOOGLE_REDIRECT_URI"]
    )

# ---- AI helpers (safe fallbacks) ----
AI_MODEL = os.environ.get("AI_MODEL", "gpt-4o-mini")

def summarize_history_to_profile(raw_history_text: str) -> str:
    if not OPENAI_ENABLED or not raw_history_text.strip():
        return ("- Tone: friendly, concise\n"
                "- Do: keep emails short, suggest 2–3 items, be helpful\n"
                "- Don't: promise discounts or availability not provided\n"
                "- Interests: infer from category preferences if present\n")
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
    prod_bullets = []
    for p in recommendations:
        name = p.get("name")
        price = p.get("price")
        url = p.get("url")
        line = f"- {name}"
        if isinstance(price, (int, float)):
            line += f" — £{price:.2f}"
        if url:
            line += f" ({url})"
        prod_bullets.append(line)
    prod_text = "\n".join(prod_bullets) if prod_bullets else "- (no items)"

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
        # Fallback
        subject = subject_tpl.format(name=customer.get("name",""))
        body = build_email_for_customer(
            customer=customer, recommendations=recommendations,
            greeting_tpl=greeting_tpl, intro_tpl=intro_tpl,
            footer_tpl=footer_tpl, sender_name=sender_name
        )
        return subject, body

# ---- Utilities ----
REQUIRED_CUSTOMER_COLS = ["email", "name"]
SUGGESTED_PRODUCT_COLS = ["name", "price"]  # others optional

def assert_required_cols(df, required, label):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label} file is missing required column(s): {', '.join(missing)}")

@app.errorhandler(413)
def too_large(_e):
    flash("Upload is too large. Please keep files under 25 MB.", "error")
    return redirect(url_for("index"))

@app.route("/healthz")
def health():
    return {"status": "ok"}, 200

# ---- Main page ----
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template("index.html")

    # templates
    sender_name = request.form.get("sender_name", "Customer Success")
    subject_tpl = request.form.get("subject_tpl", "Your picks from our latest catalogue")
    greeting_tpl = request.form.get("greeting_tpl", "Hi {name},")
    intro_tpl    = request.form.get("intro_tpl", "We picked a few things we think you'll like:")
    footer_tpl   = request.form.get("footer_tpl", "If you have any questions, just hit reply.\n\nBest,\n{sender_name}")
    try:
        max_recs = int(request.form.get("max_recs", "3"))
    except ValueError:
        max_recs = 3

    # files
    prod_pdf = request.files.get("prod_pdf")
    prod_csv = request.files.get("prod_csv")
    cust_csv = request.files.get("cust_csv")
    history  = request.files.get("history_file")  # optional customer email history text/CSV

    if not cust_csv or not allowed(cust_csv.filename, ALLOWED_CSV):
        flash("Please upload a valid Customers CSV.", "error")
        return redirect(url_for("index"))

    try:
        customers_df = pd.read_csv(cust_csv)
        assert_required_cols(customers_df, REQUIRED_CUSTOMER_COLS, "Customers")
    except Exception as e:
        flash(f"Could not read Customers CSV: {e}", "error")
        return redirect(url_for("index"))

    products_df = None
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

    for c in ["name", "price", "category", "sku", "url"]:
        if c not in products_df.columns:
            products_df[c] = None
    products_df["price"] = pd.to_numeric(products_df["price"], errors="coerce")
    products_df = products_df.dropna(subset=["name"]).copy()

    # Build a single profile from optional history file (simple v1)
    history_blob = ""
    if history:
        try:
            history_blob = history.read().decode("utf-8", errors="ignore")
        except Exception:
            history_blob = ""
    profile_text = summarize_history_to_profile(history_blob) if history_blob else ""

    # Match products → recommendations per customer
    matched = match_products_to_customers(products_df, customers_df, max_recs=max_recs)

    # Generate emails
    rows = []
    for _, row in matched.iterrows():
        cust = row["customer"]
        recs = row["recommendations"]
        if profile_text:
            subject, body = generate_personalized_email(
                profile_text, cust, recs,
                subject_tpl, greeting_tpl, intro_tpl, footer_tpl, sender_name
            )
        else:
            subject = subject_tpl.format(name=cust.get("name",""))
            body = build_email_for_customer(
                customer=cust, recommendations=recs,
                greeting_tpl=greeting_tpl, intro_tpl=intro_tpl,
                footer_tpl=footer_tpl, sender_name=sender_name
            )
        rows.append({
            "email": (cust.get("email") or "").strip(),
            "name": (cust.get("name") or "").strip(),
            "subject": subject,
            "body": body
        })

    # Preview page (first 10) + carry data for download
    preview_rows = rows[:10]
    payload = base64.b64encode(json.dumps(rows).encode("utf-8")).decode("utf-8")
    return render_template("preview.html", rows=preview_rows, payload=payload)

# ---- Downloads ----
@app.route("/download", methods=["POST"])
def download_csv():
    try:
        payload = request.form.get("payload", "")
        rows = json.loads(base64.b64decode(payload).decode("utf-8"))
    except Exception:
        flash("Could not retrieve generated data. Please run again.", "error")
        return redirect(url_for("index"))
    out_df = pd.DataFrame(rows)
    csv_bytes = out_df.to_csv(index=False).encode("utf-8")
    return send_file(io.BytesIO(csv_bytes), mimetype="text/csv",
                     as_attachment=True, download_name="mail_merge.csv")

@app.route("/download-eml", methods=["POST"])
def download_eml_zip():
    try:
        payload = request.form.get("payload", "")
        rows = json.loads(base64.b64decode(payload).decode("utf-8"))
    except Exception:
        flash("Could not retrieve generated data. Please run again.", "error")
        return redirect(url_for("index"))

    from email.message import EmailMessage
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for r in rows:
            msg = EmailMessage()
            msg["To"] = r["email"]
            msg["Subject"] = r["subject"]
            msg.set_content(r["body"])
            fname = f"{(r['name'] or 'contact').replace(' ', '_')}.eml"
            z.writestr(fname, msg.as_string())
    buf.seek(0)
    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    return send_file(buf, mimetype="application/zip", as_attachment=True,
                     download_name=f"emails-{stamp}.zip")

# ---- Gmail OAuth routes ----
@app.route("/google/login")
def google_login():
    flow = get_google_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )
    if "SECRET_KEY" not in app.config:
        app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "devkey")
    from flask import session
    session["oauth_state"] = state
    return redirect(auth_url)

@app.route("/oauth2callback")
def oauth2callback():
    from flask import session
    _state = session.get("oauth_state")
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
    flash("Google connected! You can now fetch past emails.", "success")
    return redirect(url_for("index"))

@app.route("/gmail/sample")
def gmail_sample():
    from flask import session
    data = session.get("gmail_creds")
    if not data:
        flash("Please connect Google first.", "error")
        return redirect(url_for("index"))
    creds = Credentials(**data)
    service = build("gmail", "v1", credentials=creds)

    resp = service.users().messages().list(userId="me", maxResults=5, labelIds=["INBOX"]).execute()
    ids = [m["id"] for m in resp.get("messages", [])]

    results = []
    for mid in ids:
        msg = service.users().messages().get(
            userId="me", id=mid, format="metadata", metadataHeaders=["Subject", "From"]
        ).execute()
        headers = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
        results.append({
            "id": mid,
            "from": headers.get("From",""),
            "subject": headers.get("Subject",""),
            "snippet": msg.get("snippet","")
        })
    return {"messages": results}

# ------------------------------------------------------------
# Prevent caching so users always see the latest version
# ------------------------------------------------------------
@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ------------------------------------------------------------
# Run the app (local only; Render uses gunicorn in production)
# ------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

