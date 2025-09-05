import os
import io
import pandas as pd
from flask import Flask, request, render_template, send_file, flash, redirect, url_for
from product_parser import parse_products_from_pdf, parse_products_from_csv
from matcher import match_products_to_customers
from email_templates import build_email_for_customer

from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json, secrets

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

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


import os
from openai import OpenAI
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

AI_MODEL = os.environ.get("AI_MODEL", "gpt-4o-mini")  # fast/cost-effective



def summarize_history_to_profile(raw_history_text: str) -> str:
    """
    Returns a compact profile: tone, do/don't, interests, price comfort, phrases, opt-outs.
    """
    prompt = f"""
You are an email personalization assistant. Read the raw email history below and produce a short profile.

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
    r = client.responses.create(model=AI_MODEL, input=prompt)
    return r.output_text.strip()



REQUIRED_CUSTOMER_COLS = ["email", "name"]
SUGGESTED_PRODUCT_COLS = ["name", "price"]  # category/sku/url optional
def generate_personalized_email(profile: str, customer: dict, recommendations: list,
                                subject_tpl: str, greeting_tpl: str, intro_tpl: str, footer_tpl: str, sender_name: str) -> tuple[str, str]:
    # Build a concise product list for the model
    prod_bullets = []
    for p in recommendations:
        name = p.get("name")
        price = p.get("price")
        url = p.get("url")
        piece = f"- {name}"
        if isinstance(price, (int, float)):
            piece += f" — £{price:.2f}"
        if url:
            piece += f" ({url})"
        prod_bullets.append(piece)
    prod_text = "\n".join(prod_bullets) if prod_bullets else "- (no items)"

    sys_rules = """You write short, clear marketing emails that feel human and helpful.
Follow the provided customer profile strictly; do not invent facts or make promises not given.
Never imply discounts or availability beyond the product list. Keep to UK spelling if ambiguous.
Include a natural CTA. Keep to 90–140 words unless profile suggests otherwise."""

    user_prompt = f"""
CUSTOMER:
- name: {customer.get('name','')}
- email: {customer.get('email','')}

PROFILE:
{profile}

PRODUCT PICKS:
{prod_text}

TEMPLATES (use as guidance for structure, but adapt tone to profile):
- Greeting: "{greeting_tpl}"
- Intro: "{intro_tpl}"
- Footer: "{footer_tpl}" (sender_name = "{sender_name}")

Write:
1) A subject line (<= 60 chars), tailored to the profile.
2) The full email body with greeting, short intro, 2–3 bullets for products, friendly close.

Return in this format:

SUBJECT: <subject line>
BODY:
<final body text>
"""
    r = client.responses.create(model=AI_MODEL, input=[{"role":"system","content":sys_rules},
                                                       {"role":"user","content":user_prompt}])
    txt = r.output_text
    subj = "Your picks from our latest catalogue"
    body = txt
    # Lightweight parse
    if "BODY:" in txt:
        parts = txt.split("BODY:", 1)
        subj_line = parts[0].replace("SUBJECT:", "").strip()
        body = parts[1].strip()
        if subj_line:
            subj = subj_line
    # Personalize greeting/footer placeholders
    subj = subj.replace("{name}", customer.get("name",""))
    body = body.replace("{name}", customer.get("name","")).replace("{sender_name}", sender_name)
    return subj, body


def assert_required_cols(df, required, label):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label} file is missing required column(s): {', '.join(missing)}")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "devkey")

# Allow larger uploads (25 MB)
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024

ALLOWED_CSV = {"csv"}
ALLOWED_PDF = {"pdf"}

def allowed(filename, exts):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in exts

# Friendly message if files are too large
@app.errorhandler(413)
def too_large(_e):
    flash("Upload is too large. Please keep files under 25 MB.", "error")
    return redirect(url_for("index"))

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template("index.html")

    # Email template settings (with defaults)
    sender_name = request.form.get("sender_name", "Customer Success")
    subject_tpl = request.form.get("subject_tpl", "Your picks from our latest catalogue")
    greeting_tpl = request.form.get("greeting_tpl", "Hi {name},")
    intro_tpl = request.form.get("intro_tpl", "We picked a few things we think you'll like:")
    footer_tpl = request.form.get("footer_tpl", "If you have any questions, just hit reply.\n\nBest,\n{sender_name}")
    try:
        max_recs = int(request.form.get("max_recs", "3"))
    except ValueError:
        max_recs = 3

    # Files
    prod_pdf = request.files.get("prod_pdf")
    prod_csv = request.files.get("prod_csv")
    cust_csv = request.files.get("cust_csv")

    # Validate customers CSV
    if not cust_csv or not allowed(cust_csv.filename, ALLOWED_CSV):
        flash("Please upload a valid Customers CSV.", "error")
        return redirect(url_for("index"))

    try:
        customers_df = pd.read_csv(cust_csv)
    except Exception as e:
        flash(f"Could not read Customers CSV: {e}", "error")
        return redirect(url_for("index"))

    # Load products (prefer CSV over PDF)
    products_df = None
    try:
        if prod_csv and allowed(prod_csv.filename, ALLOWED_CSV):
            products_df = pd.read_csv(prod_csv)
        elif prod_pdf and allowed(prod_pdf.filename, ALLOWED_PDF):
            products_df, _logs = parse_products_from_pdf(prod_pdf)
        else:
            flash("Please upload either a Products CSV or a Product PDF.", "error")
            return redirect(url_for("index"))
    except Exception as e:
        flash(f"Problem reading products file: {e}", "error")
        return redirect(url_for("index"))

    # Normalize product columns
    for c in ["name", "price", "category", "sku", "url"]:
        if c not in products_df.columns:
            products_df[c] = None

    # Clean types
    products_df["price"] = pd.to_numeric(products_df["price"], errors="coerce")
    products_df = products_df.dropna(subset=["name"]).copy()

    # Match products → recommendations per customer
    matched = match_products_to_customers(products_df, customers_df, max_recs=max_recs)

    # Build personalized emails
    rows = []
    for _, row in matched.iterrows():
        cust = row["customer"]
        recs = row["recommendations"]
        email = (cust.get("email") or "").strip()
        name = (cust.get("name") or "").strip()
        subject = subject_tpl.format(name=name)
        body = build_email_for_customer(
            customer=cust,
            recommendations=recs,
            greeting_tpl=greeting_tpl,
            intro_tpl=intro_tpl,
            footer_tpl=footer_tpl,
            sender_name=sender_name
        )
        rows.append({"email": email, "name": name, "subject": subject, "body": body})

    # Return mail_merge.csv as a download
    out_df = pd.DataFrame(rows)
    csv_bytes = out_df.to_csv(index=False).encode("utf-8")
    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name="mail_merge.csv",
    )
   
