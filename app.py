import os
import io
import pandas as pd
from flask import Flask, request, render_template, send_file, flash, redirect, url_for
from product_parser import parse_products_from_pdf, parse_products_from_csv
from matcher import match_products_to_customers
from email_templates import build_email_for_customer

REQUIRED_CUSTOMER_COLS = ["email", "name"]
SUGGESTED_PRODUCT_COLS = ["name", "price"]  # category/sku/url optional

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
