import pdfplumber
import pandas as pd
import re

def _guess_price(text):
    if text is None:
        return None
    m = re.search(r'(?:£|\$|€)?\s?(\d{1,4}(?:[\.,]\d{2})?)', text)
    if m:
        try:
            return float(m.group(1).replace(',', '.'))
        except:
            return None
    return None

def parse_products_from_pdf(file_like):
    logs, rows = [], []
    with pdfplumber.open(file_like) as pdf:
        logs.append(f"Pages detected: {len(pdf.pages)}")
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            logs.append(f"-- Page {i}: {len(text)} chars")
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                price = _guess_price(line)
                category = None
                for kw in ["Pen", "Ink", "Paper", "Notebook", "Accessory", "Set", "Refill"]:
                    if re.search(rf"\b{kw}\b", line, re.IGNORECASE):
                        category = kw
                        break
                if re.search(r"[A-Za-z]{3,}", line):
                    rows.append({
                        "name": line,
                        "category": category,
                        "price": price,
                        "sku": None,
                        "url": None
                    })
    df = pd.DataFrame(rows).drop_duplicates(subset=["name"])
    return df.head(1000), logs

def parse_products_from_csv(file_like):
    return pd.read_csv(file_like)
