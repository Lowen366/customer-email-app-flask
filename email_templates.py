# email_templates.py
from typing import List, Dict, Optional

def _to_price_str(val) -> str:
    try:
        f = float(val)
        return f"£{f:.2f}"
    except Exception:
        return ""

def _format_product_line(p: Dict) -> str:
    price_str = _to_price_str(p.get("price"))
    name = (p.get("name") or "Product").strip()
    url = (p.get("url") or "").strip()
    bullet = f"- {name}"
    if price_str:
        bullet += f" — {price_str}"
    if url:
        bullet += f" ({url})"
    return bullet

def build_email_for_customer(
    customer: Dict,
    recommendations: List[Dict],
    greeting_tpl: str,
    intro_tpl: str,
    footer_tpl: str,
    sender_name: str,
) -> str:
    """
    Plain-text email body (BACKWARD-COMPATIBLE with your current code).
    """
    name = (customer.get("name") or "").strip() or "there"
    greeting = greeting_tpl.format(name=name)
    intro = intro_tpl.format(name=name)
    bullets = "\n".join(_format_product_line(p) for p in (recommendations or [])) if recommendations else "- (No suitable items found yet)"
    footer = footer_tpl.format(sender_name=sender_name, name=name)
    body = f"""{greeting}

{intro}

{bullets}

{footer}
"""
    return body.strip()

# -----------------------------
# Optional: HTML rendering
# -----------------------------
def _format_product_li_html(p: Dict) -> str:
    price_str = _to_price_str(p.get("price"))
    name = (p.get("name") or "Product").strip()
    url = (p.get("url") or "").strip()

    label = name
    if price_str:
        label += f" — {price_str}"

    if url:
        # basic safe anchor; real sanitization should be done upstream if needed
        return f'<li><a href="{url}" target="_blank" rel="noopener noreferrer">{label}</a></li>'
    return f"<li>{label}</li>"

def build_email_html_for_customer(
    customer: Dict,
    recommendations: List[Dict],
    greeting_tpl: str,
    intro_tpl: str,
    footer_tpl: str,
    sender_name: str,
    cta_text: Optional[str] = None,
    cta_url: Optional[str] = None,
) -> str:
    """
    HTML email body (nice for preview panes and ESPs). Safe, minimal inline styles.
    """
    name = (customer.get("name") or "").strip() or "there"
    greeting = greeting_tpl.format(name=name)
    intro = intro_tpl.format(name=name)
    items_html = (
        "<ul>" + "".join(_format_product_li_html(p) for p in (recommendations or [])) + "</ul>"
        if recommendations else "<p><em>(No suitable items found yet)</em></p>"
    )
    footer = footer_tpl.format(sender_name=sender_name, name=name)

    cta_html = ""
    if cta_text and cta_url:
        cta_html = f'''
        <p style="margin-top:12px;">
          <a href="{cta_url}" target="_blank" rel="noopener noreferrer"
             style="display:inline-block;padding:10px 14px;border:1px solid #0d6efd;border-radius:8px;
                    text-decoration:none;">{cta_text}</a>
        </p>'''

    html = f"""
    <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;line-height:1.5;color:#222;">
      <p>{greeting}</p>
      <p>{intro}</p>
      {items_html}
      {cta_html}
      <p style="margin-top:12px; color:#555; font-size:13px; white-space:pre-line;">{footer}</p>
    </div>
    """.strip()
    return html

# -----------------------------
# Optional: subject helper
# -----------------------------
def build_subject(subject_tpl: str, customer: Dict) -> str:
    """
    Allows templates like "Your picks, {name}".
    """
    name = (customer.get("name") or "").strip() or "there"
    return subject_tpl.format(name=name)
