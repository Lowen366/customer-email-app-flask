def _format_product_line(p):
    price = p.get("price")
    price_str = f"£{price:.2f}" if isinstance(price, (int, float)) else ""
    name = p.get("name") or "Product"
    url = p.get("url")
    bullet = f"- {name}"
    if price_str: bullet += f" — {price_str}"
    if url: bullet += f" ({url})"
    return bullet

def build_email_for_customer(customer, recommendations, greeting_tpl, intro_tpl, footer_tpl, sender_name):
    name = (customer.get("name") or "").strip() or "there"
    greeting = greeting_tpl.format(name=name)
    intro = intro_tpl.format(name=name)
    bullets = "\n".join(_format_product_line(p) for p in recommendations) if recommendations else "- (No suitable items found yet)"
    footer = footer_tpl.format(sender_name=sender_name, name=name)
    body = f"""{greeting}

{intro}

{bullets}

{footer}
"""
    return body.strip()
