import pandas as pd

def _norm_category(val):
    return str(val).strip().lower() if pd.notna(val) else None

def match_products_to_customers(products_df, customers_df, max_recs=3):
    p = products_df.copy()
    p["category_norm"] = p["category"].apply(_norm_category)
    p["price_num"] = pd.to_numeric(p["price"], errors="coerce")
    p_sorted = p.sort_values(by=["price_num"], ascending=True, na_position="last")

    rows = []
    for _, cust in customers_df.iterrows():
        cdict = cust.to_dict()
        preferred = _norm_category(cdict.get("preferred_category"))
        max_budget = None
        try:
            if pd.notna(cdict.get("max_budget")):
                max_budget = float(cdict.get("max_budget"))
        except:
            max_budget = None

        cand = p_sorted
        if preferred:
            cand = cand[
                (cand["category_norm"] == preferred)
                | (cand["name"].str.lower().str.contains(preferred, na=False))
            ]
            if cand.empty:
                cand = p_sorted
        if max_budget is not None:
            cand = cand[cand["price_num"].isna() | (cand["price_num"] <= max_budget)]

        recs = []
        for _, r in cand.head(int(max_recs)).iterrows():
            recs.append({
                "name": r.get("name"),
                "category": r.get("category"),
                "price": r.get("price_num"),
                "sku": r.get("sku"),
                "url": r.get("url"),
            })
        rows.append({"customer": cdict, "recommendations": recs})
    return pd.DataFrame(rows)
