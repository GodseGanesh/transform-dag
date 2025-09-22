import re

import re


def parse_coupon_rate(value):
    if not value or value.strip().upper().replace(".", "") in ["-", "NA"]:
        return None, "na"


    val = value.strip().upper()

    # --- XIRR linked ---
    if "XIRR" in val:
        matches = re.findall(r"(\d+(\.\d+)?)", val)
        rate = float(matches[0][0]) if matches else None  # pick first number
        return rate, "xirr"


    # --- Category-specific ---
    if re.search(r"\d+(\.\d+)?%?\s+FOR\s+CATEGORY", val):
        match = re.search(r"(\d+(\.\d+)?)", val)
        return float(match.group(1)), "category-specific"

    # --- Multi-rate (ranges or multiple %) ---
    if "/" in val or "," in val:
        matches = re.findall(r"(\d+(\.\d+)?)", val)
        rates = [float(m[0]) for m in matches]
        if rates:
            return rates, "multi-rate"

    # --- Reset/Base rate ---
    if "RESET RATE" in val or "BASE RATE" in val:
        return None, "reset-remark"

    # --- Linked (NIFTY, GSEC, RBI, Equity, Index, Performance) ---
    linked_keywords = ["NIFTY","G-SEC","GSEC","UNDERLYING","RBI REPO","INDEX","EQUITY","PERFORMANCE"]
    if any(keyword in val for keyword in linked_keywords):
        return None, "linked"   # always treat as linked, ignore numbers

    # --- Zero coupon ---
    if val in ["ZERO COUPON", "0", "0%", "0.001", "0.01", "ON MATURITY"]:
        return 0.0, "zero-coupon"

    # --- Numeric fixed rate ---
    numeric_match = re.search(r"(\d+(\.\d+)?)", val)
    if numeric_match:
        return float(numeric_match.group(1)), "fixed"

    # --- Fallback ---
    return None, "unknown"


examples = [
    "8.40%",
    "7%",
    "10.26",
    "9.55",
    "12.5",
    "11.5",
    "9",
    "9.56",
    "12",
    "10.39",
    "9.3",
    "9.35%",
    "8.65%",
    "9.40%",
    "9.05%",
    "9.00%",
    "8.58%",
    "7.06%",
    "5.78%",
    "7.07%",
    "9.90%",
    "10.32%",
    "8.46%",
    "7.17%",
    "8.85%",
    "6.80%",
    "7.14%",
    "8.70%",
    "8.99%",
    "8.25%",
    "8.50%",
    "7.39%",
    "11.95%",
    "9.9%",
    "0.01",
    "0.001",
    "15.5",
    "19.8",
    "14",
    "17.5",
    "5.25",
    "13.25",
    "8.1",
    "10",
    "8.75",
    "14.75",
    "20",
    "12.25",
    "11",
    "13.5",
    "13.8",
    "14.5",
    "8.9",
    "10.75",
    "10.1",
    "9.75",
    "8.91% (REFER REMARK)",
    "9% p.a.",
    "NIFTY LINKED",
    "NIFTY INDEX LINKED",
    "NIFTY LINKED.",
    "NIFTY 50 INDEX LINKED",
    "NIFTY 50 LINKED",
    "NIFTY 5O INDEX LINKED",
    "NIFTY 10 YR BENCHMARK G-SEC (CLEAN PRICE) INDEX LINKED",
    "NIFTY 10 YR BENCHMARK G-SEC INDEX LINKED",
    "10 YEAR GSEC LINKED",
    "G-SEC LINKED",
    "GSEC LINKED REFER REMARKS",
    "5.36%,10 YEAR GSEC LINKED",
    "5.69%",
    "5.79% GSEC 2030 LINKED",
    "(8.75%) 5.79% GSEC 2030 LINKED",
    "RESET RATE (REFER REMARKS)",
    "BASE RATE - REFER REMARKS",
    "RESET RATE (REFER REMARK)",
    "NA",
    "N.A.",
    "N.A",
    "8.76% FOR CATEGORY I, II AND III",
    "7.19% p.a. FOR CATAGORY I, II & III, 7.69% - IV",
    "ZERO COUPON",
    "0%",
    "0% (XIRR @ 8.55%)",
    "15% XIRR AND 7.26 GSEC LINKED",
    "12.51% XIRR",
    "(7.19/7.69)%",
    "(7.4/7.9)%",
    "6.88% (ADDITIONAL 0.50% TO RIs)",
    "8.1% (cat I & II and 0.2% additn int for cat III)",
    "LINKED TO PERFORMANCE OF THE BENCHMARK.",
    "10 YEAR GOVERNMENT SECURITY PRICE LINKED (CONTACT ISSUER FOR FURTHER DETAILS)",
    "EQUITY LINKED",
    "INDEX LINKED",
    "RBI Repo Rate",
    "Underlying Investment Strat",
    "-",
    "",
]

for ex in examples:
    print(ex, "=>", parse_coupon_rate(ex))
