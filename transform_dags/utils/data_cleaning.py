from datetime import datetime
from decimal import Decimal
import re

def clean_string(value):
    """Clean string values, converting '-' to None or handling empty values."""
    if value and value.strip().upper() in ("-", "N.A.", "NA"):
        return None

    return value.strip()

def parse_date(date_str):
    """Parse date string to datetime.date, return None if invalid."""
    if not date_str or date_str.strip() in ["-", ""]:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None

def parse_decimal(value):
    """Parse value to Decimal, return None if invalid."""
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return None
    
def parse_int(value):
    """Convert value safely to int or None."""
    try:
        if value is None or str(value).strip() in ["-", ""]:
            return None
        return int(float(value))  # handle cases like "10.0"
    except Exception:
        return None
    

def parse_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in ("yes", "true")
    return None


def normalize_interest_frequency(raw_text: str) -> str:
    """
    Normalize interest payment frequency into standard categories:
    MONTHLY, QUARTERLY, SEMI-ANNUAL, ANNUAL, ON_MATURITY, UNKNOWN, OTHER
    """
    if not raw_text or raw_text.strip().upper() in ["-", "N.A.", "NA", "NULL", "NONE"]:
        return "UNKNOWN"
    
    text = raw_text.lower()
    
    if "monthly" in text or "twelve times" in text:
        return "MONTHLY"
    elif "quarter" in text or "quarterly" in text:
        return "QUARTERLY"
    elif "semi" in text and "annual" in text:
        return "SEMI-ANNUAL"
    elif "twice a year" in text:
        return "SEMI-ANNUAL"
    elif "annual" in text or "once a year" in text:
        return "ANNUAL"
    elif "maturity" in text or "till maturity" in text:
        return "ON_MATURITY"
    else:
        return "OTHER"



def build_tenure_interval(years=0, months=0, days=0):
    parts = []
    if years: parts.append(f"{years} years")
    if months: parts.append(f"{months} months")
    if days: parts.append(f"{days} days")
    return " ".join(parts) if parts else None



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