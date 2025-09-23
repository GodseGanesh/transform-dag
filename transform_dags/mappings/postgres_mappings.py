from datetime import datetime
from utils.data_cleaning import clean_string, parse_date, parse_decimal, parse_int,normalize_interest_frequency,parse_bool,parse_coupon_rate


# ---------------- ISIN BASIC INFO ----------------
def map_postgres_isin_basic_info(data):
    """Map MongoDB data to PostgreSQL isin_basic_info table."""
    return {
        # "_table": "isin_basic_info",
        "isin_code": clean_string(data.get("ISIN_CODE")),
        "security_type": clean_string(data.get("SECURITY_TYPE")),
        "isin_description": clean_string(data.get("ISIN_DESCRIPTION")),
        "issue_description": clean_string(data.get("ISSUE_DESCRIPTION")),
        "former_name": clean_string(data.get("FORMER_NAME")),
        "coupon_rate_percent": parse_coupon_rate(data.get("COUPON_RATE_PERCENT")),
        "maturity_date": parse_date(data.get("MATURITY_DATE")),
        "ytm_percent": parse_decimal("YTM_PERCENT",data.get("YTM_PERCENT")),
        "tenure_years": parse_int(data.get("TENURE_YEARS")),
        "tenure_months": parse_int(data.get("TENURE_MONTHS")),
        "tenure_days": parse_int(data.get("TENURE_DAYS")),
        "minimum_investment_rs": parse_decimal("MINIMUM_INVESTMENT_RS",data.get("MINIMUM_INVESTMENT_RS")),
        "interest_payment_frequency_raw": clean_string(data.get("INTEREST_PAYMENT_FREQUENCY")),
        "interest_payment_frequency": normalize_interest_frequency(data.get("INTEREST_PAYMENT_FREQUENCY")),
        "face_value_rs": parse_decimal("FACE_VALUE_RS",data.get("FACE_VALUE_RS")),
        "percentage_sold": parse_decimal("PERCENTAGE_SOLD",data.get("PERCENTAGE_SOLD")),
        "isin_status": clean_string(data.get("ISIN_STATUS")),
        "issue_size_lakhs": parse_decimal("ISSUE_SIZE_LAKHS",data.get("ISSUE_SIZE_LAKHS")),
        "issue_date": parse_date(data.get("ISSUE_DATE")),
        "first_interest_payment_date": parse_date(data.get("FIRST_INTEREST_PAYMENT_DATE")),
        "mode_of_issuance": clean_string(data.get("MODE_OF_ISSUANCE")),
        "closing_date": parse_date(data.get("CLOSING_DATE")),
        "series": clean_string(data.get("SERIES")),
        "paid_up_value_rs": parse_decimal("PAID_UP_VALUE_RS",data.get("PAID_UP_VALUE_RS")),
        # Handle arrays for ratings
        "credit_ratings": [clean_string(data.get("CREDIT_RATING"))] if data.get("CREDIT_RATING") else [],
        "rating_agencies": [clean_string(data.get("RATING_AGENCY"))] if data.get("RATING_AGENCY") else [],
        "data_hash": clean_string(data.get("DATA_HASH")),
        "last_updated": datetime.now()
    }

# ---------------- ISIN DETAILED INFO ----------------
def map_postgres_isin_detailed_info(data):
    """Map MongoDB data to PostgreSQL isin_detailed_info table."""
    return {
        # "_table": "isin_detailed_info",
        "isin_code": clean_string(data.get("ISIN_CODE")),
        "listing_date": parse_date(data.get("LISTING_DATE")),
        "allotment_date": parse_date(data.get("ALLOTMENT_DATE")),
        "coupon_type": clean_string(data.get("COUPON_TYPE")),
        "day_count_convention": clean_string(data.get("DAY_COUNT_CONVENTION")),
        "security_collateral": clean_string(data.get("SECURITY_COLLATERAL")),
        "tax_category": clean_string(data.get("TAX_CATEGORY")),
        "call_option_date": parse_date(data.get("CALL_OPTION_DATE")),
        "put_option_date": parse_date(data.get("PUT_OPTION_DATE")),
        "primary_exchange": clean_string(data.get("PRIMARY_EXCHANGE")),
        "secondary_exchange": clean_string(data.get("SECONDARY_EXCHANGE")),
        "listed_unlisted": clean_string(data.get("LISTED_UNLISTED")),
        "listing_exchanges": clean_string(data.get("LISTING_EXCHANGES")),
        "trading_status": clean_string(data.get("TRADING_STATUS")),
        "market_lot": parse_int(data.get("MARKET_LOT")),
        "settlement_cycle": clean_string(data.get("SETTLEMENT_CYCLE")),
        "last_traded_price_rs": parse_decimal("LAST_TRADED_PRICE_RS",data.get("LAST_TRADED_PRICE_RS")),
        "last_traded_date": parse_date(data.get("LAST_TRADED_DATE")),
        "volume_traded": parse_int(data.get("VOLUME_TRADED")),
        "value_traded_lakhs": parse_decimal("VALUE_TRADED_LAKHS",data.get("VALUE_TRADED_LAKHS")),
        "number_of_trades": parse_int(data.get("NUMBER_OF_TRADES")),
        "weighted_avg_price_rs": parse_decimal("WEIGHTED_AVG_PRICE_RS",data.get("WEIGHTED_AVG_PRICE_RS")),
        "weighted_avg_yield_percent": parse_decimal('WEIGHTED_AVG_YIELD_PERCENT',data.get("WEIGHTED_AVG_YIELD_PERCENT")),
        "current_yield_percent": parse_decimal('CURRENT_YIELD_PERCENT',data.get("CURRENT_YIELD_PERCENT")),
        "duration_years": parse_decimal('DURATION_YEARS',data.get("DURATION_YEARS")),
        "convexity": parse_decimal('CONVEXITY',data.get("CONVEXITY")),
        "demat_requests_pending": parse_int(data.get("DEMAT_REQUESTS_PENDING")),
        "services_stopped": data.get("SERVICES_STOPPED") if isinstance(data.get("SERVICES_STOPPED"), bool) else None,
        "no_of_bonds_ncd": parse_int(data.get("NO_OF_BONDS_NCD")),
        "benefit_under_section": clean_string(data.get("BENEFIT_UNDER_SECTION")),
        "basel_compliant": data.get("BASEL_COMPLIANT") if isinstance(data.get("BASEL_COMPLIANT"), bool) else None,
        "lock_in_period": clean_string(data.get("LOCK_IN_PERIOD")),
        "use_of_proceeds": clean_string(data.get("USE_OF_PROCEEDS")),
        "seniority": clean_string(data.get("SENIORITY")),
        "redemption": clean_string(data.get("REDEMPTION")),
        "opening_date": parse_date(data.get("OPENING_DATE")),
        "bse_date_of_listing": parse_date(data.get("BSE_DATE_OF_LISTING")),
        "pricing_method": clean_string(data.get("PRICING_METHOD")),
        "due_for_maturity": parse_int(data.get("DUE_FOR_MATURITY")),
        "compounding_frequency": clean_string(data.get("COMPOUNDING_FREQUENCY")),
        "interest_payment_dates": clean_string(data.get("INTEREST_PAYMENT_DATES")),
        "interest_payment_day_convention": clean_string(data.get("INTEREST_PAYMENT_DAY_CONVENTION")),
        "payment_schedule": clean_string(data.get("PAYMENT_SCHEDULE")),
        "redemption_premium": clean_string(data.get("REDEMPTION_PREMIUM")),
        "call_option":parse_bool(data.get("CALL_OPTION")),
        "call_notification_period": clean_string(data.get("CALL_NOTIFICATION_PERIOD")),
        "put_option": parse_bool(data.get("PUT_OPTION")),
        "put_notification_period": clean_string(data.get("PUT_NOTIFICATION_PERIOD")),
        "buyback_option": clean_string(data.get("BUYBACK_OPTION")),
        "secured": parse_bool(data.get("SECURED")),
        "liquidation_status": clean_string(data.get("LIQUIDATION_STATUS")),
        "record_date_day_convention": clean_string(data.get("RECORD_DATE_DAY_CONVENTION")),
        "redemption_payment_day_convention": clean_string(data.get("REDEMPTION_PAYMENT_DAY_CONVENTION")),
        "reset_details": clean_string(data.get("RESET_DETAILS")),
        "transferable": parse_bool(data.get("TRANSFERABLE")),
        "greenshoe_option": parse_bool(data.get("GREENSHOE_OPTION")),
        "oversubscription_multiple": parse_decimal('OVERSUBSCRIPTION_MULTIPLE',data.get("OVERSUBSCRIPTION_MULTIPLE")),
        "percentage_sold_cumulative": parse_decimal('PERCENTAGE_SOLD_CUMULATIVE',data.get("PERCENTAGE_SOLD_CUMULATIVE")),
        "bse_scrip_code": clean_string(data.get("BSE_SCRIP_CODE")),
        "nse_symbol": clean_string(data.get("NSE_SYMBOL")),
        "nse_date_of_listing": parse_date(data.get("NSE_DATE_OF_LISTING")),
        "data_hash": clean_string(data.get("DATA_HASH")),
        "last_updated": datetime.now()
    }


# ---------------- COMPANY INFO ----------------
def map_postgres_company_info(data):
    """Map MongoDB data to PostgreSQL company_info table."""
    return {
        # "_table": "company_info",
        "issuer_name": clean_string(data.get("ISSUER_NAME")),
        "issuer_address": clean_string(data.get("ISSUER_ADDRESS")),
        "issuer_type": clean_string(data.get("ISSUER_TYPE")),
        "issuer_state": clean_string(data.get("ISSUER_STATE")),
        "issuer_website": clean_string(data.get("ISSUER_WEBSITE")),
        "contact_person": clean_string(data.get("CONTACT_PERSON")),
        "phone_number": clean_string(data.get("PHONE_NUMBER")),
        "fax_number": clean_string(data.get("FAX_NUMBER")),
        "email_id": clean_string(data.get("EMAIL_ID")),
        "guaranteed_by": clean_string(data.get("GUARANTEED_BY")),
        "registrar": clean_string(data.get("REGISTRAR")),
        "industry_group": clean_string(data.get("INDUSTRY_GROUP")),
        "macro_sector": clean_string(data.get("MACRO_SECTOR")),
        "micro_industry": clean_string(data.get("MICRO_INDUSTRY")),
        "product_service_activity": clean_string(data.get("PRODUCT_SERVICE_ACTIVITY")),
        "sector": clean_string(data.get("SECTOR")),
        "security_code": clean_string(data.get("SECURITY_CODE")),
        "data_hash": clean_string(data.get("DATA_HASH")),
        "last_updated": datetime.now()
    }

# ---------------- RTA INFO ----------------
def map_postgres_rta_info(data):
    """Map MongoDB data to PostgreSQL rta_info table."""
    return {
        # "_table": "rta_info",
        "rta_name": clean_string(data.get("RTA_NAME")),
        "rta_bp_id": clean_string(data.get("RTA_BP_ID")),
        "rta_address": clean_string(data.get("RTA_ADDRESS")),
        "rta_contact_person": clean_string(data.get("RTA_CONTACT_PERSON")),
        "rta_phone": clean_string(data.get("RTA_PHONE")),
        "rta_fax": clean_string(data.get("RTA_FAX")),
        "rta_email": clean_string(data.get("RTA_EMAIL")),
        "arrangers": clean_string(data.get("ARRANGERS")),
        "trustee": clean_string(data.get("TRUSTEE")),
        "im_term_sheet": clean_string(data.get("IM_TERM_SHEET")),
        "data_hash": clean_string(data.get("DATA_HASH")),
        "last_updated": datetime.now()
    }

# ---------------- MAPPING TABLES ----------------
def map_postgres_isin_company_map(isin_code, company_id):
    """Map ISIN to company_id for isin_company_map table."""
    return {
        # "_table": "isin_company_map",
        "isin_code": isin_code,
        "company_id": company_id,
        "primary_company": True,
        "mapped_on": datetime.now()
    }

def map_postgres_isin_rta_map(isin_code, rta_id, effective_from=None, effective_to=None):
    """Map ISIN to rta_id for isin_rta_map table."""
    return {
        # "_table": "isin_rta_map",
        "isin_code": isin_code,
        "rta_id": rta_id,
        "effective_from": effective_from or datetime.now().date(),
        "effective_to": effective_to,
        "mapped_on": datetime.now()
    }



def map_to_postgres(data):
    return {
        "isin_basic_info": map_postgres_isin_basic_info(data.get("isin_basic_info", {})),
        "isin_detailed_info": map_postgres_isin_detailed_info(data.get("isin_detailed_info", {})),
        "company_info": map_postgres_company_info(data.get("company_info", {})),
        "rta_info": map_postgres_rta_info(data.get("rta_info", {}))
    }



def upsert_company_and_map(cur, isin_code, company_data):
    """Upsert into company_info and link with isin_company_map."""
    issuer_name = company_data.get("issuer_name")
    if not issuer_name:
        return

    # 1. Try to find existing company
    cur.execute("SELECT company_id FROM company_info WHERE issuer_name = %s", (issuer_name,))
    row = cur.fetchone()

    if row:
        company_id = row[0]
        # Optionally update existing fields
        update_str = ", ".join([f"{col} = %s" for col in company_data.keys() if col != "issuer_name"])
        if update_str:
            values = [company_data[col] for col in company_data.keys() if col != "issuer_name"]
            values.append(issuer_name)
            cur.execute(f"UPDATE company_info SET {update_str}, last_updated = NOW() WHERE issuer_name = %s", values)
    else:
        # 2. Insert new company
        columns = list(company_data.keys())
        values = [company_data[col] for col in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)
        cur.execute(
            f"INSERT INTO company_info ({columns_str}) VALUES ({placeholders}) RETURNING company_id",
            values
        )
        company_id = cur.fetchone()[0]

    # 3. Ensure mapping exists
    cur.execute(
        "INSERT INTO isin_company_map (isin_code, company_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (isin_code, company_id)
    )


def upsert_rta_and_map(cur, isin_code, rta_data):
    """Upsert into rta_info and link with isin_rta_map."""
    rta_name = rta_data.get("rta_name")
    if not rta_name:
        return

    # 1. Try to find existing RTA
    cur.execute("SELECT rta_id FROM rta_info WHERE rta_name = %s", (rta_name,))
    row = cur.fetchone()

    if row:
        rta_id = row[0]
        # Update existing fields if needed
        update_str = ", ".join([f"{col} = %s" for col in rta_data.keys() if col != "rta_name"])
        if update_str:
            values = [rta_data[col] for col in rta_data.keys() if col != "rta_name"]
            values.append(rta_name)
            cur.execute(f"UPDATE rta_info SET {update_str}, last_updated = NOW() WHERE rta_name = %s", values)
    else:
        # 2. Insert new RTA
        columns = list(rta_data.keys())
        values = [rta_data[col] for col in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)
        cur.execute(
            f"INSERT INTO rta_info ({columns_str}) VALUES ({placeholders}) RETURNING rta_id",
            values
        )
        rta_id = cur.fetchone()[0]

    # 3. Ensure mapping exists
    cur.execute(
        "INSERT INTO isin_rta_map (isin_code, rta_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (isin_code, rta_id)
    )
