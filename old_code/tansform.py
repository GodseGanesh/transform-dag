import re
import logging
import time
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
from dateutil import parser
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json

load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

# PostgreSQL connection
PG_DSN = os.getenv("PG_DSN")

# Debug flag
DEBUG_MODE = True  # Set to False to disable detailed debug logging

# Setup logging
logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------
# Helpers
# -------------------------
def parse_date(val):
    if val in [None, "", "-", "N.A", "NA"]:
        return None
    if isinstance(val, (datetime, date)):
        return val
    try:
        parsed = parser.parse(str(val), dayfirst=True)
        return parsed.date() if parsed.time() == datetime.min.time() else parsed
    except Exception:
        logging.warning(f"parse_date: invalid {val}")
        return None

def parse_numeric(val, is_percent=False):
    if val in [None, "", "-", "N.A", "NA"]:
        return None
    try:
        s = str(val).strip()
        if is_percent:
            s = s.replace('%', '')
        cleaned = re.sub(r'[^\d\.\-]', '', s)
        if cleaned == '':
            return None
        num = Decimal(cleaned)
        return num
    except InvalidOperation:
        logging.warning(f"parse_numeric: invalid {val}")
        return None

def parse_boolean(val):
    if val in [None, "", "-", "N.A", "NA"]:
        return None
    s = str(val).strip().lower()
    return s in ("yes", "true", "y", "1", "t")

def clean_string(val, max_length=None):
    if val in [None, "", "-", "N.A", "NA"]:
        return None
    s = str(val).strip()
    if max_length and len(s) > max_length:
        return s[:max_length]
    return s

def log_migration(pg_conn, isin, collection, message, input_data=None):
    try:
        cur = pg_conn.cursor()
        error_message = message
        if input_data and DEBUG_MODE:
            # Truncate input_data to avoid exceeding VARCHAR(255) for error_message
            input_str = json.dumps(input_data, default=str)[:200]
            error_message = f"{message} | Input: {input_str}"
        cur.execute(
            "INSERT INTO migration_logs (isin_code, collection_name, error_message) VALUES (%s,%s,%s)",
            (isin, collection, error_message)
        )
        pg_conn.commit()
        if DEBUG_MODE:
            logging.debug(f"Logged to migration_logs: ISIN={isin}, collection={collection}, message={error_message}")
    except Exception as e:
        logging.error(f"Failed to insert migration log for ISIN {isin}: {str(e)}")
        pg_conn.rollback()

# -------------------------
# ETL Core
# -------------------------
def upsert_isin_and_related(pg_conn, mongo_db, isin):
    cur = pg_conn.cursor()
    try:
        # Read docs from Mongo
        doc_basic = mongo_db['isin_basic_info'].find_one({"ISIN_CODE": isin})
        doc_detail = mongo_db['isin_detailed_info'].find_one({"ISIN_CODE": isin})
        doc_company = mongo_db['isin_company_info'].find_one({"ISIN_CODE": isin})
        doc_rta = mongo_db['isin_rta_info'].find_one({"ISIN_CODE": isin})
        doc_rating = mongo_db.get('isin_rating_info', {}).find_one({"ISIN_CODE": isin}) if 'isin_rating_info' in mongo_db.list_collection_names() else None

        if DEBUG_MODE:
            logging.debug(f"Processing ISIN {isin}: basic={bool(doc_basic)}, detail={bool(doc_detail)}, "
                          f"company={bool(doc_company)}, rta={bool(doc_rta)}, rating={bool(doc_rating)}")

        if not isin or not re.match(r'^[A-Za-z0-9]{5,12}$', isin):
            log_migration(pg_conn, isin, 'validate', f"Invalid ISIN format: {isin}")
            return

        # ------------------------
        # 1. ISIN Master
        # ------------------------
        try:
            security_name = None
            security_type = None
            data_hash_master = None
            if doc_basic:
                security_type = clean_string(doc_basic.get("SECURITY_TYPE"))
                data_hash_master = clean_string(doc_basic.get("DATA_HASH"))
                security_name = clean_string(doc_basic.get("ISSUER_NAME") or doc_basic.get("ISIN_DESCRIPTION"))
            if doc_company and not security_name:
                security_name = clean_string(doc_company.get("ISSUER_NAME"))

            if DEBUG_MODE:
                logging.debug(f"ISIN {isin} isin_master: security_name={security_name}, "
                              f"security_type={security_type}, data_hash={data_hash_master}")

            cur.execute("""
                INSERT INTO isin_master (isin_code, security_name, security_type, data_hash)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (isin_code) DO UPDATE
                SET security_name = COALESCE(EXCLUDED.security_name, isin_master.security_name),
                    security_type = COALESCE(EXCLUDED.security_type, isin_master.security_type),
                    data_hash = COALESCE(EXCLUDED.data_hash, isin_master.data_hash),
                    last_updated = NOW();
            """, (isin, security_name, security_type, data_hash_master))
            pg_conn.commit()
            # Verify insert
            cur.execute("SELECT COUNT(*) FROM isin_master WHERE isin_code = %s", (isin,))
            count = cur.fetchone()[0]
            if count > 0:
                logging.info(f"ISIN {isin} inserted/updated in isin_master successfully")
            else:
                logging.error(f"ISIN {isin} failed to insert/update in isin_master")
                log_migration(pg_conn, isin, 'isin_master', "Failed to insert/update: no rows affected")
        except Exception as e:
            logging.error(f"Error in isin_master for ISIN {isin}: {str(e)}")
            pg_conn.rollback()
            log_migration(pg_conn, isin, 'isin_master', f"master upsert error: {str(e)}",
                          input_data={"security_name": security_name, "security_type": security_type})

        # ------------------------
        # 2. Company Info
        # ------------------------
        try:
            company_id = None
            if doc_company:
                issuer_name = clean_string(doc_company.get("ISSUER_NAME"))
                if issuer_name:
                    company_data = {
                        "issuer_name": issuer_name,
                        "issuer_address": clean_string(doc_company.get("ISSUER_ADDRESS")),
                        "issuer_type": clean_string(doc_company.get("ISSUER_TYPE")),
                        "issuer_state": clean_string(doc_company.get("ISSUER_STATE")),
                        "issuer_website": clean_string(doc_company.get("ISSUER_WEBSITE")),
                        "contact_person": clean_string(doc_company.get("CONTACT_PERSON")),
                        "phone_number": clean_string(doc_company.get("PHONE_NUMBER")),
                        "fax_number": clean_string(doc_company.get("FAX_NUMBER")),
                        "email_id": clean_string(doc_company.get("EMAIL_ID")),
                        "guaranteed_by": clean_string(doc_company.get("GUARANTEED_BY")),
                        "registrar": clean_string(doc_company.get("REGISTRAR")),
                        "industry_group": clean_string(doc_company.get("INDUSTRY_GROUP")),
                        "macro_sector": clean_string(doc_company.get("MACRO_SECTOR")),
                        "micro_industry": clean_string(doc_company.get("MICRO_INDUSTRY")),
                        "product_service_activity": clean_string(doc_company.get("PRODUCT_SERVICE_ACTIVITY")),
                        "sector": clean_string(doc_company.get("SECTOR")),
                        "data_hash": clean_string(doc_company.get("DATA_HASH"))
                    }
                    if DEBUG_MODE:
                        logging.debug(f"ISIN {isin} company_info: {company_data}")

                    cur.execute("""
                        INSERT INTO company_info (issuer_name, issuer_address, issuer_type, issuer_state,
                                                  issuer_website, contact_person, phone_number, fax_number,
                                                  email_id, guaranteed_by, registrar, industry_group, macro_sector,
                                                  micro_industry, product_service_activity, sector, data_hash)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (issuer_name) DO UPDATE
                        SET issuer_address = COALESCE(EXCLUDED.issuer_address, company_info.issuer_address),
                            issuer_type = COALESCE(EXCLUDED.issuer_type, company_info.issuer_type),
                            issuer_state = COALESCE(EXCLUDED.issuer_state, company_info.issuer_state),
                            issuer_website = COALESCE(EXCLUDED.issuer_website, company_info.issuer_website),
                            contact_person = COALESCE(EXCLUDED.contact_person, company_info.contact_person),
                            phone_number = COALESCE(EXCLUDED.phone_number, company_info.phone_number),
                            fax_number = COALESCE(EXCLUDED.fax_number, company_info.fax_number),
                            email_id = COALESCE(EXCLUDED.email_id, company_info.email_id),
                            guaranteed_by = COALESCE(EXCLUDED.guaranteed_by, company_info.guaranteed_by),
                            registrar = COALESCE(EXCLUDED.registrar, company_info.registrar),
                            industry_group = COALESCE(EXCLUDED.industry_group, company_info.industry_group),
                            macro_sector = COALESCE(EXCLUDED.macro_sector, company_info.macro_sector),
                            micro_industry = COALESCE(EXCLUDED.micro_industry, company_info.micro_industry),
                            product_service_activity = COALESCE(EXCLUDED.product_service_activity, company_info.product_service_activity),
                            sector = COALESCE(EXCLUDED.sector, company_info.sector),
                            data_hash = COALESCE(EXCLUDED.data_hash, company_info.data_hash),
                            last_updated = NOW()
                        RETURNING company_id;
                    """, tuple(company_data.values()))
                    row = cur.fetchone()
                    company_id = row[0] if row else None
                    if company_id:
                        cur.execute("""
                            INSERT INTO isin_company_map (isin_code, company_id, primary_company)
                            VALUES (%s,%s,TRUE)
                            ON CONFLICT (isin_code, company_id) DO NOTHING;
                        """, (isin, company_id))
                        if DEBUG_MODE:
                            logging.debug(f"ISIN {isin} inserted into isin_company_map with company_id={company_id}")
                    # Verify insert
                    cur.execute("SELECT COUNT(*) FROM company_info WHERE issuer_name = %s", (issuer_name,))
                    count = cur.fetchone()[0]
                    if count > 0:
                        logging.info(f"ISIN {isin} inserted/updated in company_info successfully")
                    else:
                        logging.error(f"ISIN {isin} failed to insert/update in company_info")
                        log_migration(pg_conn, isin, 'company_info', "Failed to insert/update: no rows affected")
                    if company_id:
                        cur.execute("SELECT COUNT(*) FROM isin_company_map WHERE isin_code = %s AND company_id = %s",
                                    (isin, company_id))
                        map_count = cur.fetchone()[0]
                        if map_count > 0:
                            logging.info(f"ISIN {isin} inserted/updated in isin_company_map successfully")
                        else:
                            logging.error(f"ISIN {isin} failed to insert/update in isin_company_map")
                            log_migration(pg_conn, isin, 'isin_company_map', "Failed to insert/update: no rows affected")
                else:
                    logging.warning(f"ISIN {isin} skipped company_info: no issuer_name")
                    log_migration(pg_conn, isin, 'company_info', "Skipped: no issuer_name")
            pg_conn.commit()
        except Exception as e:
            logging.error(f"Error in company_info for ISIN {isin}: {str(e)}")
            pg_conn.rollback()
            log_migration(pg_conn, isin, 'company_info', f"company upsert error: {str(e)}", input_data=doc_company)

        # ------------------------
        # 3. Basic Info
        # ------------------------
        try:
            if doc_basic:
                basic_data = {
                    "isin": isin,
                    "security_type": clean_string(doc_basic.get("SECURITY_TYPE")),
                    "isin_description": clean_string(doc_basic.get("ISIN_DESCRIPTION")),
                    "former_name": clean_string(doc_basic.get("FORMER_NAME")),
                    "coupon_rate_percent": parse_numeric(doc_basic.get("COUPON_RATE_PERCENT"), is_percent=True),
                    "maturity_date": parse_date(doc_basic.get("MATURITY_DATE")),
                    "ytm_percent": parse_numeric(doc_basic.get("YTM_PERCENT"), is_percent=True),
                    "tenure_years": parse_numeric(doc_basic.get("TENURE_YEARS")),
                    "tenure_months": parse_numeric(doc_basic.get("TENURE_MONTHS")),
                    "tenure_days": parse_numeric(doc_basic.get("TENURE_DAYS")),
                    "minimum_investment_rs": parse_numeric(doc_basic.get("MINIMUM_INVESTMENT_RS")),
                    "interest_payment_frequency": clean_string(doc_basic.get("INTEREST_PAYMENT_FREQUENCY")),
                    "face_value_rs": parse_numeric(doc_basic.get("FACE_VALUE_RS")),
                    "percentage_sold": parse_numeric(doc_basic.get("PERCENTAGE_SOLD"), is_percent=True),
                    "isin_status": clean_string(doc_basic.get("ISIN_STATUS")),
                    "issue_size_lakhs": parse_numeric(doc_basic.get("ISSUE_SIZE_LAKHS")),
                    "bse_scrip_code": clean_string(doc_basic.get("BSE_SCRIP_CODE")),
                    "nse_symbol": clean_string(doc_basic.get("NSE_SYMBOL")),
                    "data_hash": clean_string(doc_basic.get("DATA_HASH"))
                }
                if DEBUG_MODE:
                    logging.debug(f"ISIN {isin} isin_basic_info: {basic_data}")
                cur.execute("""
                    INSERT INTO isin_basic_info (isin_code, security_type, isin_description, former_name,
                        coupon_rate_percent, maturity_date, ytm_percent, tenure_years, tenure_months, tenure_days,
                        minimum_investment_rs, interest_payment_frequency, face_value_rs, percentage_sold,
                        isin_status, issue_size_lakhs, bse_scrip_code, nse_symbol, data_hash)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (isin_code) DO UPDATE
                    SET isin_description = COALESCE(EXCLUDED.isin_description, isin_basic_info.isin_description),
                        coupon_rate_percent = COALESCE(EXCLUDED.coupon_rate_percent, isin_basic_info.coupon_rate_percent),
                        maturity_date = COALESCE(EXCLUDED.maturity_date, isin_basic_info.maturity_date),
                        ytm_percent = COALESCE(EXCLUDED.ytm_percent, isin_basic_info.ytm_percent),
                        tenure_years = COALESCE(EXCLUDED.tenure_years, isin_basic_info.tenure_years),
                        tenure_months = COALESCE(EXCLUDED.tenure_months, isin_basic_info.tenure_months),
                        tenure_days = COALESCE(EXCLUDED.tenure_days, isin_basic_info.tenure_days),
                        minimum_investment_rs = COALESCE(EXCLUDED.minimum_investment_rs, isin_basic_info.minimum_investment_rs),
                        interest_payment_frequency = COALESCE(EXCLUDED.interest_payment_frequency, isin_basic_info.interest_payment_frequency),
                        face_value_rs = COALESCE(EXCLUDED.face_value_rs, isin_basic_info.face_value_rs),
                        percentage_sold = COALESCE(EXCLUDED.percentage_sold, isin_basic_info.percentage_sold),
                        isin_status = COALESCE(EXCLUDED.isin_status, isin_basic_info.isin_status),
                        issue_size_lakhs = COALESCE(EXCLUDED.issue_size_lakhs, isin_basic_info.issue_size_lakhs),
                        bse_scrip_code = COALESCE(EXCLUDED.bse_scrip_code, isin_basic_info.bse_scrip_code),
                        nse_symbol = COALESCE(EXCLUDED.nse_symbol, isin_basic_info.nse_symbol),
                        data_hash = COALESCE(EXCLUDED.data_hash, isin_basic_info.data_hash),
                        last_updated = NOW();
                """, tuple(basic_data.values()))
                # Verify insert
                cur.execute("SELECT COUNT(*) FROM isin_basic_info WHERE isin_code = %s", (isin,))
                count = cur.fetchone()[0]
                if count > 0:
                    logging.info(f"ISIN {isin} inserted/updated in isin_basic_info successfully")
                else:
                    logging.error(f"ISIN {isin} failed to insert/update in isin_basic_info")
                    log_migration(pg_conn, isin, 'isin_basic_info', "Failed to insert/update: no rows affected")
            else:
                logging.warning(f"ISIN {isin} skipped isin_basic_info: no doc_basic")
                log_migration(pg_conn, isin, 'isin_basic_info', "Skipped: no doc_basic")
            pg_conn.commit()
        except Exception as e:
            logging.error(f"Error in isin_basic_info for ISIN {isin}: {str(e)}")
            pg_conn.rollback()
            log_migration(pg_conn, isin, 'isin_basic_info', f"basic upsert error: {str(e)}", input_data=doc_basic)

        # ------------------------
        # 4. Detailed Info
        # ------------------------
        try:
            if doc_detail:
                detail_data = {
                    "isin": isin,
                    "nse_date_of_listing": parse_date(doc_detail.get("NSE_DATE_OF_LISTING")),
                    "closing_date": parse_date(doc_detail.get("CLOSING_DATE")),
                    "series": clean_string(doc_detail.get("SERIES"), 100),
                    "paid_up_value_rs": parse_numeric(doc_detail.get("PAID_UP_VALUE_RS")),
                    "issue_date": parse_date(doc_detail.get("ISSUE_DATE")),
                    "listing_date": parse_date(doc_detail.get("LISTING_DATE")),
                    "allotment_date": parse_date(doc_detail.get("ALLOTMENT_DATE")),
                    "coupon_type": clean_string(doc_detail.get("COUPON_TYPE"), 100),
                    "day_count_convention": clean_string(doc_detail.get("DAY_COUNT_CONVENTION"), 100),
                    "security_collateral": clean_string(doc_detail.get("SECURITY_COLLATERAL")),
                    "tax_category": clean_string(doc_detail.get("TAX_CATEGORY"), 50),
                    "call_option_date": parse_date(doc_detail.get("CALL_OPTION_DATE")),
                    "put_option_date": parse_date(doc_detail.get("PUT_OPTION_DATE")),
                    "primary_exchange": clean_string(doc_detail.get("PRIMARY_EXCHANGE"), 50),
                    "secondary_exchange": clean_string(doc_detail.get("SECONDARY_EXCHANGE"), 50),
                    "listed_unlisted": clean_string(doc_detail.get("LISTED_UNLISTED"), 50),
                    "listing_exchanges": clean_string(doc_detail.get("LISTING_EXCHANGES"), 255),
                    "trading_status": clean_string(doc_detail.get("TRADING_STATUS"), 100),
                    "market_lot": parse_numeric(doc_detail.get("MARKET_LOT")),
                    "settlement_cycle": clean_string(doc_detail.get("SETTLEMENT_CYCLE"), 100),
                    "last_traded_price_rs": parse_numeric(doc_detail.get("LAST_TRADED_PRICE_RS")),
                    "last_traded_date": parse_date(doc_detail.get("LAST_TRADED_DATE")),
                    "volume_traded": parse_numeric(doc_detail.get("VOLUME_TRADED")),
                    "value_traded_lakhs": parse_numeric(doc_detail.get("VALUE_TRADED_LAKHS")),
                    "number_of_trades": parse_numeric(doc_detail.get("NUMBER_OF_TRADES")),
                    "weighted_avg_price_rs": parse_numeric(doc_detail.get("WEIGHTED_AVG_PRICE_RS")),
                    "weighted_avg_yield_percent": parse_numeric(doc_detail.get("WEIGHTED_AVG_YIELD_PERCENT"), is_percent=True),
                    "current_yield_percent": parse_numeric(doc_detail.get("CURRENT_YIELD_PERCENT"), is_percent=True),
                    "duration_years": parse_numeric(doc_detail.get("DURATION_YEARS")),
                    "convexity": parse_numeric(doc_detail.get("CONVEXITY")),
                    "demat_requests_pending": parse_numeric(doc_detail.get("DEMAT_REQUESTS_PENDING")),
                    "services_stopped": parse_boolean(doc_detail.get("SERVICES_STOPPED")),
                    "no_of_bonds_ncd": parse_numeric(doc_detail.get("NO_OF_BONDS_NCD")),
                    "benefit_under_section": clean_string(doc_detail.get("BENEFIT_UNDER_SECTION"), 255),
                    "basel_compliant": parse_boolean(doc_detail.get("BASEL_COMPLIANT")),
                    "lock_in_period": clean_string(doc_detail.get("LOCK_IN_PERIOD"), 100),
                    "use_of_proceeds": clean_string(doc_detail.get("USE_OF_PROCEEDS")),
                    "seniority": clean_string(doc_detail.get("SENIORITY"), 255),
                    "redemption": clean_string(doc_detail.get("REDEMPTION")),
                    "opening_date": parse_date(doc_detail.get("OPENING_DATE")),
                    "bse_date_of_listing": parse_date(doc_detail.get("BSE_DATE_OF_LISTING")),
                    "pricing_method": clean_string(doc_detail.get("PRICING_METHOD")),
                    "due_for_maturity": parse_numeric(doc_detail.get("DUE_FOR_MATURITY")),
                    "compounding_frequency": clean_string(doc_detail.get("COMPOUNDING_FREQUENCY"), 100),
                    "interest_payment_dates": clean_string(doc_detail.get("INTEREST_PAYMENT_DATES")),
                    "interest_payment_day_convention": clean_string(doc_detail.get("INTEREST_PAYMENT_DAY_CONVENTION"), 100),
                    "payment_schedule": clean_string(doc_detail.get("PAYMENT_SCHEDULE")),
                    "redemption_premium": clean_string(doc_detail.get("REDEMPTION_PREMIUM"), 255),
                    "call_option": parse_boolean(doc_detail.get("CALL_OPTION")),
                    "call_notification_period": clean_string(doc_detail.get("CALL_NOTIFICATION_PERIOD"), 255),
                    "put_option": parse_boolean(doc_detail.get("PUT_OPTION")),
                    "put_notification_period": clean_string(doc_detail.get("PUT_NOTIFICATION_PERIOD"), 255),
                    "buyback_option": clean_string(doc_detail.get("BUYBACK_OPTION"), 100),
                    "secured": parse_boolean(doc_detail.get("SECURED")),
                    "liquidation_status": clean_string(doc_detail.get("LIQUIDATION_STATUS"), 255),
                    "record_date_day_convention": clean_string(doc_detail.get("RECORD_DATE_DAY_CONVENTION"), 255),
                    "redemption_payment_day_convention": clean_string(doc_detail.get("REDEMPTION_PAYMENT_DAY_CONVENTION"), 255),
                    "reset_details": clean_string(doc_detail.get("RESET_DETAILS")),
                    "transferable": parse_boolean(doc_detail.get("TRANSFERABLE")),
                    "greenshoe_option": parse_boolean(doc_detail.get("GREENSHOE_OPTION")),
                    "oversubscription_multiple": parse_numeric(doc_detail.get("OVERSUBSCRIPTION_MULTIPLE")),
                    "percentage_sold_cumulative": parse_numeric(doc_detail.get("PERCENTAGE_SOLD_CUMULATIVE"), is_percent=True),
                    "data_hash": clean_string(doc_detail.get("DATA_HASH"), 64)
                }
                if DEBUG_MODE:
                    logging.debug(f"ISIN {isin} isin_detailed_info: {detail_data}")
                cur.execute("""
                    INSERT INTO isin_detailed_info (
                        isin_code, nse_date_of_listing, closing_date, series, paid_up_value_rs,
                        issue_date, listing_date, allotment_date, coupon_type, day_count_convention,
                        security_collateral, tax_category, call_option_date, put_option_date,
                        primary_exchange, secondary_exchange, listed_unlisted, listing_exchanges,
                        trading_status, market_lot, settlement_cycle, last_traded_price_rs, last_traded_date,
                        volume_traded, value_traded_lakhs, number_of_trades, weighted_avg_price_rs,
                        weighted_avg_yield_percent, current_yield_percent, duration_years, convexity,
                        demat_requests_pending, services_stopped, no_of_bonds_ncd, benefit_under_section,
                        basel_compliant, lock_in_period, use_of_proceeds, seniority, redemption,
                        opening_date, bse_date_of_listing, pricing_method, due_for_maturity, compounding_frequency,
                        interest_payment_dates, interest_payment_day_convention, payment_schedule,
                        redemption_premium, call_option, call_notification_period, put_option,
                        put_notification_period, buyback_option, secured, liquidation_status,
                        record_date_day_convention, redemption_payment_day_convention, reset_details,
                        transferable, greenshoe_option, oversubscription_multiple, percentage_sold_cumulative,
                        data_hash
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (isin_code) DO UPDATE
                    SET 
                        nse_date_of_listing = COALESCE(EXCLUDED.nse_date_of_listing, isin_detailed_info.nse_date_of_listing),
                        closing_date = COALESCE(EXCLUDED.closing_date, isin_detailed_info.closing_date),
                        series = COALESCE(EXCLUDED.series, isin_detailed_info.series),
                        paid_up_value_rs = COALESCE(EXCLUDED.paid_up_value_rs, isin_detailed_info.paid_up_value_rs),
                        issue_date = COALESCE(EXCLUDED.issue_date, isin_detailed_info.issue_date),
                        listing_date = COALESCE(EXCLUDED.listing_date, isin_detailed_info.listing_date),
                        allotment_date = COALESCE(EXCLUDED.allotment_date, isin_detailed_info.allotment_date),
                        coupon_type = COALESCE(EXCLUDED.coupon_type, isin_detailed_info.coupon_type),
                        day_count_convention = COALESCE(EXCLUDED.day_count_convention, isin_detailed_info.day_count_convention),
                        security_collateral = COALESCE(EXCLUDED.security_collateral, isin_detailed_info.security_collateral),
                        tax_category = COALESCE(EXCLUDED.tax_category, isin_detailed_info.tax_category),
                        call_option_date = COALESCE(EXCLUDED.call_option_date, isin_detailed_info.call_option_date),
                        put_option_date = COALESCE(EXCLUDED.put_option_date, isin_detailed_info.put_option_date),
                        primary_exchange = COALESCE(EXCLUDED.primary_exchange, isin_detailed_info.primary_exchange),
                        secondary_exchange = COALESCE(EXCLUDED.secondary_exchange, isin_detailed_info.secondary_exchange),
                        listed_unlisted = COALESCE(EXCLUDED.listed_unlisted, isin_detailed_info.listed_unlisted),
                        listing_exchanges = COALESCE(EXCLUDED.listing_exchanges, isin_detailed_info.listing_exchanges),
                        trading_status = COALESCE(EXCLUDED.trading_status, isin_detailed_info.trading_status),
                        market_lot = COALESCE(EXCLUDED.market_lot, isin_detailed_info.market_lot),
                        settlement_cycle = COALESCE(EXCLUDED.settlement_cycle, isin_detailed_info.settlement_cycle),
                        last_traded_price_rs = COALESCE(EXCLUDED.last_traded_price_rs, isin_detailed_info.last_traded_price_rs),
                        last_traded_date = COALESCE(EXCLUDED.last_traded_date, isin_detailed_info.last_traded_date),
                        volume_traded = COALESCE(EXCLUDED.volume_traded, isin_detailed_info.volume_traded),
                        value_traded_lakhs = COALESCE(EXCLUDED.value_traded_lakhs, isin_detailed_info.value_traded_lakhs),
                        number_of_trades = COALESCE(EXCLUDED.number_of_trades, isin_detailed_info.number_of_trades),
                        weighted_avg_price_rs = COALESCE(EXCLUDED.weighted_avg_price_rs, isin_detailed_info.weighted_avg_price_rs),
                        weighted_avg_yield_percent = COALESCE(EXCLUDED.weighted_avg_yield_percent, isin_detailed_info.weighted_avg_yield_percent),
                        current_yield_percent = COALESCE(EXCLUDED.current_yield_percent, isin_detailed_info.current_yield_percent),
                        duration_years = COALESCE(EXCLUDED.duration_years, isin_detailed_info.duration_years),
                        convexity = COALESCE(EXCLUDED.convexity, isin_detailed_info.convexity),
                        demat_requests_pending = COALESCE(EXCLUDED.demat_requests_pending, isin_detailed_info.demat_requests_pending),
                        services_stopped = COALESCE(EXCLUDED.services_stopped, isin_detailed_info.services_stopped),
                        no_of_bonds_ncd = COALESCE(EXCLUDED.no_of_bonds_ncd, isin_detailed_info.no_of_bonds_ncd),
                        benefit_under_section = COALESCE(EXCLUDED.benefit_under_section, isin_detailed_info.benefit_under_section),
                        basel_compliant = COALESCE(EXCLUDED.basel_compliant, isin_detailed_info.basel_compliant),
                        lock_in_period = COALESCE(EXCLUDED.lock_in_period, isin_detailed_info.lock_in_period),
                        use_of_proceeds = COALESCE(EXCLUDED.use_of_proceeds, isin_detailed_info.use_of_proceeds),
                        seniority = COALESCE(EXCLUDED.seniority, isin_detailed_info.seniority),
                        redemption = COALESCE(EXCLUDED.redemption, isin_detailed_info.redemption),
                        opening_date = COALESCE(EXCLUDED.opening_date, isin_detailed_info.opening_date),
                        bse_date_of_listing = COALESCE(EXCLUDED.bse_date_of_listing, isin_detailed_info.bse_date_of_listing),
                        pricing_method = COALESCE(EXCLUDED.pricing_method, isin_detailed_info.pricing_method),
                        due_for_maturity = COALESCE(EXCLUDED.due_for_maturity, isin_detailed_info.due_for_maturity),
                        compounding_frequency = COALESCE(EXCLUDED.compounding_frequency, isin_detailed_info.compounding_frequency),
                        interest_payment_dates = COALESCE(EXCLUDED.interest_payment_dates, isin_detailed_info.interest_payment_dates),
                        interest_payment_day_convention = COALESCE(EXCLUDED.interest_payment_day_convention, isin_detailed_info.interest_payment_day_convention),
                        payment_schedule = COALESCE(EXCLUDED.payment_schedule, isin_detailed_info.payment_schedule),
                        redemption_premium = COALESCE(EXCLUDED.redemption_premium, isin_detailed_info.redemption_premium),
                        call_option = COALESCE(EXCLUDED.call_option, isin_detailed_info.call_option),
                        call_notification_period = COALESCE(EXCLUDED.call_notification_period, isin_detailed_info.call_notification_period),
                        put_option = COALESCE(EXCLUDED.put_option, isin_detailed_info.put_option),
                        put_notification_period = COALESCE(EXCLUDED.put_notification_period, isin_detailed_info.put_notification_period),
                        buyback_option = COALESCE(EXCLUDED.buyback_option, isin_detailed_info.buyback_option),
                        secured = COALESCE(EXCLUDED.secured, isin_detailed_info.secured),
                        liquidation_status = COALESCE(EXCLUDED.liquidation_status, isin_detailed_info.liquidation_status),
                        record_date_day_convention = COALESCE(EXCLUDED.record_date_day_convention, isin_detailed_info.record_date_day_convention),
                        redemption_payment_day_convention = COALESCE(EXCLUDED.redemption_payment_day_convention, isin_detailed_info.redemption_payment_day_convention),
                        reset_details = COALESCE(EXCLUDED.reset_details, isin_detailed_info.reset_details),
                        transferable = COALESCE(EXCLUDED.transferable, isin_detailed_info.transferable),
                        greenshoe_option = COALESCE(EXCLUDED.greenshoe_option, isin_detailed_info.greenshoe_option),
                        oversubscription_multiple = COALESCE(EXCLUDED.oversubscription_multiple, isin_detailed_info.oversubscription_multiple),
                        percentage_sold_cumulative = COALESCE(EXCLUDED.percentage_sold_cumulative, isin_detailed_info.percentage_sold_cumulative),
                        data_hash = COALESCE(EXCLUDED.data_hash, isin_detailed_info.data_hash),
                        last_updated = NOW()
                ;
                """, tuple(detail_data.values()))
                # Verify insert
                cur.execute("SELECT COUNT(*) FROM isin_detailed_info WHERE isin_code = %s", (isin,))
                count = cur.fetchone()[0]
                if count > 0:
                    logging.info(f"ISIN {isin} inserted/updated in isin_detailed_info successfully")
                else:
                    logging.error(f"ISIN {isin} failed to insert/update in isin_detailed_info")
                    log_migration(pg_conn, isin, 'isin_detailed_info', "Failed to insert/update: no rows affected")
            else:
                logging.warning(f"ISIN {isin} skipped isin_detailed_info: no doc_detail")
                log_migration(pg_conn, isin, 'isin_detailed_info', "Skipped: no doc_detail")
            pg_conn.commit()
        except Exception as e:
            logging.error(f"Error in isin_detailed_info for ISIN {isin}: {str(e)}")
            pg_conn.rollback()
            log_migration(pg_conn, isin, 'isin_detailed_info', f"detailed upsert error: {str(e)}", input_data=doc_detail)

        # ------------------------
        # 5. RTA Info (normalized)
        # ------------------------
        try:
            rta_id = None
            if doc_rta:
                rta_name = clean_string(doc_rta.get("RTA_NAME"))
                if rta_name:
                    rta_data = {
                        "rta_name": rta_name,
                        "rta_bp_id": clean_string(doc_rta.get("RTA_BP_ID")),
                        "rta_address": clean_string(doc_rta.get("RTA_ADDRESS")),
                        "rta_contact_person": clean_string(doc_rta.get("RTA_CONTACT_PERSON")),
                        "rta_phone": clean_string(doc_rta.get("RTA_PHONE")),
                        "rta_fax": clean_string(doc_rta.get("RTA_FAX")),
                        "rta_email": clean_string(doc_rta.get("RTA_EMAIL")),
                        "arrangers": clean_string(doc_rta.get("ARRANGERS")),
                        "trustee": clean_string(doc_rta.get("TRUSTEE")),
                        "im_term_sheet": clean_string(doc_rta.get("IM_TERM_SHEET")),
                        "data_hash": clean_string(doc_rta.get("DATA_HASH"))
                    }
                    if DEBUG_MODE:
                        logging.debug(f"ISIN {isin} rta_info: {rta_data}")
                    cur.execute("""
                        INSERT INTO rta_info (rta_name, rta_bp_id, rta_address, rta_contact_person,
                                              rta_phone, rta_fax, rta_email, arrangers, trustee, im_term_sheet, data_hash)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (rta_name) DO UPDATE
                        SET rta_bp_id = COALESCE(EXCLUDED.rta_bp_id, rta_info.rta_bp_id),
                            rta_address = COALESCE(EXCLUDED.rta_address, rta_info.rta_address),
                            rta_contact_person = COALESCE(EXCLUDED.rta_contact_person, rta_info.rta_contact_person),
                            rta_phone = COALESCE(EXCLUDED.rta_phone, rta_info.rta_phone),
                            rta_fax = COALESCE(EXCLUDED.rta_fax, rta_info.rta_fax),
                            rta_email = COALESCE(EXCLUDED.rta_email, rta_info.rta_email),
                            arrangers = COALESCE(EXCLUDED.arrangers, rta_info.arrangers),
                            trustee = COALESCE(EXCLUDED.trustee, rta_info.trustee),
                            im_term_sheet = COALESCE(EXCLUDED.im_term_sheet, rta_info.im_term_sheet),
                            data_hash = COALESCE(EXCLUDED.data_hash, rta_info.data_hash),
                            last_updated = NOW()
                        RETURNING rta_id;
                    """, tuple(rta_data.values()))
                    row = cur.fetchone()
                    rta_id = row[0] if row else None
                    if rta_id:
                        cur.execute("""
                            INSERT INTO isin_rta_map (isin_code, rta_id, effective_from)
                            VALUES (%s, %s, CURRENT_DATE)
                            ON CONFLICT (isin_code, rta_id, effective_from) DO NOTHING;
                        """, (isin, rta_id))
                        if DEBUG_MODE:
                            logging.debug(f"ISIN {isin} inserted into isin_rta_map with rta_id={rta_id}")
                        # Verify insert
                        cur.execute("SELECT COUNT(*) FROM isin_rta_map WHERE isin_code = %s AND rta_id = %s",
                                    (isin, rta_id))
                        map_count = cur.fetchone()[0]
                        if map_count > 0:
                            logging.info(f"ISIN {isin} inserted/updated in isin_rta_map successfully")
                        else:
                            logging.error(f"ISIN {isin} failed to insert/update in isin_rta_map")
                            log_migration(pg_conn, isin, 'isin_rta_map', "Failed to insert/update: no rows affected")
                    else:
                        logging.warning(f"ISIN {isin} failed to get rta_id for isin_rta_map")
                        log_migration(pg_conn, isin, 'isin_rta_map', "Failed to get rta_id", input_data=rta_data)
                else:
                    logging.warning(f"ISIN {isin} skipped rta_info: no rta_name")
                    log_migration(pg_conn, isin, 'rta_info', "Skipped: no rta_name", input_data=doc_rta)
            else:
                logging.warning(f"ISIN {isin} skipped rta_info: no doc_rta")
                log_migration(pg_conn, isin, 'rta_info', "Skipped: no doc_rta")
            pg_conn.commit()
        except Exception as e:
            logging.error(f"Error in rta_info for ISIN {isin}: {str(e)}")
            pg_conn.rollback()
            log_migration(pg_conn, isin, 'rta_info', f"RTA insert/update error: {str(e)}", input_data=doc_rta)

        # ------------------------
        # 6. Rating Info (normalized, fallback to basic)
        # ------------------------
        try:
            rating_doc = doc_rating if doc_rating else doc_basic
            if rating_doc:
                credit_rating = clean_string(rating_doc.get("CREDIT_RATING"))
                rating_agency = clean_string(rating_doc.get("RATING_AGENCY"))
                data_hash = clean_string(rating_doc.get("DATA_HASH"))
                rating_data = {
                    "credit_rating": credit_rating,
                    "rating_agency": rating_agency,
                    "data_hash": data_hash
                }
                if DEBUG_MODE:
                    logging.debug(f"ISIN {isin} isin_credit_ratings: {rating_data}")
                if credit_rating and rating_agency:
                    cur.execute("""
                        INSERT INTO isin_credit_ratings (isin_code, rating_agency, credit_rating, outlook, rating_date, data_hash)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (isin_code, rating_agency, credit_rating, rating_date) DO UPDATE
                        SET outlook = COALESCE(EXCLUDED.outlook, isin_credit_ratings.outlook),
                            data_hash = COALESCE(EXCLUDED.data_hash, isin_credit_ratings.data_hash);
                    """, (
                        isin,
                        rating_agency,
                        credit_rating,
                        None,
                        None,
                        data_hash
                    ))
                    # Verify insert
                    cur.execute("SELECT COUNT(*) FROM isin_credit_ratings WHERE isin_code = %s AND rating_agency = %s AND credit_rating = %s",
                                (isin, rating_agency, credit_rating))
                    count = cur.fetchone()[0]
                    if count > 0:
                        logging.info(f"ISIN {isin} inserted/updated in isin_credit_ratings successfully")
                    else:
                        logging.error(f"ISIN {isin} failed to insert/update in isin_credit_ratings")
                        log_migration(pg_conn, isin, 'isin_credit_ratings', "Failed to insert/update: no rows affected")
                else:
                    logging.warning(f"ISIN {isin} skipped isin_credit_ratings: missing credit_rating={credit_rating} or rating_agency={rating_agency}")
                    log_migration(pg_conn, isin, 'isin_credit_ratings', f"Skipped: missing credit_rating or rating_agency", input_data=rating_data)
            else:
                logging.warning(f"ISIN {isin} skipped isin_credit_ratings: no rating_doc or doc_basic")
                log_migration(pg_conn, isin, 'isin_credit_ratings', "Skipped: no rating_doc or doc_basic")
            pg_conn.commit()
        except Exception as e:
            logging.error(f"Error in isin_credit_ratings for ISIN {isin}: {str(e)}")
            pg_conn.rollback()
            log_migration(pg_conn, isin, 'isin_credit_ratings', f"Rating insert/update error: {str(e)}", input_data=rating_doc)

    except Exception as ex:
        logging.exception(f"Unexpected ETL error for ISIN {isin}: {str(ex)}")
        pg_conn.rollback()
        log_migration(pg_conn, isin, 'etl', f"unexpected error: {str(ex)}")
    finally:
        cur.close()

def run_etl(limit=None):
    mongo = MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]
    pg_conn = psycopg2.connect(PG_DSN)

    isin_set = set()
    for col in ['isin_basic_info', 'isin_detailed_info', 'isin_company_info', 'isin_rta_info', 'isin_rating_info']:
        if col in db.list_collection_names():
            try:
                cursor = db[col].find({}, {"ISIN_CODE": 1})
                for d in cursor:
                    code = d.get("ISIN_CODE")
                    if code:
                        isin_set.add(code)
            except Exception as e:
                logging.warning(f"Could not enumerate collection {col}: {e}")

    logging.info(f"Found {len(isin_set)} unique ISINs to process")

    for count, isin in enumerate(sorted(isin_set), 1):
        upsert_isin_and_related(pg_conn, db, isin)
        if count % 100 == 0:
            logging.info(f"Processed {count} ISINs")
        if limit and count >= limit:
            logging.info(f"Stopping after processing {count} ISINs (test mode)")
            break

    pg_conn.close()
    mongo.close()

if __name__ == "__main__":
    run_etl(limit=2)  # ✅ test limit