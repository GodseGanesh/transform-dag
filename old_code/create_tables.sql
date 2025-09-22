
-- ===============================
-- Main ISIN Basic Information
-- ===============================
CREATE TABLE IF NOT EXISTS isin_basic_info (
    isin_code VARCHAR(12) PRIMARY KEY,
    security_type VARCHAR(100),
    isin_description TEXT,
    issue_description TEXT,
    former_name VARCHAR(255),
    coupon_rate_percent DECIMAL(6,3),
    maturity_date DATE,
    ytm_percent DECIMAL(6,3),
    tenure_years INTEGER,
    tenure_months INTEGER,
    tenure_days INTEGER,
    minimum_investment_rs DECIMAL(18,2),
    interest_payment_frequency_raw TEXT,
    interest_payment_frequency VARCHAR(50),
    face_value_rs DECIMAL(18,2),
    paid_up_value_rs DECIMAL(18,2),
    percentage_sold DECIMAL(6,3),
    isin_status VARCHAR(50),
    issue_size_lakhs DECIMAL(18,2),
    issue_date DATE,
    closing_date DATE,
    first_interest_payment_date DATE,
    mode_of_issuance VARCHAR(100),
    series VARCHAR(100),

    -- Arrays for credit ratings & agencies
    credit_ratings TEXT[],        
    rating_agencies TEXT[],       

    data_hash CHAR(64),
    record_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================
-- Detailed ISIN Information
-- ===============================
CREATE TABLE IF NOT EXISTS isin_detailed_info (
    isin_code VARCHAR(12) PRIMARY KEY REFERENCES isin_basic_info(isin_code) ON DELETE CASCADE,

    -- Issuance & listing details
    allotment_date DATE,
    opening_date DATE,
    listing_date DATE,
    bse_date_of_listing DATE,
    nse_date_of_listing DATE,
    bse_scrip_code VARCHAR(50),
    nse_symbol VARCHAR(50),
    listed_unlisted VARCHAR(50),
    listing_exchanges VARCHAR(255),
    primary_exchange VARCHAR(50),
    secondary_exchange VARCHAR(50),

    -- Bond structure
    coupon_type VARCHAR(100),
    day_count_convention VARCHAR(100),
    compounding_frequency VARCHAR(100),
    interest_payment_dates TEXT,
    interest_payment_day_convention VARCHAR(100),
    payment_schedule TEXT,
    redemption TEXT,
    redemption_premium VARCHAR(255),
    redemption_payment_day_convention VARCHAR(255),
    call_option BOOLEAN,
    call_option_date DATE,
    call_notification_period VARCHAR(255),
    put_option BOOLEAN,
    put_option_date DATE,
    put_notification_period VARCHAR(255),
    buyback_option VARCHAR(100),
    call_notification BOOLEAN,
    secured BOOLEAN,
    security_collateral TEXT,
    seniority VARCHAR(255),
    lock_in_period VARCHAR(100),
    transferable BOOLEAN,

    -- Regulatory/structural
    tax_category VARCHAR(50),
    benefit_under_section VARCHAR(255),
    basel_compliant BOOLEAN,
    use_of_proceeds TEXT,
    pricing_method TEXT,

    -- Market data
    trading_status VARCHAR(100),
    market_lot BIGINT,
    settlement_cycle VARCHAR(100),
    last_traded_price_rs DECIMAL(18,4),
    last_traded_date DATE,
    volume_traded BIGINT,
    value_traded_lakhs DECIMAL(18,4),
    number_of_trades BIGINT,
    weighted_avg_price_rs DECIMAL(18,4),
    weighted_avg_yield_percent DECIMAL(6,3),
    current_yield_percent DECIMAL(6,3),

    -- Risk/yield measures
    duration_years DECIMAL(8,4),
    convexity DECIMAL(20,8),

    -- Operational
    demat_requests_pending BIGINT,
    services_stopped BOOLEAN,
    no_of_bonds_ncd BIGINT,
    greenshoe_option BOOLEAN,
    oversubscription_multiple DECIMAL(18,6),
    percentage_sold_cumulative DECIMAL(6,3),
    record_date_day_convention VARCHAR(255),
    reset_details TEXT,
    liquidation_status VARCHAR(255),

    -- Derived
    due_for_maturity INT,   -- # of days or years until maturity (ETL can decide granularity)

    data_hash CHAR(64),
    record_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS company_info (
    company_id SERIAL PRIMARY KEY,
    issuer_name VARCHAR(255) UNIQUE,
    issuer_address TEXT,
    issuer_type VARCHAR(50),
    issuer_state VARCHAR(100),
    issuer_website VARCHAR(255),
    contact_person VARCHAR(100),
    phone_number VARCHAR(255),
    fax_number VARCHAR(50),
    email_id TEXT,
    guaranteed_by TEXT,
    registrar VARCHAR(255),
    industry_group VARCHAR(100),
    macro_sector VARCHAR(100),
    micro_industry VARCHAR(100),
    product_service_activity TEXT,
    sector VARCHAR(100),
    security_code VARCHAR(50),
    data_hash CHAR(64),
    record_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS isin_company_map (
    isin_code VARCHAR(12) REFERENCES isin_basic_info(isin_code) ON DELETE CASCADE,
    company_id INT REFERENCES company_info(company_id) ON DELETE CASCADE,
    primary_company BOOLEAN DEFAULT TRUE,
    mapped_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (isin_code, company_id)
);

CREATE TABLE IF NOT EXISTS rta_info (
    rta_id SERIAL PRIMARY KEY,
    rta_name VARCHAR(255) UNIQUE,
    rta_bp_id VARCHAR(50),
    rta_address TEXT,
    rta_contact_person VARCHAR(100),
    rta_phone VARCHAR(255),
    rta_fax VARCHAR(50),
    rta_email TEXT,
    trustee VARCHAR(255),
    arrangers TEXT,
    im_term_sheet VARCHAR(500),
    data_hash CHAR(64),
    record_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS isin_rta_map (
    isin_code VARCHAR(12) REFERENCES isin_basic_info(isin_code) ON DELETE CASCADE,
    rta_id INT REFERENCES rta_info(rta_id) ON DELETE CASCADE,
    effective_from DATE DEFAULT CURRENT_DATE,
    effective_to DATE,
    mapped_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (isin_code, rta_id, effective_from)
);
