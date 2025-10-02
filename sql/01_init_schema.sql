-- ============================================
-- NYC Taxi Analytics Database Schema
-- ============================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================
-- REFERENCE/DIMENSION TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS warehouse.dim_vendor (
    vendor_key SERIAL PRIMARY KEY,
    vendor_id INT UNIQUE NOT NULL,
    vendor_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_rate_code (
    rate_code_key SERIAL PRIMARY KEY,
    rate_code_id INT UNIQUE NOT NULL,
    rate_code_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_payment_type (
    payment_type_key SERIAL PRIMARY KEY,
    payment_type_id INT UNIQUE NOT NULL,
    payment_type_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- STAGING TABLE (raw structure from CSV)
-- ============================================

CREATE TABLE IF NOT EXISTS staging.trip_raw (
    -- Raw columns (all TEXT for initial landing)
    vendorid TEXT,
    tpep_pickup_datetime TEXT,
    tpep_dropoff_datetime TEXT,
    passenger_count TEXT,
    trip_distance TEXT,
    ratecodeid TEXT,
    store_and_fwd_flag TEXT,
    pulocationid TEXT,
    dolocationid TEXT,
    payment_type TEXT,
    fare_amount TEXT,
    extra TEXT,
    mta_tax TEXT,
    tip_amount TEXT,
    tolls_amount TEXT,
    improvement_surcharge TEXT,
    total_amount TEXT,
    congestion_surcharge TEXT,
    airport_fee TEXT,
    
    -- Metadata
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- WAREHOUSE FACT TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS warehouse.fact_trip (
    trip_key BIGSERIAL PRIMARY KEY,
    
    -- Foreign keys
    vendor_key INT REFERENCES warehouse.dim_vendor(vendor_key),
    rate_code_key INT REFERENCES warehouse.dim_rate_code(rate_code_key),
    payment_type_key INT REFERENCES warehouse.dim_payment_type(payment_type_key),
    
    -- Trip details
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pickup_location_id INT,
    dropoff_location_id INT,
    
    -- Metrics
    passenger_count SMALLINT CHECK (passenger_count BETWEEN 0 AND 9),
    trip_distance NUMERIC(8,2) CHECK (trip_distance >= 0),
    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    
    -- Derived metrics
    trip_duration_minutes INT,
    trip_date DATE NOT NULL,
    
    -- Metadata
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW(),
    
    -- Natural key constraint (prevent duplicates)
    CONSTRAINT unique_trip UNIQUE (vendor_key, pickup_datetime, dropoff_datetime, pickup_location_id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_trip_pickup_dt ON warehouse.fact_trip(pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_fact_trip_trip_date ON warehouse.fact_trip(trip_date);
CREATE INDEX IF NOT EXISTS idx_fact_trip_vendor_date ON warehouse.fact_trip(vendor_key, trip_date);

-- ============================================
-- AUDIT TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    run_id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    pipeline_stage TEXT NOT NULL CHECK (pipeline_stage IN ('extract', 'validate', 'transform', 'load')),
    source_file TEXT,
    rows_processed INT DEFAULT 0,
    rows_passed INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    status TEXT CHECK (status IN ('success', 'failure', 'partial')) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    duration_seconds INT GENERATED ALWAYS AS 
        (EXTRACT(EPOCH FROM (completed_at - started_at))::INT) STORED
);

CREATE INDEX IF NOT EXISTS idx_audit_run_date ON audit.pipeline_run(run_date, pipeline_stage);

-- ============================================
-- VIEWS FOR ANALYTICS
-- ============================================

CREATE OR REPLACE VIEW warehouse.daily_trip_summary AS
SELECT 
    trip_date,
    v.vendor_name,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(fare_amount) as avg_fare,
    SUM(passenger_count) as total_passengers
FROM warehouse.fact_trip f
JOIN warehouse.dim_vendor v ON f.vendor_key = v.vendor_key
GROUP BY trip_date, v.vendor_name
ORDER BY trip_date DESC;-- ============================================
-- NYC Taxi Analytics Database Schema
-- ============================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================
-- REFERENCE/DIMENSION TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS warehouse.dim_vendor (
    vendor_key SERIAL PRIMARY KEY,
    vendor_id INT UNIQUE NOT NULL,
    vendor_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_rate_code (
    rate_code_key SERIAL PRIMARY KEY,
    rate_code_id INT UNIQUE NOT NULL,
    rate_code_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_payment_type (
    payment_type_key SERIAL PRIMARY KEY,
    payment_type_id INT UNIQUE NOT NULL,
    payment_type_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- STAGING TABLE (raw structure from CSV)
-- ============================================

CREATE TABLE IF NOT EXISTS staging.trip_raw (
    -- Raw columns (all TEXT for initial landing)
    vendorid TEXT,
    tpep_pickup_datetime TEXT,
    tpep_dropoff_datetime TEXT,
    passenger_count TEXT,
    trip_distance TEXT,
    ratecodeid TEXT,
    store_and_fwd_flag TEXT,
    pulocationid TEXT,
    dolocationid TEXT,
    payment_type TEXT,
    fare_amount TEXT,
    extra TEXT,
    mta_tax TEXT,
    tip_amount TEXT,
    tolls_amount TEXT,
    improvement_surcharge TEXT,
    total_amount TEXT,
    congestion_surcharge TEXT,
    airport_fee TEXT,
    
    -- Metadata
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- WAREHOUSE FACT TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS warehouse.fact_trip (
    trip_key BIGSERIAL PRIMARY KEY,
    
    -- Foreign keys
    vendor_key INT REFERENCES warehouse.dim_vendor(vendor_key),
    rate_code_key INT REFERENCES warehouse.dim_rate_code(rate_code_key),
    payment_type_key INT REFERENCES warehouse.dim_payment_type(payment_type_key),
    
    -- Trip details
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pickup_location_id INT,
    dropoff_location_id INT,
    
    -- Metrics
    passenger_count SMALLINT CHECK (passenger_count BETWEEN 0 AND 9),
    trip_distance NUMERIC(8,2) CHECK (trip_distance >= 0),
    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    
    -- Derived metrics
    trip_duration_minutes INT,
    trip_date DATE NOT NULL,
    
    -- Metadata
    source_file TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW(),
    
    -- Natural key constraint (prevent duplicates)
    CONSTRAINT unique_trip UNIQUE (vendor_key, pickup_datetime, dropoff_datetime, pickup_location_id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_trip_pickup_dt ON warehouse.fact_trip(pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_fact_trip_trip_date ON warehouse.fact_trip(trip_date);
CREATE INDEX IF NOT EXISTS idx_fact_trip_vendor_date ON warehouse.fact_trip(vendor_key, trip_date);

-- ============================================
-- AUDIT TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    run_id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    pipeline_stage TEXT NOT NULL CHECK (pipeline_stage IN ('extract', 'validate', 'transform', 'load')),
    source_file TEXT,
    rows_processed INT DEFAULT 0,
    rows_passed INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    status TEXT CHECK (status IN ('success', 'failure', 'partial')) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    duration_seconds INT GENERATED ALWAYS AS 
        (EXTRACT(EPOCH FROM (completed_at - started_at))::INT) STORED
);

CREATE INDEX IF NOT EXISTS idx_audit_run_date ON audit.pipeline_run(run_date, pipeline_stage);

-- ============================================
-- VIEWS FOR ANALYTICS
-- ============================================

CREATE OR REPLACE VIEW warehouse.daily_trip_summary AS
SELECT 
    trip_date,
    v.vendor_name,
    COUNT(*) as total_trips,
    SUM(total_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(fare_amount) as avg_fare,
    SUM(passenger_count) as total_passengers
FROM warehouse.fact_trip f
JOIN warehouse.dim_vendor v ON f.vendor_key = v.vendor_key
GROUP BY trip_date, v.vendor_name
ORDER BY trip_date DESC;
