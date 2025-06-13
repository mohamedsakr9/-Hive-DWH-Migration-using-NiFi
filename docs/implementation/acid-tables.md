```sql
-- Enable ACID transaction support
SET hive.support.concurrency=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;   --to create tables in acid check it 
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;


-- 1. Create non-ACID fact table with denormalized dimension attributes
CREATE  TABLE reservation_fact (
    reservation_id BIGINT,
    passenger_id BIGINT,
    airport_code STRING,
    airplane_id STRING,
    date_id INT,
    promotion_id INT,
    revenue DECIMAL(10,2),
    profit DECIMAL(10,2),
    payment_method STRING,
    reservation_channel STRING,
    fare_basis STRING,
    airport_fees DECIMAL(10,2),
    tax_fees DECIMAL(10,2),
    fuel_cost DECIMAL(10,2),
    crew_fees DECIMAL(10,2),
    airport_name STRING,
    city STRING,
    country STRING,
    day_of_week STRING,
    calendar_quarter INT,
    promotion_key STRING,
    name STRING,
    category STRING,
    assigned_tier STRING,
    model STRING,
    manufacturer STRING
    
)
PARTITIONED BY (year INT, month STRING)
CLUSTERED BY (passenger_id) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION '/user/hive/warehouse/reservation_fact';

-- 2. Create ACID dimension tables that support UPDATE and DELETE
-- Only dim_passenger keeps bucketing (high join likelihood, large dimension)

-- Passenger dimension with ACID support and bucketing
CREATE TABLE dim_passenger (
    passenger_id INT,
    passenger_key INT,
    age_category STRING,
    age INT,
    gender STRING,
    occupation STRING,
    nationality STRING,
    city STRING,
    country STRING,
    start_date STRING,
    end_date STRING,
    created_at STRING,
    last_updated STRING
)
PARTITIONED BY (membership_status STRING, loyalty_tier STRING)
CLUSTERED BY (passenger_id) INTO 32 BUCKETS  -- Keep bucketing, same count as fact
STORED AS ORC
TBLPROPERTIES ("transactional"="true");

-- Airport dimension with ACID support 
CREATE TABLE dim_airport (
    airport_code STRING,
    airport_name STRING,
    city STRING,
    country STRING
)
STORED AS ORC
TBLPROPERTIES ("transactional"="true");

-- Date dimension with ACID support
CREATE TABLE dim_date (
    date_id INT,
    `date` STRING,
    full_date_description STRING,
    day_of_week STRING,
    day_number_in_calendar_month INT,
    calendar_week_number_in_year INT,
    calendar_month_name STRING,
    calendar_month_number_in_year INT,
    calendar_quarter INT,
    calendar_year_quarter STRING,
    day_number_in_fiscal_month INT,
    last_day_in_fiscal_month_indicator BOOLEAN,
    fiscal_month STRING,
    fiscal_month_number_in_year INT
)
PARTITIONED BY (calendar_year INT)
STORED AS ORC
LOCATION '/user/hive/warehouse/dim_date';

-- Promotion dimension with ACID support
CREATE TABLE dim_promotion (
    promotion_id BIGINT,
    promotion_key STRING,
    description STRING,
    name STRING,
    assigned_tier STRING
)
PARTITIONED BY   (category STRING)
STORED AS ORC
TBLPROPERTIES ("transactional"="true");

-- Airplane dimension with ACID support 
CREATE TABLE dim_airplane (
    airplane_id STRING,
    model STRING,
    capacity INT,
    business_seats INT,
    economy_seats INT,
    first_class_seats INT,
    max_range_km INT,
    max_speed_kmh INT,
    fuel_capacity INT,
    number_of_engines INT,
    engine_type STRING,
    fuel_consumption Decimal(10,2),
    activity_status STRING
)
PARTITIONED BY (manufacturer STRING)
STORED AS ORC
TBLPROPERTIES ("transactional"="true");

-- 3. Load data from staging tables to dimension tables with ACID support

-- Load passenger dimension
INSERT INTO TABLE dim_passenger
PARTITION(membership_status, loyalty_tier)
SELECT 
    passenger_id,
    passenger_key,
    age_category,
    age,
    gender,
    occupation,
    nationality,
    city,
    country,
    start_date,
    end_date,
    created_at,
    created_at AS last_updated,  
    membership_status,
    loyalty_tier
FROM staging_passenger_dim;

-- Load airport dimension
INSERT INTO TABLE dim_airport
SELECT 
    airport_code,
    airport_name,
    city,
    country
FROM staging_airport_dim;

-- Load date dimension
INSERT INTO TABLE dim_date
PARTITION(calendar_year)
SELECT 
    date_id,
    `date`,
    full_date_description,
    day_of_week,
    day_number_in_calendar_month,
    calendar_week_number_in_year,
    calendar_month_name,
    calendar_month_number_in_year,
    calendar_quarter,
    calendar_year_quarter,
    day_number_in_fiscal_month,
    last_day_in_fiscal_month_indicator,
    fiscal_month,
    fiscal_month_number_in_year,
    calendar_year
FROM staging_date_dim;

-- Load promotion dimension
INSERT INTO TABLE dim_promotion
SELECT 
    promotion_id,
    promotion_key,
    description,
    name,
    assigned_tier,
    category
FROM staging_dim_promotion;

-- Load airplane dimension
INSERT INTO TABLE dim_airplane
SELECT 
    airplane_id,
    model,
    capacity,
    business_seats,
    economy_seats,
    first_class_seats,
    max_range_km,
    max_speed_kmh,
    fuel_capacity,
    number_of_engines,
    engine_type,
    fuel_consumption,
    activity_status,
        manufacturer
FROM staging_dim_airplane;

-- 4. Load denormalized fact table from staging and dimension tables
CREATE VIEW temp_reservation_denormalized AS
SELECT 
    -- Main fact fields
    f.reservation_id,
    f.passenger_id,
    f.airport_code,
    f.airplane_id,
    f.date_id,
    f.promotion_id,
    f.revenue,
    f.profit,
    f.payment_method,
    f.reservation_channel,
    f.fare_basis,
    f.airport_fees,
    f.taxes,
    f.fuel_cost,
    f.crew_fees,
    
    -- Airport attributes
    a.airport_name,
    a.city,
    a.country,
    
    -- Date attributes
    d.day_of_week,
    d.calendar_quarter,

    -- Promotion attributes
    p.promotion_key,
    p.name,
    p.category,
    p.assigned_tier,
    
    -- Airplane attributes
    ap.model,
    ap.manufacturer,
    -- Partition keys
    d.calendar_year  as year,
    d.calendar_month_name as month 

FROM staging_reservation_fact f
LEFT JOIN staging_date_dim d ON f.date_id = d.date_id
LEFT JOIN staging_airport_dim a ON f.airport_code = a.airport_code

LEFT JOIN staging_dim_promotion p ON f.promotion_id = p.promotion_id
LEFT JOIN staging_dim_airplane ap ON f.airplane_id = ap.airplane_id
LEFT JOIN staging_passenger_dim ps ON f.passenger_id = ps.passenger_id;

-- Load the denormalized data into the fact table
INSERT INTO TABLE reservation_fact
PARTITION (year, month)
SELECT 
    reservation_id,
    passenger_id,
    airport_code,
    airplane_id,
    date_id,
    promotion_id,
    revenue,
    profit,
    payment_method,
    reservation_channel,
    fare_basis,
    airport_fees,
    taxes,
    fuel_cost,
    crew_fees,
    airport_name,
    city,
    country,
    day_of_week,
   
    calendar_quarter,
   
    promotion_key,
    name,
    category,
    assigned_tier,
    model,
    manufacturer,
    year,
    month
FROM temp_reservation_denormalized;

-- Drop the temporary view
DROP VIEW temp_reservation_denormalized;



