```sql
-- Set Hive configuration for optimal performance
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET parquet.compression=SNAPPY;
SET hive.execution.engine=tez;
SET hive.enforce.bucketing=true;
SET hive.enforce.sorting=true;
SET hive.auto.convert.join=true;

-- 1. Create optimized fact table with denormalized dimension attributes
-- Only includes existing columns from the original schema
CREATE EXTERNAL TABLE reservation_fact (
    reservation_id BIGINT,
    passenger_id BIGINT,
    airport_code STRING,
    airplane_id STRING,
    date_id INT,
    promotion_id INT,
    revenue STRING,
    profit STRING,
    payment_method STRING,
    reservation_channel STRING,
    fare_basis STRING,
    airport_fees STRING,
    taxes STRING,
    fuel_cost STRING,
    crew_fees STRING,
    
    -- Denormalized from airport_dim (existing columns only)
    airport_name STRING,
    city STRING,
    country STRING,
    
    -- Denormalized from date_dim (existing columns only)
    full_date_description STRING,
    day_of_week STRING,
    calendar_month_name STRING,
    calendar_quarter INT,
    fiscal_month STRING,
    
    -- Denormalized from promotion_dim (existing columns only)
    promotion_key STRING,
    description STRING,
    name STRING,
    category STRING,
    assigned_tier STRING,
    
    -- Denormalized from airplane_dim (existing columns only)
    model STRING,
    manufacturer STRING,
    
    -- Denormalized from passenger_dim (existing columns only)
    passenger_key INT,
    age_category STRING,
    age INT,
    gender STRING,
    occupation STRING,
    nationality STRING,
    membership_status STRING,
    loyalty_tier STRING
)
PARTITIONED BY (year INT, month INT)
CLUSTERED BY (passenger_id) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION '/user/hive/warehouse/reservation_fact';

-- 2. Create slim dimension tables with only original columns
-- No new columns are added to any of these tables

-- Passenger dimension with original columns
CREATE EXTERNAL TABLE dim_passenger (
    passenger_id INT,
    passenger_key INT,
    age_category STRING,
    age INT,
    gender STRING,
    occupation STRING,
    nationality STRING,
    membership_status STRING, 
    loyalty_tier STRING,
    city STRING,
    country STRING,
    start_date STRING,
    end_date STRING,
    created_at STRING
)
PARTITIONED BY (membership_status STRING, loyalty_tier STRING)
CLUSTERED BY (passenger_id) INTO 32 BUCKETS  -- Same bucket count as fact table
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dim_passenger';

-- Airport dimension with original columns
CREATE EXTERNAL TABLE dim_airport (
    airport_code STRING,
    airport_name STRING,
    city STRING,
    country STRING
)
PARTITIONED BY (country STRING)
CLUSTERED BY (airport_code) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dim_airport';

-- Date dimension with original columns
CREATE EXTERNAL TABLE dim_date (
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
    calendar_year INT,
    day_number_in_fiscal_month INT,
    last_day_in_fiscal_month_indicator BOOLEAN,
    fiscal_month STRING,
    fiscal_month_number_in_year INT
)
PARTITIONED BY (calendar_year INT)
CLUSTERED BY (date_id) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dim_date';

-- Promotion dimension with original columns
CREATE EXTERNAL TABLE dim_promotion (
    promotion_id BIGINT,
    promotion_key STRING,
    description STRING,
    name STRING,
    category STRING,
    assigned_tier STRING
)
PARTITIONED BY (category STRING)
CLUSTERED BY (promotion_id) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dim_promotion';

-- Airplane dimension with original columns
CREATE EXTERNAL TABLE dim_airplane (
    airplane_id STRING,
    model STRING,
    capacity INT,
    manufacturer STRING,
    business_seats INT,
    economy_seats INT,
    first_class_seats INT,
    max_range_km INT,
    max_speed_kmh INT,
    fuel_capacity INT,
    number_of_engines INT,
    engine_type STRING,
    fuel_consumption STRING,
    activity_status STRING
)
PARTITIONED BY (manufacturer STRING)
CLUSTERED BY (airplane_id) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dim_airplane';

-- 3. Load data from staging tables to dimension tables with partitioning

-- Load passenger dimension
INSERT OVERWRITE TABLE dim_passenger
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
    membership_status,
    loyalty_tier
FROM staging_dim_passenger;

-- Load airport dimension
INSERT OVERWRITE TABLE dim_airport
PARTITION(country)
SELECT 
    airport_code,
    airport_name,
    city,
    country,
    country
FROM staging_dim_airport;

-- Load date dimension
INSERT OVERWRITE TABLE dim_date
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
FROM staging_dim_date;

-- Load promotion dimension
INSERT OVERWRITE TABLE dim_promotion
PARTITION(category)
SELECT 
    promotion_id,
    promotion_key,
    description,
    name,
    assigned_tier,
    category
FROM staging_dim_promotion;

-- Load airplane dimension
INSERT OVERWRITE TABLE dim_airplane
PARTITION(manufacturer)
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
-- First create a temporary view to simplify the complex join
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
    d.full_date_description,
    d.day_of_week,
    d.calendar_month_name,
    d.calendar_quarter,
    d.fiscal_month,
    
    -- Promotion attributes
    p.promotion_key,
    p.description,
    p.name,
    p.category,
    p.assigned_tier,
    
    -- Airplane attributes
    ap.model,
    ap.manufacturer,
    
    -- Passenger attributes
    ps.passenger_key,
    ps.age_category,
    ps.age,
    ps.gender,
    ps.occupation,
    ps.nationality,
    ps.membership_status,
    ps.loyalty_tier,
    
    -- Partition keys
    d.calendar_year AS year,
    d.calendar_month_number_in_year AS month
FROM staging_reservation_fact f
LEFT JOIN staging_dim_airport a ON f.airport_code = a.airport_code
LEFT JOIN staging_dim_date d ON f.date_id = d.date_id
LEFT JOIN staging_dim_promotion p ON f.promotion_id = p.promotion_id
LEFT JOIN staging_dim_airplane ap ON f.airplane_id = ap.airplane_id
LEFT JOIN staging_dim_passenger ps ON f.passenger_id = ps.passenger_id;

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
    full_date_description,
    day_of_week,
    calendar_month_name,
    calendar_quarter,
    fiscal_month,
    promotion_key,
    description,
    name,
    category,
    assigned_tier,
    model,
    manufacturer,
    passenger_key,
    age_category,
    age,
    gender,
    occupation,
    nationality,
    membership_status,
    loyalty_tier,
    year,
    month
FROM temp_reservation_denormalized;

-- Drop the temporary view
DROP VIEW temp_reservation_denormalized;




--Handeling Updates 
--trial 1 
CREATE TABLE dim_passenger_temp AS
SELECT 
    passenger_id,
    passenger_key,
    age_category,
    CASE 
        WHEN passenger_id = 134 THEN 35 
        ELSE age  
    END as age,
    gender,
    occupation,
    nationality,
    city,
    country,
    start_date,
    end_date,
    created_at,
    CAST(CURRENT_TIMESTAMP AS STRING) as last_updated,
    membership_status,loyalty_tier
    
FROM dim_passenger;

INSERT OVERWRITE TABLE dim_passenger
SELECT * FROM dim_passenger_temp ;

--trial 2 
CREATE TABLE staging_table LIKE dim_passenger;
INSERT INTO staging_table
values(134,775221,'Young Adult',25,'Male','Doctor','Australian','Beijing','UK','2024-06-19','9999-12-31','2025-03-12',
'2025-05-07 11:27:19.251','Active','Silver');

INSERT OVERWRITE TABLE dim_passenger
SELECT s.* 
FROM staging_table s
UNION ALL
SELECT t.*
FROM dim_passenger t
WHERE NOT EXISTS (
    SELECT 1 FROM staging_table s 
    WHERE s.passenger_id = t.passenger_id
);


select * from dim_passenger where passenger_id=134;


--trial 3
INSERT OVERWRITE TABLE dim_passenger 
PARTITION(membership_status='Active', loyalty_tier='Silver')
SELECT 
    passenger_id,
    passenger_key,
    age_category,
    CASE WHEN passenger_id = 134 THEN 26 ELSE age END as age,
    gender,
    occupation,
    nationality,
    city,
    country,
    start_date,
    end_date,
    created_at,
    CAST(CURRENT_TIMESTAMP AS STRING) as last_updated
FROM dim_passenger
WHERE membership_status='Active' AND loyalty_tier='Silver';

--trial 4
CREATE TABLE staging_passenger (
    passenger_id INT,
    age INT
);
INSERT INTO staging_passenger VALUES (134, 22);
INSERT OVERWRITE TABLE dim_passenger
SELECT 
    t.passenger_id,
    t.passenger_key,
    t.age_category,
    COALESCE(s.age, t.age) as age,  
    t.gender,
    t.occupation,
    t.nationality,
    t.city,
    t.country,
    t.start_date,
    t.end_date,
    t.created_at,
    CASE WHEN s.passenger_id IS NOT NULL THEN CAST(CURRENT_TIMESTAMP AS STRING) ELSE t.last_updated END,
    t.membership_status,
    t.loyalty_tier
FROM 
    dim_passenger t
LEFT JOIN 
    staging_passenger s ON t.passenger_id = s.passenger_id;
    




--Handeling Deletes 

INSERT OVERWRITE TABLE dim_passenger
SELECT * FROM dim_passenger where passenger_id !=557;



-- Handeling Incremental Loading -- when not matched 
        CREATE EXTERNAL TABLE delta_dim_passenger
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
        LOCATION '/user/hive/staging/delta_passenger_dim'
        TBLPROPERTIES (
        'avro.schema.url'='hdfs:///user/hive/schemas/delta_passenger_dim_schema.avsc' 
        );

        insert into dim_passenger
        SELECT passenger_id,CAST((POW(passenger_id, 2) * 73) % 900000 + 100000 AS INT)  AS passenger_key,
            CASE
                WHEN FLOOR(DATEDIFF(current_date, dob) / 365) < 13 THEN 'Child'
                WHEN FLOOR(DATEDIFF(current_date, dob) / 365) BETWEEN 13 AND 19 THEN 'Young Adult'
                WHEN FLOOR(DATEDIFF(current_date, dob) / 365) BETWEEN 20 AND 39 THEN 'Adult'
                WHEN FLOOR(DATEDIFF(current_date, dob) / 365) BETWEEN 40 AND 59 THEN 'Middle-Aged'
                ELSE 'Senior'
            END AS age_category,
                FLOOR(DATEDIFF(current_date, dob) / 365) AS age,gender,occupation,nationality,city,country,current_date as start_date ,
                '9999-12-31' as end_date,
                created_at,modified_at as last_updated,membership_status,loyalty_tier
        FROM 
            delta_dim_passenger;


    -- incremental with updates  (SCD Type 2)
    -- Step 1: Create a staging table with the new/updated data with your transformations
CREATE TABLE dim_passenger_stage AS
SELECT 
    passenger_id,
    CAST((POW(passenger_id, 2) * 73) % 900000 + 100000 AS INT) AS passenger_key,
    CASE                
        WHEN FLOOR(DATEDIFF(current_date, dob) / 365) < 13 THEN 'Child'                
        WHEN FLOOR(DATEDIFF(current_date, dob) / 365) BETWEEN 13 AND 19 THEN 'Young Adult'                
        WHEN FLOOR(DATEDIFF(current_date, dob) / 365) BETWEEN 20 AND 39 THEN 'Adult'                
        WHEN FLOOR(DATEDIFF(current_date, dob) / 365) BETWEEN 40 AND 59 THEN 'Middle-Aged'                
        ELSE 'Senior'            
    END AS age_category,
    FLOOR(DATEDIFF(current_date, dob) / 365) AS age,
    gender,
    occupation,
    nationality,
    city,
    country,
    current_date as start_date,
    '9999-12-31' as end_date,
    created_at,
    modified_at as last_updated,
    membership_status,
    loyalty_tier
FROM delta_dim_passenger;

-- Step 2: Handle the existing records (mark them as expired if they're being updated)
INSERT OVERWRITE TABLE dim_passenger
SELECT
    d.passenger_id,
    d.passenger_key,
    d.age_category,
    d.age,
    d.gender,
    d.occupation,
    d.nationality,
    d.city,
    d.country,
    d.start_date,
    -- End date if there's a newer version
    CASE 
        WHEN s.passenger_id IS NOT NULL THEN CAST(DATE_SUB(CURRENT_DATE, 1) AS STRING)
        ELSE d.end_date
    END as end_date,
    d.created_at,
    d.last_updated,
    d.membership_status,
    d.loyalty_tier
FROM 
    dim_passenger d
LEFT JOIN 
    dim_passenger_stage s ON d.passenger_id = s.passenger_id;

-- Step 3: Insert the new and updated records from staging
INSERT INTO TABLE dim_passenger
SELECT * FROM dim_passenger_stage;








-- 7. Example optimized queries using the new schema

-- Example 1: Revenue analysis by passenger loyalty tier and month
-- No joins needed - uses denormalized fields
SELECT 
    year,
    month,
    loyalty_tier,
    SUM(CAST(revenue AS DECIMAL(18,2))) AS total_revenue
FROM reservation_fact
WHERE year = 2023
GROUP BY year, month, loyalty_tier
ORDER BY month, total_revenue DESC;

-- Example 2: Airport performance analysis
-- No joins needed - uses denormalized fields
SELECT 
    country,
    city,
    COUNT(*) AS reservation_count,
    SUM(CAST(revenue AS DECIMAL(18,2))) AS total_revenue,
    SUM(CAST(profit AS DECIMAL(18,2))) AS total_profit
FROM reservation_fact
WHERE year = 2023
GROUP BY country, city
ORDER BY total_revenue DESC;

-- Example 3: Complex analysis requiring dimension tables
-- Still requires joins but with bucketing optimization
SELECT 
    r.loyalty_tier,
    r.model,
    r.occupation,
    COUNT(*) AS reservation_count,
    SUM(CAST(r.revenue AS DECIMAL(18,2))) AS total_revenue
FROM reservation_fact r
WHERE r.year = 2023
GROUP BY r.loyalty_tier, r.model, r.occupation
ORDER BY total_revenue DESC
LIMIT 20;
```