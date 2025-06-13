
DROP TABLE IF EXISTS delta_dim_passenger;

-- Create the delta table pointing to the staged data
CREATE EXTERNAL TABLE delta_dim_passenger
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/passenger_changes/2025-05-10_19'
TBLPROPERTIES ('avro.schema.url'='hdfs:///user/hive/schemas/delta_passenger_schema.avsc');
-- Step 1: Identify records that need to be expired
CREATE TEMPORARY TABLE records_to_expire AS 
SELECT 
    dim.passenger_id, 
    dim.start_date,
    dim.membership_status,
    dim.loyalty_tier
FROM 
    reservation.dim_passenger dim
JOIN 
    delta_dim_passenger delta
ON 
    dim.passenger_id = delta.passenger_id
WHERE 
    dim.end_date = '9999-12-31'
    AND (
        dim.age != delta.age OR
        dim.gender != delta.gender OR
        dim.occupation != delta.occupation OR
        dim.nationality != delta.nationality OR
        dim.city != delta.city OR
        dim.country != delta.country OR
        dim.membership_status != delta.membership_status OR
        dim.loyalty_tier != delta.loyalty_tier
    );



-- Step 2: Update existing records
INSERT OVERWRITE TABLE reservation.dim_passenger
PARTITION(membership_status, loyalty_tier)
SELECT
    p.passenger_id,
    p.passenger_key,
    p.age_category,
    p.age,
    p.gender,
    p.occupation, 
    p.nationality,
    p.city,
    p.country,
    p.start_date,
    CASE 
        WHEN e.passenger_id IS NOT NULL THEN CAST(CURRENT_DATE AS STRING)
        ELSE p.end_date
    END AS end_date,
    p.created_at,
    CASE 
        WHEN e.passenger_id IS NOT NULL THEN CAST(CURRENT_TIMESTAMP AS STRING)
        ELSE p.last_updated
    END AS last_updated,
    p.membership_status,
    p.loyalty_tier
FROM 
    reservation.dim_passenger p
LEFT JOIN 
    records_to_expire e
ON 
    p.passenger_id = e.passenger_id 
    AND p.end_date = '9999-12-31';

-- Step 3: Insert new versions of changed records
CREATE TEMPORARY TABLE new_versions AS
SELECT
    delta.passenger_id,
    delta.passenger_key,
    delta.age_category,
    delta.age,
    delta.gender,
    delta.occupation,
    delta.nationality,
    delta.city,
    delta.country,
    CAST(CURRENT_DATE AS STRING) AS start_date,
    '9999-12-31' AS end_date,
    delta.is_inserted AS created_at,
    delta.last_updated,
    delta.membership_status,
    delta.loyalty_tier
FROM
    delta_dim_passenger delta
JOIN
    records_to_expire e
ON
    delta.passenger_id = e.passenger_id;



-- Insert the new versions
INSERT INTO TABLE reservation.dim_passenger
SELECT * FROM new_versions;

-- Step 4: Find completely new records
CREATE TEMPORARY TABLE new_records AS
SELECT
    delta.passenger_id,
    delta.passenger_key,
    delta.age_category,
    delta.age,
    delta.gender,
    delta.occupation,
    delta.nationality,
    delta.city,
    delta.country,
    CAST(CURRENT_DATE AS STRING) AS start_date,
    '9999-12-31' AS end_date,
    delta.is_inserted AS created_at,
    delta.last_updated,
    delta.membership_status,
    delta.loyalty_tier
FROM
    delta_dim_passenger delta
WHERE
    NOT EXISTS (
        SELECT 1 
        FROM reservation.dim_passenger p 
        WHERE p.passenger_id = delta.passenger_id
    );



-- Insert the new records
INSERT INTO TABLE reservation.dim_passenger
SELECT * FROM new_records;

-- Clean up
DROP TABLE records_to_expire;
DROP TABLE new_versions;
DROP TABLE new_records;
