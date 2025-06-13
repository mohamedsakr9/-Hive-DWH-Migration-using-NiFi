# SCD Type 2 Implementation in Hive: Documentation and Rationale

## Overview

This document details the implementation of Slowly Changing Dimension (SCD) Type 2 in Hive, using a four-step approach that efficiently handles historical data tracking. The implementation allows for maintaining a complete history of dimension changes while optimizing for Hive's specific execution characteristics.

## Implementation

The SCD Type 2 implementation consists of four distinct steps:

### Step 1: Identify Records to Expire

```sql
-- Step 1: Identify records that need to be expired (current records that are changing)
CREATE TEMPORARY TABLE records_to_expire AS 
SELECT 
    dim.passenger_id, 
    dim.start_date,
    dim.membership_status,
    dim.loyalty_tier
FROM 
    dim_passenger dim
JOIN 
    delta_dim_passenger delta
ON 
    dim.passenger_id = delta.passenger_id
WHERE 
    dim.end_date = '9999-12-31'  -- Current records
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
```

### Step 2: Update Existing Records

```sql
-- Step 2: Update existing records - set end date for records being expired
INSERT OVERWRITE TABLE dim_passenger
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
    -- Set end date if this record is being updated
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
    dim_passenger p
LEFT JOIN 
    records_to_expire e
ON 
    p.passenger_id = e.passenger_id 
    AND p.end_date = '9999-12-31';
```

### Step 3: Insert New Versions of Changed Records

```sql
-- Step 3: Insert new versions of changed records
INSERT INTO TABLE dim_passenger
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
```

### Step 4: Insert Completely New Records

```sql
-- Step 4: Insert completely new records (never seen before)
INSERT INTO TABLE dim_passenger
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
        FROM dim_passenger p 
        WHERE p.passenger_id = delta.passenger_id
    );
```

### Cleanup

```sql
-- Clean up temporary tables
DROP TABLE records_to_expire;
```

## Why This Approach Works Best

This SCD Type 2 implementation was specifically designed to address the unique requirements and constraints of Hive. Let's examine why each aspect of this approach is optimal:

### 1. Temporary Table for Change Identification

The use of a temporary table in Step 1 provides several critical advantages:

- **Performance Optimization**: By identifying changed records once and storing them in a temporary table, we avoid repeating complex comparison logic multiple times
- **Reduced Data Volume**: The temporary table contains only records that need changes, dramatically reducing the volume of data processed in subsequent steps
- **Improved Join Efficiency**: Subsequent joins against this small temporary table are much faster than joins against the full dimension table
- **Partition Information Preservation**: Including partition columns (`membership_status`, `loyalty_tier`) in the temporary table maintains partition context for efficient operations

### 2. Partition-Aware Operations

The `INSERT OVERWRITE TABLE ... PARTITION` statement in Step 2 leverages Hive's partition handling:

- **Dynamic Partition Management**: Hive efficiently manages dynamic partitioning with this syntax
- **Targeted Updates**: Only modifies the partitions containing changed records
- **Atomic Operation**: Updates all affected records in a single operation
- **Reduced Partition Limit Issues**: By processing data through the temporary table first, we avoid hitting Hive's dynamic partition limits


### 3. Minimal Data Movement

This approach minimizes the amount of data that needs to be moved:

- Only affected partitions are modified
- The temporary table approach avoids full table scans
- Targeted operations reduce network and disk I/O



## Integration with Incremental Loading

This SCD Type 2 implementation integrates seamlessly with incremental loading from PostgreSQL:

1. NiFi extracts changed records using `IS_MODIFIED` and `IS_INSERTED` timestamp columns
2. The extracted data lands in HDFS and is registered as an external table (`delta_dim_passenger`)
3. The four-step SCD process efficiently applies these changes to the dimension table
4. The process can run on any schedule (daily, hourly, etc.) as needed

## Conclusion

The four-step SCD Type 2 implementation presented here represents an ideal approach for Hive-based data warehouses. It balances simplicity, efficiency, and maintainability while addressing the specific characteristics and limitations of Hive.

By structuring the operations as a series of distinct, focused steps with a clever use of temporary tables, this approach avoids common pitfalls and delivers optimal performance for historical data tracking.