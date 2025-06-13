# 🚀 Hive Data Warehouse Migration Documentation

## 📅 Project Title

**Airline Data Warehouse Migration from Redshift to Hive**

---

## 📄 Objective

Re-engineer the airline enterprise data warehouse to run on Apache Hive for scalability, schema evolution, cost efficiency, and compatibility with Hadoop ecosystem tools.

---

## 💡 Overview

This documentation presents the architectural design, implementation steps, scripts, and rationale for migrating an existing DWH from Amazon Redshift to Apache Hive. The system supports:

* ACID and non-ACID table designs
* Incremental loading using `cron` and NiFi pipelines
* Schema evolution via Avro in staging
* Denormalized and optimized fact models for performance

---

## 💼 Components & Steps

### 1. 🌐 Source to Staging: Redshift to HDFS

* **Tool**: Apache NiFi

* **Steps**:

  1. Discover all `public` schema tables using SQL in NiFi.
  2. Extract table data as Avro.
  3. Convert Avro to Parquet (or keep as Avro in staging).
  4. Store in HDFS `/user/hive/staging/<table_name>`
  5. **Extract schemas** dynamically using the following NiFi processors:

     * `ExtractRecordSchema`: Reads Avro and extracts schema into flowfile attributes.
     * `UpdateAttribute`: Used to name and route the schema file.
     * `ReplaceText`: Writes the schema content into an Avro `.avsc` file ready for Hive.

  ![NiFi Schema Flow](attachment\:d07d712c-fc3a-4af7-9674-14e5d7700017.png)

* **Documentation**: Refer to `migration.pdf`

* **Tool**: Apache NiFi

* **Steps**:

  1. Discover all `public` schema tables using SQL in NiFi.
  2. Extract table data as Avro.
  3. Convert Avro to Parquet (or keep as Avro in staging).
  4. Store in HDFS `/user/hive/staging/<table_name>`

* **Documentation**: Refer to `migration.pdf`

### 2. 🔗 Staging Area Design

* **Format**: Avro — chosen specifically for its advantages in schema enforcement, evolution, and binary performance over CSV

* **Why Avro over CSV in Staging?**

  * Native integration with **NiFi Schema Registry**
  * **Faster read performance** (2–3x vs CSV) and better compression
  * We leveraged **NiFi processors** like `ExtractRecordSchema`, `UpdateAttribute`, and `ReplaceText` to automatically extract and manage schema files
  * Unlike CSV, Avro allows **explicit typing and schema evolution**, which is essential for long-term data pipeline stability
  * **Efficient conversion to ORC**: Hive can transform Avro into ORC format more effectively than CSV due to Avro's binary structure and schema alignment, reducing transformation cost and improving query performance

* **Hive External Table Example**:

```sql
CREATE EXTERNAL TABLE staging_airport_dim
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/airport_dim'
TBLPROPERTIES ('avro.schema.url'='hdfs:///user/hive/schemas/airport_dim_schema.avsc');
```

* **More details**: See `staging.md`

* **Format**: Avro — chosen specifically for its advantages in schema enforcement, evolution, and binary performance over CSV

* **Why Avro over CSV in Staging?**

  * Native integration with **NiFi Schema Registry**
  * **Faster read performance** (2–3x vs CSV) and better compression
  * We leveraged **NiFi processors** like `ExtractRecordSchema`, `UpdateAttribute`, and `ReplaceText` to automatically extract and manage schema files
  * Unlike CSV, Avro allows **explicit typing and schema evolution**, which is essential for long-term data pipeline stability

  ref:https://medium.com/ssense-tech/csv-vs-parquet-vs-avro-choosing-the-right-tool-for-the-right-job-79c9f56914a8

* **Hive External Table Example**:

```sql
CREATE EXTERNAL TABLE staging_airport_dim
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/airport_dim'
TBLPROPERTIES ('avro.schema.url'='hdfs:///user/hive/schemas/airport_dim_schema.avsc');
```

* **More details**: See `staging.md`, `why avro not csv.md`
* **Format**: Avro (for schema evolution, type safety, performance)
* **Hive External Table Example**:

```sql
CREATE EXTERNAL TABLE staging_airport_dim
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/airport_dim'
TBLPROPERTIES ('avro.schema.url'='hdfs:///user/hive/schemas/airport_dim_schema.avsc');
```

* **More details**: See `staging.md`, `why avro not csv.md`

### 3. 📂 Target Schema Design in Hive

### 🔄 Why Duplicate Dimension Columns in Fact Tables?

In big data environments like Hive, joining large fact tables with multiple dimension tables during query time is costly due to shuffle and scan overhead. To mitigate this:

* We **denormalize** and duplicate key columns (e.g., `airport_name`, `city`, `calendar_quarter`, `loyalty_tier`) directly in the fact table.
* This design enables:

  * **Faster query performance** by avoiding runtime joins
  * **Snapshot reporting**, as the fact row captures the exact state of dimension values at time of reservation
  * **Resilience to dimension changes**, since fact rows retain context independently

### 🧩 Why Match Bucket Count Between Fact and Dimension Tables?

* We **cluster** both `reservation_fact` and `dim_passenger` by `passenger_id` into **32 buckets**.
* Benefits:

  * Hive can perform **bucket map joins**, avoiding full table scans and reducing shuffle
  * Buckets become **co-located**, improving **join efficiency**
  * Aligning bucket count across tables ensures **partition compatibility** for scalable parallel execution

#### A. Non-ACID Tables (for append-only Fact tables)

* Created using `EXTERNAL TABLE`, partitioned and bucketed.
* Denormalized using views for performance.
* File format: Parquet.
* Reference: `not acid.md`

**✨ Pros of Using Denormalized Non-ACID Fact Schema in Big Data Modeling:**

* Optimized for **analytical workloads** by minimizing the need for joins
* Ensures **better query performance** due to localized data access
* Bucketed by `passenger_id` enables **fast filtering and joins** with dimension tables
* Partitioning by `(year, month)` supports **efficient time-based querying**
* Duplication of dimension data (e.g., `airport_name`, `city`, etc.) supports **snapshot-based reporting**
* **Append-only** design aligns with Hive's strength in bulk inserts over updates
* Designed for **scalability**: handles massive volumes of immutable transactional data efficiently

#### B. ACID-Compliant Tables (for dimensions with change history)

* Format: ORC
* ACID enabled via:

```sql
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
```

* Example: `dim_passenger` with partitioning and bucketing
* Reference: `acid.md`

### 4. 🏛 Data Loading

#### ➔ Dimension Tables

* Load from staging to ACID/External dim tables.
* Use `INSERT INTO` for ACID; `INSERT OVERWRITE` for External.

#### ➔ Fact Table

* Create view `temp_reservation_denormalized`
* Join staging + dim tables, insert into partitioned & bucketed fact

### 5. ⏳ Incremental Load + SCD Type 2

* **Tool**: Hive scripts triggered via `cron` or bash
* **Script**: `run_scd.sh` (uses Beeline to execute `scd_dim_passenger.hql`)
* **SCD Logic**:

  * Identify changes from `delta_dim_passenger`
  * Expire old records
  * Insert updated and new versions
* Full logic breakdown: `SCD.md`

---

## 💮 Partitioning and Bucketing Strategy

| Table             | Partition Column(s)      | Bucket Column | Buckets |
| ----------------- | ------------------------ | ------------- | ------- |
| reservation\_fact | year, month              | passenger\_id | 32      |
| dim\_passenger    | membership\_status, tier | passenger\_id | 32      |
| dim\_date         | calendar\_year           | date\_id      | 8       |
| dim\_airport      | country                  | airport\_code | 8       |
| dim\_airplane     | manufacturer             | airplane\_id  | 8       |

---

## 💰 Performance Tuning & Configurations

```sql
SET hive.execution.engine=tez;
SET parquet.compression=SNAPPY;
SET hive.enforce.bucketing=true;
SET hive.enforce.sorting=true;
SET hive.auto.convert.join=true;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=8192;
```

---

## ⚙️ Crontab for Scheduling

Sample entry in `crontab -e`:

```bash
0 * * * * /home/hadoop/scripts/run_scd.sh >> /tmp/crontab_scd.log 2>&1
```

This triggers hourly loading of changed passengers.

---

## 🔮 Sample Use Cases

* Revenue by loyalty tier and month
* Airport-level performance
* Aircraft utilization by tier and occupation
  (Queries provided in `not acid.md` and `acid.md`)

---

## 🏆 Conclusion

This system implements a full data warehouse pipeline on Hive with:

* Efficient schema migration from Redshift
* ACID/Non-ACID hybrid schema
* Incremental + SCD Type 2 support
* Scalable partitioning & bucketing
* Automation using NiFi and cron

With Hive on Tez and ORC/Parquet, the warehouse is optimized for analytical workloads at scale.

---

## 📁 Attachments

* `acid.md` — ACID schema
* `not acid.md` — External schema
* `migration.pdf` — Redshift to Hive migration
* `run_scd.sh` — Shell automation
* `SCD.md` — SCD logic breakdown
* `staging.md` — Avro staging strategy
* `why avro not csv.md` — Format rationale

