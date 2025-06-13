# My Implementation of Avro-Based Staging Tables in Hive

## Background: Why I Chose Avro Over CSV

After evaluating different formats for our data pipeline's staging area, I decided to implement Avro-based staging tables instead of CSV for several compelling reasons:

1. **NiFi Integration Advantages**
   - Our Apache NiFi data pipelines benefit from Avro's native Schema Registry integration
   - Conversion efficiency from Avro→ORC is significantly better than CSV→ORC in NiFi's ConvertRecord processors
   - Complex data types (nested structures, arrays) maintain integrity through the pipeline with Avro

2. **Performance Benefits I Needed**
   - Reading performance is 2-3x faster than CSV due to binary format
   - Our transformation costs to final ORC format are lower with Avro
   - Better compression ratios save storage space in our staging area

3. **Schema Management Requirements**
   - Our data sources frequently change schemas, and Avro's schema evolution capabilities were essential
   - Strong schema enforcement helps catch data issues early in our pipeline
   - Complex data structures in our airline data model are better represented in Avro

## My Implementation Process

### Step 1: Extracting the Schema Files using NIFI



### Step 2: Setting Up HDFS Directory Structure

I established a consistent HDFS directory structure to organize our staging environment:

```
/user/hive/
  ├── schemas/           # Central location for all Avro schema files
  └── staging/           # Base directory for staging tables
      ├── airport_dim/
      ├── complaint_category_dim/
      ├── date_dim/
      └── ... other tables
```



### Step 3: Creating the Staging Tables

I created external Hive tables pointing to these locations. For each table, I used the same pattern with specific schema references:

```sql
CREATE EXTERNAL TABLE staging_airport_dim
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/airport_dim'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/airport_dim_schema.avsc' 
);
```

I chose the EXTERNAL table type because:
1. It gives more flexibility in managing the underlying data
2. NiFi can write directly to the HDFS locations
3. The staging tables are temporary and I wanted full control over data lifecycle

### Step 4: Implementing NiFi Pipeline for Schema Extraction and Avro Processing

I built a custom NiFi pipeline specifically designed to handle the schema extraction and Avro processing:

1. **Schema Extraction Flow**:
   - Used `ExecuteSQL` processor to query metadata from source systems
   - Implemented `ExtractRecordSchema` to dynamically generate Avro schemas
   - Added `UpdateAttribute` and `ReplaceText` processors to handle file naming and metadata
   - Used `PutHDFS` to store the generated schema files directly to the HDFS schema location

2. **Data Processing Flow**:
   - Configured processors to convert source data to Avro format
   - Used the same extracted schemas to ensure consistency
   - Implemented `PutHDFS` to write Avro files to the staging locations
   - Added `LogAttribute` processors for monitoring and tracking

This approach allowed me to automate schema management entirely, minimizing manual intervention and ensuring schema consistency across the pipeline.

## My Implemented Tables

I set up the following staging tables for our airline data warehouse:

### Dimension Tables
- `staging_airport_dim` - Stores airport reference data
- `staging_complaint_category_dim` - Categorization of customer complaints
- `staging_date_dim` - Calendar dimension table
- `staging_dim_airplane` - Aircraft inventory and details
- `staging_dim_crew` - Flight crew information
- `staging_dim_promotion` - Marketing promotions data
- `staging_employee_dim` - Employee reference data
- `staging_passenger_dim` - Passenger profile information

### Fact Tables
- `staging_fact_flight_activity` - Operational flight data
- `staging_interaction_fact` - Customer service interactions
- `staging_loyalty_program_fact` - Frequent flyer program activity
- `staging_overnight_stay_fact` - Crew accommodations data
- `staging_reservation_fact` - Booking information
- `staging_reservation_tracking_fact` - Reservation status tracking

## Challenges I Overcame

### NiFi Schema Extraction Pipeline
One of the main challenges was dynamically parsing and extracting Avro schemas. Instead of manually defining columns in the Hive DDL, I built a NiFi pipeline to:

1. Extract the schema information from source data
2. Generate Avro schema files (.avsc) automatically
3. Store these schema files in HDFS

As shown in the NiFi pipeline screenshot, this process involved:
- Using `ExecuteSQL` to obtain metadata
- `ExtractRecordSchema` processor to dynamically generate Avro schemas
- `PutHDFS` to store the schema files in HDFS
- `UpdateAttribute` and `ReplaceText` processors to handle metadata and file naming

This approach ensured that my Hive tables were always created with the correct schema that matched the incoming data structure, eliminating manual schema definition and maintenance.

