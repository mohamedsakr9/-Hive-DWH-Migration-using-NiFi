# Redshift to Hive Data Migration Pipeline Documentation

## Overview

This document outlines the implementation of an Apache NiFi data pipeline that extracts data from Amazon Redshift and loads it into Apache Hive. The pipeline utilizes a dynamic approach to discover and process tables from a specified schema, making it flexible and adaptable to schema changes.

## Pipeline Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Redshift      │    │     Apache      │    │     Apache      │
│   Database      │───▶│     NiFi        │───▶│     Hive        │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

**The pipeline follows these sequential steps:**

1. Discover tables in the source schema
2. Process each table individually
3. Extract data from source tables
4. Extract schema from source tables
5. Load data into Hive HDFS directories

---

## Implementation Details

### 1. Table Discovery

The pipeline begins by extracting table metadata from the Redshift information schema.

**Processor**: `ExecuteSQL`

**Configuration**:
- **Database Connection**: Redshift Connection Pool
- **SQL Query**:
  ```sql
  SELECT table_schema, table_name 
  FROM information_schema.tables 
  WHERE table_schema = 'airline' 
  AND table_type = 'BASE TABLE';
  ```
- **Output Format**: Avro

**Purpose**: The processor discovers all tables within the "airline" schema of the Redshift database.

---

### 2. Format Conversion

Converts the Avro output from the initial query to JSON for easier processing.

**Processor**: `ConvertRecord`

**Configuration**:
- **Record Reader**: AvroReader
- **Record Writer**: JsonRecordSetWriter

**Purpose**: Transforms Avro format to JSON for downstream JSON processing.

---

### 3. Table Separation

Splits the JSON array into individual flowfiles, one per table.

**Processor**: `SplitJson`

**Configuration**:
- **JsonPath Expression**: `$.*`

**Purpose**: Each resulting flowfile represents a single table to be processed.

**Sample Output**:
```json
{
  "table_schema": "airline",
  "table_name": "passengers"
}
```

---

### 4. Metadata Extraction

Extracts table metadata as flowfile attributes for dynamic processing.

**Processor**: `EvaluateJsonPath`

**Configuration**:
- **Destination**: flowfile-attribute
- **Properties**:
  - `table_schema`: `$.table_schema`
  - `table_name`: `$.table_name`

**Purpose**: Converts JSON fields into NiFi flowfile attributes for use in subsequent processors.

---

### 5. Data Extraction

Extracts the actual data from each source table in Redshift.

**Processor**: `ExecuteSQL`

**Configuration**:
- **Database Connection**: Redshift Connection Pool
- **SQL Query**: `SELECT * FROM ${table_schema}.${table_name}`
- **Output Format**: Avro

**Purpose**: This processor dynamically constructs SQL queries based on the table metadata extracted in the previous step.

---

### 6. HDFS Storage

Writes Avro files to the appropriate HDFS location for Hive access.

**Processor**: `PutHDFS`

**Configuration**:
- **Directory**: `/user/hive/warehouse/${table_name}`
- **Conflict Resolution Strategy**: Replace
- **Hadoop Configuration Resources**: Path to core-site.xml, hdfs-site.xml

**Purpose**: Stores the extracted data in HDFS in a location accessible by Hive.

---

## Schema Extraction Process

The pipeline includes a parallel process to extract and manage table schemas:

### Schema Extraction Components

#### 1. ExtractRecordSchema
- **Function**: Reads Avro data and extracts schema into flowfile attributes
- **Output**: Schema information stored in `avro.schema` attribute

#### 2. UpdateAttribute
- **Function**: Used to name and route the schema file
- **Configuration**: Sets filename and path attributes for schema files

#### 3. ReplaceText
- **Function**: Writes the schema content into an Avro .avsc file ready for Hive
- **Configuration**: 
  - **Replacement Strategy**: Regex Replace
  - **Regular Expression**: `.*`
  - **Replacement Value**: `${avro.schema}`

#### 4. PutHDFS (Schema)
- **Function**: Writes schema files to HDFS
- **Configuration**:
  - **Directory**: `/user/hive/schemas/`
  - **Filename**: `${table_name}_schema.avsc`

---

## Pipeline Flow Summary

```
┌──────────────────┐
│   ExecuteSQL     │ ← Discover tables
│  (Table Query)   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  ConvertRecord   │ ← Convert Avro to JSON
│  (Avro → JSON)   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   SplitJson      │ ← Split into individual tables
│                  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│EvaluateJsonPath  │ ← Extract table metadata
│                  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐    ┌──────────────────┐
│   ExecuteSQL     │    │ExtractRecordSchema│ ← Extract data & schema
│  (Data Query)    │    │                  │
└────────┬─────────┘    └────────┬─────────┘
         │                       │
         ▼                       ▼
┌──────────────────┐    ┌──────────────────┐
│     PutHDFS      │    │  UpdateAttribute │ ← Store data & schema
│   (Data Files)   │    │                  │
└──────────────────┘    └────────┬─────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │   ReplaceText    │
                        │                  │
                        └────────┬─────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │     PutHDFS      │
                        │ (Schema Files)   │
                        └──────────────────┘
```

---

## Key Features

### Dynamic Table Processing
- **Automatic Discovery**: Pipeline automatically discovers all tables in the specified schema
- **Metadata-Driven**: Uses table metadata to dynamically construct queries and file paths
- **Schema Evolution**: Automatically handles schema changes through dynamic extraction

### Flexible Configuration
- **Parameterized Queries**: Uses NiFi Expression Language for dynamic query construction
- **Configurable Paths**: HDFS paths are dynamically generated based on table names
- **Environment Agnostic**: Can be easily adapted to different environments

### Error Handling
- **Conflict Resolution**: Handles file conflicts with configurable strategies
- **Connection Pooling**: Uses database connection pooling for efficient resource usage
- **Retry Logic**: Built-in retry mechanisms for failed operations

---

## Prerequisites

### Software Requirements
- Apache NiFi 1.23.2+
- Apache Hadoop with HDFS
- Apache Hive
- Amazon Redshift access

### NiFi Processors Required
- ExecuteSQL
- ConvertRecord
- SplitJson
- EvaluateJsonPath
- ExtractRecordSchema
- UpdateAttribute
- ReplaceText
- PutHDFS

### Configuration Files
- `core-site.xml` - Hadoop core configuration
- `hdfs-site.xml` - HDFS configuration
- Database connection pool configuration for Redshift

---

## Deployment Instructions

1. **Import NiFi Template**: Import the provided XML template into NiFi
2. **Configure Connection Pools**: Set up Redshift database connection pool
3. **Configure Hadoop**: Ensure Hadoop configuration files are accessible
4. **Set Parameters**: Update schema names and file paths as needed
5. **Enable Processors**: Start the processor group
6. **Monitor Execution**: Check NiFi logs and HDFS for successful data transfer

---

## Monitoring and Troubleshooting

### Key Metrics to Monitor
- **Flowfile Processing Rate**: Monitor the rate of flowfile processing
- **Error Rates**: Track failed flowfiles and error messages
- **HDFS Storage**: Monitor HDFS disk usage and file creation
- **Database Connections**: Monitor connection pool usage

### Common Issues
- **Connection Timeouts**: Check network connectivity to Redshift
- **HDFS Permissions**: Ensure NiFi has write permissions to HDFS
- **Schema Mismatches**: Verify Avro schema compatibility
- **Resource Constraints**: Monitor memory and CPU usage

---

## Performance Optimization

### Recommended Settings
- **Concurrent Tasks**: Set appropriate concurrent task counts based on resources
- **Batch Size**: Configure optimal batch sizes for database queries
- **Connection Pool Size**: Size connection pools appropriately
- **Buffer Sizes**: Tune buffer sizes for optimal throughput

### Best Practices
- **Incremental Loading**: Implement incremental data loading where possible
- **Partitioning**: Use appropriate partitioning strategies in Hive
- **Compression**: Enable compression for better storage efficiency
- **Monitoring**: Implement comprehensive monitoring and alerting