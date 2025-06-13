CREATE EXTERNAL TABLE staging_airport_dim
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/airport_dim'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/airport_dim_schema.avsc' 
);


=========
CREATE EXTERNAL TABLE staging_complaint_category_dim
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/complaint_category_dim'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/complaint_category_dim_schema.avsc' 
);

=========
CREATE EXTERNAL TABLE staging_date_dim
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/date_dim'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/date_dim_schema.avsc' 
);

=========
CREATE EXTERNAL TABLE staging_dim_airplane
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/dim_airplane'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/dim_airplane_schema.avsc' 
);

=========
CREATE EXTERNAL TABLE staging_dim_crew
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/dim_crew'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/dim_crew_schema.avsc' 
);

=========
CREATE EXTERNAL TABLE staging_dim_promotion
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/dim_promotion'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/dim_promotion_schema.avsc' 
);

=========
CREATE EXTERNAL TABLE staging_employee_dim
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/employee_dim'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/employee_dim_schema.avsc' 
);

=========
CREATE EXTERNAL TABLE staging_fact_flight_activity
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/fact_flight_activity'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/fact_flight_activity_schema.avsc' 
);

=========
CREATE EXTERNAL TABLE staging_interaction_fact
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/interaction_fact'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/interaction_fact_schema.avsc' 
);

=========
CREATE EXTERNAL TABLE staging_loyalty_program_fact
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/loyalty_program_fact'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/loyalty_program_fact_schema.avsc' 
);

=========
CREATE EXTERNAL TABLE staging_overnight_stay_fact
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/overnight_stay_fact'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/overnight_stay_fact_schema.avsc' 
);

=========

CREATE EXTERNAL TABLE staging_passenger_dim
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/passenger_dim'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/passenger_dim_schema.avsc' 
);
=========
CREATE EXTERNAL TABLE staging_reservation_fact
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/reservation_fact'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/reservation_fact_schema.avsc' 
);

=========

CREATE EXTERNAL TABLE staging_reservation_tracking_fact
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/hive/staging/reservation_tracking_fact'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/hive/schemas/reservation_tracking_fact_schema.avsc' 
);
=========


