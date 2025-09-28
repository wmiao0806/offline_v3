-- create database if not exists work;
CREATE EXTERNAL TABLE my_csv_table
(
    CRIM    double,
    ZN      double,
    INDUS   double,
    CHAS    double,
    NOX     double,
    RM      double,
    AGE     double,
    DIS     double,
    RAD     double,
    TAX     double,
    PTRATIO double,
    B       double,
    LSTAT   double,
    TARGET  double
)
    PARTITIONED BY (ds STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    STORED AS TEXTFILE
    LOCATION 's3a://first-bucket/';
ALTER TABLE my_csv_table ADD PARTITION (ds='20250925')
    LOCATION 's3a://first-bucket/ds=20250925/';
SELECT * FROM my_csv_table WHERE ds='20250925';

MSCK REPAIR TABLE my_csv_table;
