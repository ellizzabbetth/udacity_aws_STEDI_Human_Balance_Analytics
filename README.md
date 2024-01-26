
## Summary

As a data engineer for the STEDI team, we built a data lakehouse solution for sensor data that trains a machine learning model.

The purpose of a Lakehouse is to separate data processing into stages. Deferring transformation is a hallmark of Data Lakes and Lakehouses. In this way, keeping the data at multiple stages in file storage gives more options for later analytics, because it preserves all of the format. 

### 

Raw/Landing Zone

"For pipelines that store data in the S3 data lake, data is ingested from the source into the landing zone as-is. The processing layer then validates the landing zone data and stores it in the raw zone bucket or prefix for permanent storage. "
A landing zone is a place where new data arrives prior to processing.

Trusted Zone

"The processing layer applies the schema, partitioning, and other transformations to the raw zone data to bring it to a conformed state and stores it in trusted zone."
S3 location for customers who have chosen to share with research, or a trusted zone.

Curated Zone

**"**As a last step, the processing layer curates a trusted zone dataset by modeling it and joining it with other datasets, and stores it in curated layer."

"Typically, datasets from the curated layer are partly or fully ingested into Amazon Redshift data warehouse storage to serve use cases that need very low latency access or need to run complex SQL queries."

Source: https://aws.amazon.com/blogs/big-data/build-a-lake-house-architecture-on-aws/

## Use Glue Catalog to Query a Landing Zone

### Create a Glue Table definition to query the data using SQL

1. Search for Glue Catalog in the AWS Console, and you will see the Glue Data Catalog. Click Data Catalog

2. 

##  Generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.

As a starting point, create your own S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones, and copy the data there.

### Explore data
Create two Glue tables for the two landing zones, x and y. See customer_landing.sql and accelerometer_landing.sql
Query these tables using Athena. Screenshots of each one with the resulting data are in images: customer_landing and accelerometer_landing.

## Commands


Create an S3 Gateway Endpoint
```
aws s3 mb s3://eli-stedi-lake-house
```


Step 2: S3 Gateway Endpoint (look for the VpcId in the output)
```
aws ec2 describe-vpcs
```


Routing Table (look for the RouteTableId)

```
    aws ec2 describe -route-tables
```


```
aws ec2 create-vpc-endpoint --vpc-id vpc-02d0241f204830c2b --service-name com.amazonaws.us-east-1.s3 --route-table-ids rtb-0faadd47a6204a48b
```

Finally create the S3 Gateway, replacing the blanks with the VPC and Routing Table Ids
```
aws ec2 create-vpc-endpoint --vpc-id vpc-053a44aed13d03adc --service-name com.amazonaws.us-east-1.s3 --route-table-ids rtb-0cd13a11d5c7393d8
```


Before we can process sensitive accelerometer data, we need to bring it into the landing zone.
copy the accelerometer data into an S3 landing zone with the s3 cp command
```
cd nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter
aws s3 cp accelerometer/ s3://your-own-bucket-name/accelerometer/ --recursive
```

## Requirements

1. S3 directories for the following zones
- customer_landing
- step_trainer_landing
- accelerometer_landing 

2. Create 2 Glue tables for the 2 landing zones:
- customer_landing.sql
- accelerometer_landing.sql

3. Query the 2 Glue tables in Athena.
- takes screenshots of customer_landing and accelerometer_landing

4. Create 2 Glue Jobs that do the following

- create a Glue Table called customer_trusted
- create a Glue Table called accelerometer_trusted
- take screenshot of customer trusted

5. write Glue Job called customer_curated

6. create Glue Job call step_trainer_Trusted
Count of step_trainer_landing: 28680 rows" and "Count of step_trainer_trusted: 14460 rows".

7. create Glue Job called machine_learning_curated

## Number of rows in each table:

### Landing
Customer: 956
Accelerometer: 81273
Step Trainer: 28680
### Trusted
Customer: 482
Accelerometer: 40981
Step Trainer: 14460
### Curated
Customer: 482
Machine Learning: 43681