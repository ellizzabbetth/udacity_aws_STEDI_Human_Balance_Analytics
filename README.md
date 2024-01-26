
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


## Feedback

Requires Changes
6 specifications require changes
Dear student,

You put a fantastic effort into your STEDI Human Balance Analytics project! 🚀 It's evident that you've invested time and dedication into your work, and your progress is commendable. Building skills in AWS Glue requires attention to detail, and you've shown a great grasp of many key concepts.

👏 Positive Highlights:

Your work in the Landing Zone is well-structured, and your SQL DDL scripts are comprehensive for customer_landing and accelerometer_landing tables.
Excellent job on the Glue Jobs in the Trusted Zone, especially configuring dynamic schema updates. 👍
Your implementation of inner joins in the Curated Zone showcases a solid understanding of data linking and transformation.
🚧 Areas for Improvement:

Script Completeness:

Ensure completeness in your scripts, especially in accelerometer_landing_to_trusted.py, where fetching data from the S3 bucket is missing. This is crucial for accurate data processing.
SQL DDL Scripts:

Remember to include the SQL DDL script for the step_trainer_landing table to maintain consistency across all landing scripts.
Athena Queries Screenshots:

Screenshots demonstrating row counts for customer_curated and machine_learning_curated in Athena are missing. Visual evidence is vital for comprehensive evaluation.
JOIN Operations:

The Glue Jobs for step_trainer_trusted.py and machine_learning_curated.py need adjustments to incorporate JOIN operations. Follow the feedback provided for more accurate data linking.
Consistency in Node Types:

Consider using the Transform - SQL Query node consistently, as it often provides more reliable outputs than other node types.
🛠️ Helpful Suggestions:

Ensure you reference the correct S3 bucket path in your script for fetching accelerometer data.
When implementing JOIN operations, use the provided hints and consult the AWS Glue documentation for best practices.
Incorporate Athena query result screenshots with clear labels to enhance the visual presentation of your work.
Remember, the learning process is dynamic, and your willingness to improve is admirable! 🌈 Keep up the fantastic work, and if you have any questions or need further assistance, feel free to reach out through the Knowledge Platform. 🚀💡

Landing Zone
customer_landing_to_trusted.py, accelerometer_landing_to_trusted.py, and step_trainer_trusted.py Glue jobs have a node that connects to S3 bucket for customer, accelerometer, and step trainer landing zones.

accelerometer_landing_to_trusted.py retrieves accelerometer records from a predefined catalog, but I couldn't locate any code segment that fetches data from the S3 bucket where the accelerometer records originate.

Ensure you have a section dedicated to accessing data from the specified S3 bucket in your script. The AWS S3 bucket URI housing the accelerometer records is s3://cd0030bucket/accelerometer/. You can directly access this data or create a copy in your personal S3 bucket for utilization. For more information on data, see the Project Data. 🚀

For further guidance, refer to the AWS Glue Studio documentation to better understand Glue Studio features and best practices. In case of access issues, double-check your S3 path and permissions. Errors like AmazonS3Exception: Access denied often point to an incorrect path or insufficient access permissions. 🛠️

SQL DDL scripts customer_landing.sql, accelerometer_landing.sql, and step_trainer_landing.sql include all of the JSON fields in the data input files and are appropriately typed (not everything is a string).

Great job completing the SQL DDL scripts for the customer_landing and accelerometer_landing tables! 🚀 However, it seems like the script for the step_trainer_landing table is currently missing. To ensure comprehensive coverage, include the SQL DDL script for the step_trainer_landing table. 👍

For reference, when manually creating a Glue Table from JSON data, ensure that your SQL DDL scripts (customer_landing.sql, accelerometer_landing.sql, and step_trainer_landing.sql) encompass all the fields present in the corresponding JSON input files. Verify the data types are appropriately assigned, avoiding using generic string types for all fields.

Feel free to consult the Glue Console documentation for guidance on creating tables and defining data types: AWS Glue Console Documentation. This resource provides step-by-step instructions and tips for effectively using the Glue Console to manage your data. 🌟

Include screenshots showing various queries run on Athena, along with their results:

Count of customer_landing: 956 rows
The customer_landing data contains multiple rows with a blank shareWithResearchAsOfDate.
Count of accelerometer_landing: 81273 rows
Count of step_trainer_landing: 28680 rows
Great job! You've successfully fulfilled the requirements for manually creating a Glue Table using the Glue Console from JSON data and querying the Landing Zone with Athena. 👏 Your submitted screenshot effectively demonstrates the completion of the task.

For future reference, consider exploring AWS documentation on Glue and Athena, as these resources can provide additional insights and tips for optimizing queries or handling various data scenarios. Here are the links for your convenience:

AWS Glue Documentation
Amazon Athena Documentation
Trusted Zone
Glue Job Python code shows that the option to dynamically infer and update schema is enabled.

To do this, set the Create a table in the Data Catalog and, on subsequent runs, update the schema and add new partitions option to True.

This criterion requires configuring Glue Studio to update the output data schema dynamically. To achieve this, ensure that the option "Create a table in the Data Catalog and, on subsequent runs, update the schema and add new partitions" is set to True in the AWS Glue Console. 🛠️

image1.png

Ensure your Glue Job Python code reflects this by enabling the dynamic inference and schema update feature. Specifically, set the parameter enableUpdateCatalog to True for the last S3bucket node in your code.

# Example Glue Job Python code snippet
s3_bucket_node = glueContext.create_dynamic_frame.from_catalog(database = "your_database", table_name = "your_table", transformation_ctx = "s3_bucket_node")

# Set the enableUpdateCatalog parameter to True for dynamic schema update
s3_bucket_node.toDF().write.format("parquet").mode("append").partitionBy("your_partition_column").option("enableUpdateCatalog", "true").save("s3://your_output_path")
Remember that your implementation is correct if the updateBehavior is set to UPDATE_IN_DATABASE. If it's set to LOG, be aware that the schema won't be updated on subsequent runs, which might be fine if you're certain the data won't change. However, in many projects, it's advisable to allow dynamic updates for evolving data sources. 🔄

For further details and best practices, refer to the AWS Glue documentation on DynamicFrame Class and GlueContext Methods. 💻✨

Include screenshots showing various queries run on Athena, along with their results:

Count of customer_trusted: 482 rows
The resulting customer_trusted data has no rows where shareWithResearchAsOfDate is blank.
Count of accelerometer_trusted: 40981 rows
Count of step_trainer_trusted: 14460 rows
🌟 Great effort in providing the number of records for the customer_trusted table! However, to meet the criteria fully, we must ensure that the shareWithResearchAsOfDate column in the customer_trusted table contains no blank values. Additionally, it's crucial to include screenshots for the accelerometer_trusted and step_trainer_trusted tables to complete the assessment.

Here are some suggestions to enhance your submission:

Verification of Blank Values:

Execute a SELECT * statement in Athena specifically targeting the customer_trusted table.
Add a WHERE clause to filter rows where shareWithResearchAsOfDate is blank.
Display this query result to showcase that the condition is met.
🚀 Tip: Utilize the following query template in Athena to demonstrate the absence of blank values in shareWithResearchAsOfDate:

SELECT * FROM customer_trusted WHERE shareWithResearchAsOfDate IS NOT NULL AND shareWithResearchAsOfDate <> '';
Include Screenshots:

Capture screenshots of the above query results and ensure they clearly indicate the absence of blank values.
Provide separate screenshots for the count of records in the accelerometer_trusted and step_trainer_trusted tables.
📸 Tip: Organize your screenshots neatly and consider adding brief captions to highlight the specific query or result being showcased.

Count Validation:

Confirm that the counts align with the specified criteria:
customer_trusted: 482 rows with no blank shareWithResearchAsOfDate.
accelerometer_trusted: 40,981 rows.
step_trainer_trusted: 14,460 rows.
📊 Tip: Run COUNT queries for each table to validate the record counts. Example:

SELECT COUNT(*) FROM customer_trusted WHERE shareWithResearchAsOfDate IS NOT NULL AND shareWithResearchAsOfDate <> '';
SELECT COUNT(*) FROM accelerometer_trusted;
SELECT COUNT(*) FROM step_trainer_trusted;
Including these elements will provide a comprehensive and accurate representation of your work, ensuring a successful evaluation. 🚀👩‍💻

customer_landing_to_trusted.py has a node that drops rows that do not have data in the sharedWithResearchAsOfDate column.

Hints:

Transform - SQL Query node often gives more consistent outputs than other node types.
Glue Jobs do not replace any file. Delete your S3 files and Athena table whenever you update your visual ETLs.
Great job including the node in customer_landing_to_trusted.py to filter out rows lacking data in the shareWithResearchAsOfDate column! 👏 This step is crucial for safeguarding protected Personally Identifiable Information (PII) in Spark within Glue Jobs.

Just a friendly suggestion: Considering the Rubric's hint about the Transform - SQL Query node providing more consistent outputs, you might want to explore using it in this context. It often proves beneficial for filtering operations.

Also, remember the importance of managing your S3 files and Athena table updates. Since Glue Jobs doesn't replace files, remember to delete existing S3 files and update the Athena table whenever you modify your visual ETLs. This ensures a smooth and accurate data flow. 🚀

accelerometer_landing_to_trusted.py has a node that inner joins the customer_trusted data with the accelerometer_landing data by emails. The produced table should have only columns from the accelerometer table.

Great job achieving an inner join between the user column in the accelerometer_landing table and the email column in the customer_trusted table using the accelerometer_landing_to_trusted.py script! 🚀 To enhance your submission, consider including only columns from the accelerometer_landing table in the resulting dataset.

For further improvements, you might want to check if the node in your script precisely aligns with the project specifications. Additionally, ensuring that the produced table adheres to the requirement of including only columns from the accelerometer table would be beneficial. 🧐

Here's a helpful reference on inner joins and column selection in Glue Jobs:

Joining datasets - AWS Glue
SelectFields class - AWS Glue
Curated Zone
customer_trusted_to_curated.py has a node that inner joins the customer_trusted data with the accelerometer_trusted data by emails. The produced table should have only columns from the customer table.

Fantastic work! 🚀 You've successfully implemented an inner join in the customer_trusted_to_curated.py Glue Job, linking the user column from the accelerometer_trusted table with the email column from the customer_trusted table. Your SQL statement ensures that the resulting table contains only the columns from the customer table. 👍

For further enrichment, consider exploring the AWS Glue documentation's section on joining data. It provides valuable insights that can deepen your understanding of data joins in the AWS Glue environment. Keep up the impressive work! 🌟

step_trainer_trusted.py has a node that inner joins the step_trainer_landing data with the customer_curated data by serial numbers

machine_learning_curated.py has a node that inner joins the step_trainer_trusted data with the accelerometer_trusted data by sensor reading time and timestamps

Hints:

Data Source - S3 bucket node sometimes extracted incomplete data. Use the Data Source - Data Catalog node when that's the case.
Use the Data Preview feature with at least 500 rows to ensure the number of customer-curated rows is correct. Click "Start data preview session", then click the gear next to the "Filter" text box to update the number of rows
As before, the Transform - SQL Query node often gives more consistent outputs than any other node type. Tip - replace the JOIN node with it.
The step_trainer_trusted may take about 8 minutes to run.
🛠️ Feedback on step_trainer_trusted.py:

Issue Identified:

The script lacks a crucial node for joining the step_trainer_landing data with the customer_curated data using serial numbers.
Suggested Improvement:

To address this, consider adding a node with a JOIN operation based on serial numbers. The Transform - SQL Query node can be effective for this task.
Helpful Tip:

Utilize the Data Preview feature with a substantial number of rows (at least 500) to verify the accuracy of the resulting customer-curated rows. This ensures that the join operation is functioning as intended.
📚 Additional Reference:

You may find the AWS Glue documentation on Joining DynamicFrames helpful for implementing the join operation effectively.
🛠️ Feedback on machine_learning_curated.py:

Issue Identified:

The script is missing a node that joins the step_trainer_trusted data with the accelerometer_trusted data based on sensor reading time and timestamps.
Suggested Improvement:

To rectify this, include a node that performs the necessary JOIN operation using sensor reading time and timestamps. The Transform - SQL Query node is recommended to produce accurate outputs consistently.
Helpful Tip:

Considering the time complexity of step_trainer_trusted (around 8 minutes), plan accordingly for the execution time of the entire script.
📚 Additional Reference:

Explore the AWS Glue documentation on Transforming Data with DynamicFrames to ensure proper implementation of the JOIN operation.
🌟 General Advice:

Always double-check the script execution by using the Data Preview feature to validate the correctness of the resulting data.
While students have the flexibility to choose filenames and nodes, consistency and clarity in naming conventions can improve code readability.
Include screenshots showing various queries run on Athena, along with their results:

Count of customer_curated: 482 rows
Count of machine_learning_curated: 43681 rows
Hint: If you retrieve too many rows, consider dropping duplicates using the Transform - SQL Query node with the SELECT DISTINCT query.

The submission lacks screenshots demonstrating the row counts for customer_curated and machine_learning_curated in Athena. Including visuals of these query results will enhance the clarity and completeness of your assignment. You can use the Athena console to capture the screenshots, select the appropriate database, and run the queries using SQL commands.

Here's a step-by-step guide on how to capture and include screenshots in your submission:

Navigate to Athena Console:

Go to the AWS Management Console.
Open the Athena service.
Select Database:

Choose the database that contains the curated Glue tables.
Run Queries:

For customer_curated, execute the query: SELECT COUNT(*) FROM customer_curated;.
For machine_learning_curated, execute the query: SELECT COUNT(*) FROM machine_learning_curated;.
Capture Screenshots:

Capture screenshots of the Athena query editor with the executed queries and their respective results.
Presentation:

Organize your screenshots neatly in your submission document, ensuring they are legible.
Use headings or captions to clearly label each screenshot, indicating which query result it represents.
Remember, visuals greatly enhance your work's understandability and serve as compelling evidence of task completion. 📊✨

## Acceptance

Meets Specifications
Dear student,

Fantastic job with the STEDI Human Balance Analytics project! 🎉 Your proficiency in navigating the Curated Glue Tables with Athena is commendable. Your query results provide insightful information, indicating the sizes of the customer_curated and machine_learning_curated tables.

To make your project stand out even more, here are some valuable additions:

Filtering Data with Consent Dates: Consider implementing a filter in the Glue Job to join accelerometer readings and customer data, ensuring that only data gathered after the research consent date is included. This proactive step ensures compliance and transparency, especially if consent terms change.

Anonymizing Curated Tables: Enhance data privacy and compliance by anonymizing the final curated table. Removing personally identifiable information, such as email addresses, upfront ensures alignment with GDPR and other privacy regulations. This proactive measure safeguards against potential violations and respects customer privacy rights.

By incorporating these enhancements, your project will meet and exceed industry data integrity and privacy compliance standards. Keep up the excellent work! 🚀 If you have any questions or need further guidance, please reach out through the Knowledge Platform!

Landing Zone
customer_landing_to_trusted.py, accelerometer_landing_to_trusted.py, and step_trainer_trusted.py Glue jobs have a node that connects to S3 bucket for customer, accelerometer, and step trainer landing zones.

SQL DDL scripts customer_landing.sql, accelerometer_landing.sql, and step_trainer_landing.sql include all of the JSON fields in the data input files and are appropriately typed (not everything is a string).

Include screenshots showing various queries run on Athena, along with their results:

Count of customer_landing: 956 rows
The customer_landing data contains multiple rows with a blank shareWithResearchAsOfDate.
Count of accelerometer_landing: 81273 rows
Count of step_trainer_landing: 28680 rows
Trusted Zone
Glue Job Python code shows that the option to dynamically infer and update schema is enabled.

To do this, set the Create a table in the Data Catalog and, on subsequent runs, update the schema and add new partitions option to True.

Include screenshots showing various queries run on Athena, along with their results:

Count of customer_trusted: 482 rows
The resulting customer_trusted data has no rows where shareWithResearchAsOfDate is blank.
Count of accelerometer_trusted: 40981 rows
Count of step_trainer_trusted: 14460 rows
customer_landing_to_trusted.py has a node that drops rows that do not have data in the sharedWithResearchAsOfDate column.

Hints:

Transform - SQL Query node often gives more consistent outputs than other node types.
Glue Jobs do not replace any file. Delete your S3 files and Athena table whenever you update your visual ETLs.
accelerometer_landing_to_trusted.py has a node that inner joins the customer_trusted data with the accelerometer_landing data by emails. The produced table should have only columns from the accelerometer table.

Curated Zone
customer_trusted_to_curated.py has a node that inner joins the customer_trusted data with the accelerometer_trusted data by emails. The produced table should have only columns from the customer table.

step_trainer_trusted.py has a node that inner joins the step_trainer_landing data with the customer_curated data by serial numbers

machine_learning_curated.py has a node that inner joins the step_trainer_trusted data with the accelerometer_trusted data by sensor reading time and timestamps

Hints:

Data Source - S3 bucket node sometimes extracted incomplete data. Use the Data Source - Data Catalog node when that's the case.
Use the Data Preview feature with at least 500 rows to ensure the number of customer-curated rows is correct. Click "Start data preview session", then click the gear next to the "Filter" text box to update the number of rows
As before, the Transform - SQL Query node often gives more consistent outputs than any other node type. Tip - replace the JOIN node with it.
The step_trainer_trusted may take about 8 minutes to run.
Include screenshots showing various queries run on Athena, along with their results:

Count of customer_curated: 482 rows
Count of machine_learning_curated: 43681 rows
Hint: If you retrieve too many rows, consider dropping duplicates using the Transform - SQL Query node with the SELECT DISTINCT query.

Impressive work navigating through the Curated Glue Tables with Athena! 🚀 Your query results reveal that the customer_curated table contains 482 rows, while the machine_learning_curated table boasts 43,681.