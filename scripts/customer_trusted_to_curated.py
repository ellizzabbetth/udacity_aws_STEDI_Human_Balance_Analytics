# https://knowledge.udacity.com/questions/1013185

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1706102787293 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1706102787293",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1706102937185 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1706102937185",
)

# Script generated for node Join
Join_node1706102990260 = Join.apply(
    frame1=AccelerometerTrusted_node1706102937185,
    frame2=CustomerTrusted_node1706102787293,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706102990260",
)

# Script generated for node Drop Fields
DropFields_node1706198474639 = DropFields.apply(
    frame=Join_node1706102990260,
    paths=["user", "y", "z", "x", "timestamp"],
    transformation_ctx="DropFields_node1706198474639",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1706188242941 = DynamicFrame.fromDF(
    DropFields_node1706198474639.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1706188242941",
)

# Script generated for node customer_curated
customer_curated_node1706103215252 = glueContext.getSink(
    path="s3://eli-stedi-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1706103215252",
)
customer_curated_node1706103215252.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
customer_curated_node1706103215252.setFormat("json")
customer_curated_node1706103215252.writeFrame(DropDuplicates_node1706188242941)
job.commit()
