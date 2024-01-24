import sys, os

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from dotenv import load_dotenv
load_dotenv()
print(os.environ['S3'])



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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1706102937185 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1706102937185",
)

# Script generated for node Join
Join_node1706102990260 = Join.apply(
    frame1=AccelerometerLanding_node1706102937185,
    frame2=CustomerTrusted_node1706102787293,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706102990260",
)

# Script generated for node Drop Fields
DropFields_node1706103028455 = DropFields.apply(
    frame=Join_node1706102990260,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1706103028455",
)

# Script generated for node customer_curated
customer_curated_node1706103215252 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706103028455,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eli-stedi-lake-house/customer/curated/",
        "compression": "uncompressed",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node1706103215252",
)

job.commit()
