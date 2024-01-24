import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing Data Catalog
AccelerometerLandingDataCatalog_node1705955091146 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_landing",
        transformation_ctx="AccelerometerLandingDataCatalog_node1705955091146",
    )
)

# Script generated for node Customer Trusted Data Catalog
CustomerTrustedDataCatalog_node1705955181248 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="customer_trusted",
        transformation_ctx="CustomerTrustedDataCatalog_node1705955181248",
    )
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1705955222181 = Join.apply(
    frame1=AccelerometerLandingDataCatalog_node1705955091146,
    frame2=CustomerTrustedDataCatalog_node1705955181248,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1705955222181",
)

# Script generated for node Drop Fields
DropFields_node1705955506348 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1705955222181,
    paths=[
        "customername",
        "email",
        "phone",
        "birthdate",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1705955506348",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705956559192 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1705955506348,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eli-stedi-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1705956559192",
)

job.commit()
