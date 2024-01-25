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

# Script generated for node Customer Trusted
CustomerTrusted_node1705955181248 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1705955181248",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1705955222181 = Join.apply(
    frame1=AccelerometerLandingDataCatalog_node1705955091146,
    frame2=CustomerTrusted_node1705955181248,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node1705955222181",
)

# Script generated for node Drop Fields
DropFields_node1705955506348 = DropFields.apply(
    frame=CustomerPrivacyFilter_node1705955222181,
    paths=[
        "email",
        "phone",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "customerName",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "birthDay",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1705955506348",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705956559192 = glueContext.getSink(
    path="s3://eli-stedi-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1705956559192",
)
AccelerometerTrusted_node1705956559192.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1705956559192.setFormat("json")
AccelerometerTrusted_node1705956559192.writeFrame(DropFields_node1705955506348)
job.commit()
