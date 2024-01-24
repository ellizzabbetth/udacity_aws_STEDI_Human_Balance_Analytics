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

# Script generated for node Customer Trusted
CustomerTrusted_node1706115188196 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1706115188196",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1706115242075 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1706115242075",
)

# Script generated for node Trainer Landing
TrainerLanding_node1706115394249 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eli-stedi-lake-house/trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="TrainerLanding_node1706115394249",
)

# Script generated for node Join
Join_node1706115519909 = Join.apply(
    frame1=AccelerometerLanding_node1706115242075,
    frame2=CustomerTrusted_node1706115188196,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706115519909",
)

# Script generated for node Join
Join_node1706115698776 = Join.apply(
    frame1=Join_node1706115519909,
    frame2=TrainerLanding_node1706115394249,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1706115698776",
)

# Script generated for node Drop Fields
DropFields_node1706116008830 = DropFields.apply(
    frame=Join_node1706115698776,
    paths=["user", "timestamp", "x", "y", "z", "phone", "email"],
    transformation_ctx="DropFields_node1706116008830",
)

# Script generated for node Trainer Trusted
TrainerTrusted_node1706116153085 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706116008830,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eli-stedi-lake-house/trainer/trusted/",
        "compression": "uncompressed",
        "partitionKeys": [],
    },
    transformation_ctx="TrainerTrusted_node1706116153085",
)

job.commit()