# "trainer_trusted_to_curated" is "machine_learning_curated" in the project.

# As shown in the Project Instructions page, the name is step_trainer_trusted.
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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1706117410748 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1706117410748",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1706117501582 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1706117501582",
)

# Script generated for node Trainer Landing
TrainerLanding_node1706117547769 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="trainer_landing",
    transformation_ctx="TrainerLanding_node1706117547769",
)

# Script generated for node Join
Join_node1706117589939 = Join.apply(
    frame1=AccelerometerLanding_node1706117410748,
    frame2=CustomerTrusted_node1706117501582,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706117589939",
)

# Script generated for node Join
Join_node1706117825353 = Join.apply(
    frame1=Join_node1706117589939,
    frame2=TrainerLanding_node1706117547769,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1706117825353",
)

# Script generated for node Amazon S3
AmazonS3_node1706117908112 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1706117825353,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eli-stedi-lake-house/trainer/curated/",
        "compression": "uncompressed",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1706117908112",
)

job.commit()