import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1705854855820 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://eli-stedi-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1705854855820",
)

# Script generated for node Share With Research
ShareWithResearch_node1705860899330 = Filter.apply(
    frame=CustomerLanding_node1705854855820,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ShareWithResearch_node1705860899330",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1705861571522 = glueContext.write_dynamic_frame.from_options(
    frame=ShareWithResearch_node1705860899330,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://eli-stedi-lake-house/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedCustomerZone_node1705861571522",
)

job.commit()