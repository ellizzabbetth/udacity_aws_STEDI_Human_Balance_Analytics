# "trainer_trusted_to_curated" is "machine_learning_curated" in the project.
# As shown in the Project Instructions page, the name is step_trainer_trusted.
# https://knowledge.udacity.com/questions/1019539
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Trainer Trusted
TrainerTrusted_node1706214731320 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="trainer_trusted",
    transformation_ctx="TrainerTrusted_node1706214731320",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1706117410748 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1706117410748",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from 
at inner join tt 
on at.timestamp = tt.sensorreadingtime
"""
SQLQuery_node1706214183250 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "at": AccelerometerTrusted_node1706117410748,
        "tt": TrainerTrusted_node1706214731320,
    },
    transformation_ctx="SQLQuery_node1706214183250",
)

# Script generated for node Amazon S3
AmazonS3_node1706212299322 = glueContext.getSink(
    path="s3://eli-stedi-lake-house/trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1706212299322",
)
AmazonS3_node1706212299322.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
AmazonS3_node1706212299322.setFormat("json")
AmazonS3_node1706212299322.writeFrame(SQLQuery_node1706214183250)
job.commit()
