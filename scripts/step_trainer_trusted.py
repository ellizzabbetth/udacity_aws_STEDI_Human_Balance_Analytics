# https://knowledge.udacity.com/questions/1013294
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

# Script generated for node Trainer Landing
TrainerLanding_node1706115242075 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="trainer_landing",
    transformation_ctx="TrainerLanding_node1706115242075",
)

# Script generated for node Customer Curated
CustomerCurated_node1706115188196 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1706115188196",
)

# Script generated for node JOIN trainer_landing and customer_curated
SqlQuery1 = """
select trainer_landing.sensorreadingtime, 
trainer_landing.serialnumber, 
trainer_landing.distancefromobject
from trainer_landing, customer_curated
where trainer_landing.serialnumber = customer_curated.serialnumber
"""
JOINtrainer_landingandcustomer_curated_node1706210240043 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={
        "trainer_landing": TrainerLanding_node1706115242075,
        "customer_curated": CustomerCurated_node1706115188196,
    },
    transformation_ctx="JOINtrainer_landingandcustomer_curated_node1706210240043",
)

# Script generated for node Distinct Query
SqlQuery0 = """
select distinct * from myDataSource
"""
DistinctQuery_node1706210349828 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": JOINtrainer_landingandcustomer_curated_node1706210240043},
    transformation_ctx="DistinctQuery_node1706210349828",
)

# Script generated for node S3 trainer_trusted
S3trainer_trusted_node1706210423074 = glueContext.getSink(
    path="s3://eli-stedi-lake-house/trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3trainer_trusted_node1706210423074",
)
S3trainer_trusted_node1706210423074.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="trainer_trusted"
)
S3trainer_trusted_node1706210423074.setFormat("json")
S3trainer_trusted_node1706210423074.writeFrame(DistinctQuery_node1706210349828)
job.commit()
