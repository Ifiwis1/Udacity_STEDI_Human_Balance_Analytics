import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1776720744665 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1776720744665")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1776720739730 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1776720739730")

# Script generated for node Join
SqlQuery69 = '''
select * from ct
join at
on ct.email = at.user
'''
Join_node1776723561103 = sparkSqlQuery(glueContext, query = SqlQuery69, mapping = {"ct":CustomerTrusted_node1776720744665, "at":AccelerometerTrusted_node1776720739730}, transformation_ctx = "Join_node1776723561103")

# Script generated for node Drop Fields
SqlQuery68 = '''
select distinct
    customerName,
    email,
    phone,
    birthDay,
    serialNumber,
    registrationDate,
    lastUpdateDate,
    shareWithResearchAsOfDate,
    shareWithPublicAsOfDate,
    shareWithFriendsAsOfDate
from myDataSource
'''
DropFields_node1776721486685 = sparkSqlQuery(glueContext, query = SqlQuery68, mapping = {"myDataSource":Join_node1776723561103}, transformation_ctx = "DropFields_node1776721486685")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1776721486685, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776719196300", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1776720769828 = glueContext.getSink(path="s3://ifiokudacitybucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1776720769828")
AccelerometerTrusted_node1776720769828.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AccelerometerTrusted_node1776720769828.setFormat("json")
AccelerometerTrusted_node1776720769828.writeFrame(DropFields_node1776721486685)
job.commit()