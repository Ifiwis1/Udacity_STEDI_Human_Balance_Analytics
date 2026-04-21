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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1776720739730 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1776720739730")

# Script generated for node SQL Query
SqlQuery290 = '''
select * from ct
join al
on ct.email = al.user
where al.timestamp >= ct.shareWithResearchAsOfDate
'''
SQLQuery_node1776726000121 = sparkSqlQuery(glueContext, query = SqlQuery290, mapping = {"ct":CustomerTrusted_node1776720744665, "al":AccelerometerLanding_node1776720739730}, transformation_ctx = "SQLQuery_node1776726000121")

# Script generated for node Drop Fields
SqlQuery289 = '''
select user, timestamp, x, y, z from myDataSource

'''
DropFields_node1776721486685 = sparkSqlQuery(glueContext, query = SqlQuery289, mapping = {"myDataSource":SQLQuery_node1776726000121}, transformation_ctx = "DropFields_node1776721486685")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1776721486685, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776719196300", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1776720769828 = glueContext.getSink(path="s3://ifiokudacitybucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1776720769828")
AccelerometerTrusted_node1776720769828.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1776720769828.setFormat("json")
AccelerometerTrusted_node1776720769828.writeFrame(DropFields_node1776721486685)
job.commit()