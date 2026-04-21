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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1776720744665 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted ", transformation_ctx="StepTrainerTrusted_node1776720744665")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1776720739730 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1776720739730")

# Script generated for node Join
SqlQuery302 = '''
select * 
from a
join t on t.sensorreadingtime = a.timestamp;
'''
Join_node1776723561103 = sparkSqlQuery(glueContext, query = SqlQuery302, mapping = {"t":StepTrainerTrusted_node1776720744665, "a":AccelerometerTrusted_node1776720739730}, transformation_ctx = "Join_node1776723561103")

# Script generated for node SQL Query
SqlQuery301 = '''
SELECT serialNumber, sensorReadingTime, distanceFromObject,  x,  y,  z
FROM myDataSource;
'''
SQLQuery_node1776731223579 = sparkSqlQuery(glueContext, query = SqlQuery301, mapping = {"myDataSource":Join_node1776723561103}, transformation_ctx = "SQLQuery_node1776731223579")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1776731223579, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776719196300", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1776720769828 = glueContext.getSink(path="s3://ifiokudacitybucket/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1776720769828")
MachineLearningCurated_node1776720769828.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1776720769828.setFormat("json")
MachineLearningCurated_node1776720769828.writeFrame(SQLQuery_node1776731223579)
job.commit()