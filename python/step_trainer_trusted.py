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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1776720744665 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1776720744665")

# Script generated for node Customer Trusted
CustomerTrusted_node1776720739730 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1776720739730")

# Script generated for node Join
SqlQuery414 = '''
SELECT *
FROM st
JOIN ct on st.serialnumber = ct.serialnumber
'''
Join_node1776723561103 = sparkSqlQuery(glueContext, query = SqlQuery414, mapping = {"st":StepTrainerLanding_node1776720744665, "ct":CustomerTrusted_node1776720739730}, transformation_ctx = "Join_node1776723561103")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1776723561103, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776719196300", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1776720769828 = glueContext.getSink(path="s3://ifiokudacitybucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1776720769828")
StepTrainerTrusted_node1776720769828.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted ")
StepTrainerTrusted_node1776720769828.setFormat("json")
StepTrainerTrusted_node1776720769828.writeFrame(Join_node1776723561103)
job.commit()