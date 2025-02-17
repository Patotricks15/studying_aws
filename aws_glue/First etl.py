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

# Script generated for node Amazon S3
AmazonS3_node1739818394855 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://patotricks-bucket-example/iris_dataset.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1739818394855")

# Script generated for node SQL Query
SqlQuery978 = '''
select *,
sepal_ratio / petal_ratio as sepal_petal_ratio
from myDataSource

'''
SQLQuery_node1739818411998 = sparkSqlQuery(glueContext, query = SqlQuery978, mapping = {"myDataSource":AmazonS3_node1739818394855}, transformation_ctx = "SQLQuery_node1739818411998")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1739818411998, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739818057393", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739818656692 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1739818411998, connection_type="s3", format="glueparquet", connection_options={"path": "s3://patotricks-bucket-example", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1739818656692")

job.commit()