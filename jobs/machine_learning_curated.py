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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1727046988871 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1727046988871")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1727046966341 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1727046966341")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource1 as ds1 inner join myDataSource2 as ds2 on ds1.sensorreadingtime = ds2.`timestamp`
'''
SQLQuery_node1727047024496 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource1":StepTrainerTrusted_node1727046966341, "myDataSource2":AccelerometerTrusted_node1727046988871}, transformation_ctx = "SQLQuery_node1727047024496")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1727047599370 = glueContext.getSink(path="s3://stedi-lh/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1727047599370")
MachineLearningCurated_node1727047599370.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1727047599370.setFormat("json")
MachineLearningCurated_node1727047599370.writeFrame(SQLQuery_node1727047024496)
job.commit()