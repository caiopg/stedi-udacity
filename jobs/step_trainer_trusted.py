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

# Script generated for node Customer Curated
CustomerCurated_node1727045029997 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1727045029997")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1727044987160 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1727044987160")

# Script generated for node Join
SqlQuery0 = '''
select * from myDataSource1 as ds1 inner join myDataSource2 as ds2 on ds1.serialNumber = ds2.serialNumber
'''
Join_node1727045945032 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource1":StepTrainerLanding_node1727044987160, "myDataSource2":CustomerCurated_node1727045029997}, transformation_ctx = "Join_node1727045945032")

# Script generated for node Change Schema
ChangeSchema_node1727045164336 = ApplyMapping.apply(frame=Join_node1727045945032, mappings=[("sensorReadingTime", "bigint", "sensorReadingTime", "bigint"), ("serialNumber", "string", "serialNumber", "string"), ("distanceFromObject", "int", "distanceFromObject", "int")], transformation_ctx="ChangeSchema_node1727045164336")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1727045239208 = glueContext.getSink(path="s3://stedi-lh/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1727045239208")
StepTrainerTrusted_node1727045239208.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1727045239208.setFormat("json")
StepTrainerTrusted_node1727045239208.writeFrame(ChangeSchema_node1727045164336)
job.commit()