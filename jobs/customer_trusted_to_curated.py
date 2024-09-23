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

# Script generated for node Customer Landing
CustomerLanding_node1727038776755 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1727038776755")

# Script generated for node Privacy Filter
SqlQuery0 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null
'''
PrivacyFilter_node1727039079702 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1727038776755}, transformation_ctx = "PrivacyFilter_node1727039079702")

# Script generated for node Customer Trusted
CustomerTrusted_node1727039126297 = glueContext.getSink(path="s3://stedi-lh/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1727039126297")
CustomerTrusted_node1727039126297.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1727039126297.setFormat("json")
CustomerTrusted_node1727039126297.writeFrame(PrivacyFilter_node1727039079702)
job.commit()