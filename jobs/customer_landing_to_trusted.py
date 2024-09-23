import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1727043230354 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1727043230354")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1727043395247 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1727043395247")

# Script generated for node Join
Join_node1727043934959 = Join.apply(frame1=AccelerometerLanding_node1727043395247, frame2=CustomerTrusted_node1727043230354, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1727043934959")

# Script generated for node Drop Accelerometer Fields
DropAccelerometerFields_node1727043992188 = ApplyMapping.apply(frame=Join_node1727043934959, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "bigint", "registrationdate", "bigint"), ("lastupdatedate", "bigint", "lastupdatedate", "bigint"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "bigint"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "bigint")], transformation_ctx="DropAccelerometerFields_node1727043992188")

# Script generated for node Drop Duplicates
DropDuplicates_node1727044092950 =  DynamicFrame.fromDF(DropAccelerometerFields_node1727043992188.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1727044092950")

# Script generated for node Customer Curated
CustomerCurated_node1727044104643 = glueContext.getSink(path="s3://stedi-lh/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1727044104643")
CustomerCurated_node1727044104643.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1727044104643.setFormat("json")
CustomerCurated_node1727044104643.writeFrame(DropDuplicates_node1727044092950)
job.commit()