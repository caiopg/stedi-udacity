import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1726872020356 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1726872020356")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1726872015446 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lh/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1726872015446")

# Script generated for node Filter Customer Trusted
FilterCustomerTrusted_node1726872074939 = Join.apply(frame1=AccelerometerLanding_node1726872015446, frame2=CustomerTrusted_node1726872020356, keys1=["user"], keys2=["email"], transformation_ctx="FilterCustomerTrusted_node1726872074939")

# Script generated for node Drop Customer Fields
DropCustomerFields_node1727041453530 = ApplyMapping.apply(frame=FilterCustomerTrusted_node1726872074939, mappings=[("user", "string", "user", "string"), ("timestamp", "bigint", "timestamp", "bigint"), ("x", "double", "x", "float"), ("y", "double", "y", "float"), ("z", "double", "z", "float"), ("customername", "string", "customername", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "bigint", "registrationdate", "bigint"), ("lastupdatedate", "bigint", "lastupdatedate", "bigint"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "bigint"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "bigint")], transformation_ctx="DropCustomerFields_node1727041453530")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1726873247382 = glueContext.getSink(path="s3://stedi-lh/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1726873247382")
AccelerometerTrusted_node1726873247382.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1726873247382.setFormat("json")
AccelerometerTrusted_node1726873247382.writeFrame(DropCustomerFields_node1727041453530)
job.commit()