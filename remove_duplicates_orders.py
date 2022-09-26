import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
import logging
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.types import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "delta_lake_s3_path",
        "primary_keys",
        "filter_date"
    ],
)

# Assign inputs to variables
delta_lake_s3_path = args["delta_lake_s3_path"]
primary_keys = args["primary_keys"]
filter_date = args["filter_date"]
primary_keys = primary_keys.split(',')
logger.info(
    f"[GLUE LOG] - arguments received are : {args}"
)
confa = (
    pyspark.SparkConf()
        .setAppName("dups_delete_glue_job")

)
sc = SparkContext(conf=confa)
spark = SparkSession.builder.appName("testing_file").getOrCreate()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
# Add jar file in filepath
sc.addPyFile("delta-core_2.12-1.0.0.jar")

from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

deltaTable = DeltaTable.forPath(spark, delta_lake_s3_path)
deltaTable.toDF().show()
df = deltaTable.toDF()
logger.info(
    f"[GLUE LOG] - here is the read frame : {df.columns}"
)
logger.info(
    f"[GLUE LOG] - total number of before restore records: {df.count()}"
)

# Read history of delta table
deltaTable.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
# Define partition window to select the correct record

windowSpec = Window.partitionBy(primary_keys).orderBy(filter_date)
# Select single record based on row number created using partitions

df = df.withColumn("row_number", row_number().over(windowSpec))
df.show(truncate=False)
df = df.filter(df.row_number == "1")

# Store version of table to use later while inserting single records
version_before_change = deltaTable.history().select("version")
version_before_change_int = version_before_change.collect()[0]["version"]

# Creating join condition based on primary keys passed. For composite keys , comma separated values can be passed
join_condition = ""
for i, col in enumerate(primary_keys):
    join_condition += f"main.{col} <=>nodups.{col} "
    if i < (len(primary_keys) - 1):
        join_condition += " and \n"
logger.info(
    f"[GLUE LOG] - join_condition: {join_condition}"
)

# Delete all records
deltaTable.alias("main").merge(
    df.alias("nodups"),
    join_condition).whenMatchedDelete().execute()
logger.info(
    f"[GLUE LOG] - Starting to insert the records now:"
)

logger.info(
    f"[GLUE LOG] - reading previous version:"
)
df = spark.read.format("delta").option("versionAsOf", version_before_change_int).load(delta_lake_s3_path)
logger.info(
    f"[GLUE LOG] - overwriting with previous version:"
)
# Select single record from previous version of table
df = df.withColumn("row_number", row_number().over(windowSpec))
df = df.filter(df.row_number == "1")
df.show()
df = df.drop("row_number")

# Insert single records back into the table
deltaTable.alias("main").merge(
    df.alias("nodups"),
    join_condition).whenNotMatchedInsertAll().execute()
df = deltaTable.toDF()
logger.info(
    f"[GLUE LOG] - Total number of records after final insert of deduplication: {df.count()}"
)

# Generate symlink manifest file for athena to read the updated version without duplicates
deltaTable.generate("symlink_format_manifest")
logger.info(
    f"[GLUE LOG] - Final history after deduplication is: {df.count()}"
)
deltaTable.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
job.commit()
