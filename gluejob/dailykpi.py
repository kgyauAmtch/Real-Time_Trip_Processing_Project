import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum as _sum, count as _count, min as _min, max as _max, round as _round

# Parameters passed to the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DYNAMODB_TABLE', 'S3_OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dynamodb_table = args['DYNAMODB_TABLE']  
s3_output_path = args['S3_OUTPUT_PATH']  

# Read from DynamoDB table using Glue catalog or via the DynamoDB connector
# Option 1: If you have Glue Catalog table for DynamoDB:
# dyf = glueContext.create_dynamic_frame.from_catalog(database="your_db", table_name="your_table")

# Option 2: Read directly from DynamoDB using spark.read.format (requires DynamoDB connector)
df = spark.read \
    .format("dynamodb") \
    .option("tableName", dynamodb_table) \
    .option("region", "eu-north-1") \
    .load()

# Filter only completed trips (assuming trip_completed is boolean)
df_filtered = df.filter(col("trip_completed") == True)

# Select relevant columns and cast fare_amount to float/decimal
df_selected = df_filtered.select(
    col("trip_date").alias("trip_date"),
    col("fare_amount").cast("double").alias("fare_amount")
).filter(col("trip_date").isNotNull() & col("fare_amount").isNotNull())

# Aggregate KPIs per trip_date
kpi_df = df_selected.groupBy("trip_date").agg(
    _count("fare_amount").alias("count_trips"),
    _sum("fare_amount").alias("total_fare"),
    _min("fare_amount").alias("min_fare"),
    _max("fare_amount").alias("max_fare")
)

# Calculate average fare
kpi_df = kpi_df.withColumn("average_fare", _round(col("total_fare") / col("count_trips"), 2)) \
               .withColumn("total_fare", _round(col("total_fare"), 2)) \
               .withColumn("min_fare", _round(col("min_fare"), 2)) \
               .withColumn("max_fare", _round(col("max_fare"), 2))

# Write KPIs partitioned by year/month/day extracted from trip_date
from pyspark.sql.functions import year, month, dayofmonth

kpi_df = kpi_df.withColumn("year", year(col("trip_date"))) \
               .withColumn("month", month(col("trip_date"))) \
               .withColumn("day", dayofmonth(col("trip_date")))

# Write to S3 as Parquet or JSON (Parquet recommended for analytics)
kpi_df.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(s3_output_path)

job.commit()
