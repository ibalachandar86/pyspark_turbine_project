# Load Turbine CSV file into Silver Layer
# Clean the data remove all duplicates and drop the data if any of the column has null values

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName('TurbineStats').getOrCreate()

# CSV File Options
options = {
  "header": "true",
  "delimiter": ","
}

# Silver data schema
turbSilverSchema = StructType() \
  .add("timestamp", TimestampType()) \
  .add("turbine_id", IntegerType()) \
  .add("wind_speed", DecimalType(5,1)) \
  .add("wind_direction", DecimalType(5,1)) \
  .add("power_output", DecimalType(5,1))

# Read the CSV file
raw_data = spark.read.format("csv").options(**options).load("dbfs:/FileStore/jars/temp/")

# Drop Duplicates and Drop the rows with null values in any column
raw_data_na = raw_data.dropDuplicates().na.drop()
raw_data_na.createOrReplaceTempView("raw_data_na")

# Format the RAW data
final_raw_data = spark.sql("select t1.timestamp,t1.turbine_id,t1.wind_speed,t1.wind_direction,t1.power_output,cast(t1.timestamp as date) as turb_dt from (select to_timestamp(timestamp, 'MM/dd/yyyy HH:mm:ss') as timestamp, cast(turbine_id as integer) as turbine_id, cast(wind_speed as decimal(5,1)) as wind_speed, cast(wind_direction as decimal(5,1)) as wind_direction, ast(power_output as decimal(5,1)) as power_output from raw_data_na) as t1")

# Drop and Create table if exists
spark.sql("drop table if exists test.turbsilverdata")
spark.sql("create table test.turbsilverdata (timestamp TIMESTAMP, turbine_id INT, wind_speed DECIMAL(5,1), wind_direction DECIMAL(5,1), power_output DECIMAL(5,1), turb_dt DATE) PARTITIONED BY (turb_dt, turbine_id)")

# Write data in to Silver layer table
final_raw_data.write.mode("overwrite").partitionBy("turb_dt", "turbine_id").saveAsTable("test.turbsilverdata")
