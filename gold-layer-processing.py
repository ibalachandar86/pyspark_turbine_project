# Load Stats data/Anamolies into Gold layer

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName('TurbineStats').getOrCreate()

# Calculate Aggregate data
aggData = spark.read.table("gim_curated_db.tursilverdata") \
          .groupBy("turb_dt", "turbine_id") \
          .agg(min("power_output").alias("min_power_output"), \
          .agg(max("power_output").alias("max_power_output"), \
          .agg(avg("power_output").alias("avg_power_output"), \
          .agg(mean("power_output").alias("mean_power_output"), \
          .orderBy("turb_dt", "turbine_id")

# Create Window function for data wise stats and to calculate standard deviation and anomalies
window_set = Window.partitionBy("turb_dt")
anamolyData = aggData \
.withColumn("exp_2_stddev_power_output", (stddev("avg_power_output").over(window_set)) * 2) \
.withColumn("exp_lower_bound_power_output", (col("mean_power_output") - col("exp_2_stddev_power_output"))) \
.withColumn("exp_upper_bound_power_output", (col("mean_power_output") + col("exp_2_stddev_power_output"))) \
.withColumn("anomaly", when ((col('avg_power_output') < col('exp_lower_bound_power_output')) | ((col('avg_power_output') > col('exp_upper_bound_power_output')), 'YES').otherwise('NO'))

# Drop and Create table if exists
spark.sql("drop table if exists test.turbgoldstatsdata")
spark.sql("create table test.turbgoldstatsdata ( turb_dt DATE, turbine_id INT, min_power_output DECIMAL(5,1), max_power_output DECIMAL(5,1), avg_power_output DECIMAL(9,5), mean_power_output DECIMAL(9,5), exp_2_stddev_power_output double, exp_lower_bound_power_output double, exp_upper_bound_power_output double, anomaly String) PARTITIONED BY (turb_dt)")

# Write data in to Gold layer table
anamolyData.write.mode("overwrite").partitionBy("turb_dt").saveAsTable("test.turbgoldstatsdata")
