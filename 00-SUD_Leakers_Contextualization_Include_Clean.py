# Databricks notebook source
# MAGIC %md
# MAGIC # SUD Leakers - Data Contextualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialization

# COMMAND ----------

# Restart Python
dbutils.library.restartPython()

# COMMAND ----------

import sys
import os
import pytz
import json
import datetime, time
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql import SparkSession


# COMMAND ----------

# Remove and load all widgets
try:
    dbutils.widgets.removeAll()
except Exception as e:
    logger.error(f"Failed to remove widgets: {e}")
else:
    dbutils.widgets.text("lines", "LINE 28", "")
    lines = dbutils.widgets.get("lines")

    dbutils.widgets.text("start_datetime", "2023-02-05T00:00:00.000", "")
    start_datetime = dbutils.widgets.get("start_datetime")

    dbutils.widgets.text("end_datetime", "2023-02-06T00:00:00.000", "")
    end_datetime = dbutils.widgets.get("end_datetime")

    dbutils.widgets.text(
        "lookback_hours", "36", ""
    )  # to reduce number of historian records lookback hrs can go from 48 to 36

    lookback_hours = int(dbutils.widgets.get("lookback_hours"))

# COMMAND ----------

# Define historian data time format
HISTORIAN_DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

historian_start_datetime = (
    datetime.datetime.strptime(start_datetime, HISTORIAN_DATE_TIME_FORMAT)
    - datetime.timedelta(hours=lookback_hours)
).strftime(HISTORIAN_DATE_TIME_FORMAT)

lines_to_include = lines.split(",")

input_table_name = "cccr_test"
output_table_name = ""  # TBD TODO: add contextualization target table here

# Print parameter values
params = {
    "input_table_name": input_table_name,
    "lines": lines,
    "start_datetime": start_datetime,
    "end_datetime": end_datetime,
    "lookback_hours": lookback_hours,
    "historian_start_datetime": historian_start_datetime,
    "ouput_table": output_table_name,
}
print("Params:", json.dumps(params, indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Include

# COMMAND ----------

# MAGIC %run ./01-SUD_Leakers_Contextualization_Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Buckets data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Define source query for CCCR data
cccrdb_query = (
    f"SELECT cast(Trigger_Timestamp as Timestamp), id, Machine, REPLACE(line, 'LINE ', '') as line FROM groupdb_fhc_sud_leakers."
    + input_table_name
    + " WHERE Trigger_Timestamp > '"
    + start_datetime
    + "' AND Trigger_Timestamp < '"
    + end_datetime
    + "' AND Line IN ('"
    + lines
    + "')"
)

# Create df_buckets dataframe
df_buckets = spark.sql(cccrdb_query)

df_buckets = df_buckets.select(
    "*",
    unix_timestamp(df_buckets["Trigger_Timestamp"]).alias("epoch_Trigger_Timestamp"),
)

# Calculate the total number of rows and desired chunk size
total_rows = df_buckets.count()
chunk_size = 7200  # Set your desired chunk size here

# Assign unique row IDs using monotonically_increasing_id()
df_buckets = df_buckets.withColumn("counter", monotonically_increasing_id())
w = Window.orderBy("Trigger_Timestamp")

# Use row number with the window specification
df_buckets= df_buckets.withColumn("row_id", row_number().over(w))
df_buckets = df_buckets.drop("counter")

# Calculate the chunk number based on row_id
df_buckets = df_buckets.withColumn("chunk_number", floor(df_buckets["row_id"] / chunk_size))

df_buckets = df_buckets.repartition(spark.sparkContext.defaultParallelism)
df_buckets.persist(pyspark.StorageLevel.MEMORY_ONLY).count()
df_buckets.orderBy('row_id').show(n=5, truncate=False)

# Check df_buckets dataframe size
if df_buckets.count() == 0:
    dbutils.notebook.exit(
        f"The CCCR table does not contain records for {lines} between {start_datetime} and {end_datetime}"
    )
else:
    pass

print(f"Dimension of df_buckets is: {(df_buckets.count(),len(df_buckets.columns))}") # TODO: Clean-up remove to avoid count operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate df_buckets schema

# COMMAND ----------

# Define the expected schema for df_buckets
expected_schema = StructType(
    [
        StructField("Trigger_Timestamp", TimestampType(), True),
        StructField("id", LongType(), True),
        StructField("Machine", StringType(), True),
        StructField("line", StringType(), True),
        StructField("epoch_Trigger_Timestamp", LongType(), True),
        StructField("row_id", IntegerType(), False),
        StructField("chunk_number", LongType(), True)
    ]
)

# Get the schema of the DataFrame
actual_schema = df_buckets.schema

# Compare the actual schema with the expected schema and print the result
if actual_schema == expected_schema:
    print("df_buckets schema validation passed!")
else:
    dbutils.notebook.exit(f"The df_buckets data frame does not have proper schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Historian data

# COMMAND ----------

historian_lines_to_include = [int(x.split()[1]) for x in lines.split(", ")]
print(historian_lines_to_include)

# COMMAND ----------

# Load Historian data
from pyspark.sql.functions import *
from pyspark.sql.types import *

# the tag_names list - 39 tags
tag_names = []
for lineNumber in historian_lines_to_include:
    tag_names = tag_names + [
        f"AMI-CONV.L{lineNumber}_VEPDA_Primary_BucketID_AtInfeedChute",
        f"AMI-CONV.L{lineNumber}_VEPDB_Primary_BucketID_AtInfeedChute",
        f"AMI-CONV.L{lineNumber}_Current_Machine_Speed",
        f"AMI-CONV.L{lineNumber}_VEC_Sealing_Bottom_NectarShell_Age_Actual_km",
        f"AMI-CONV.L{lineNumber}_Diverter_Position",
        f"AMI-CONV.L{lineNumber}_Machine_Speed_Setpoint",
        f"AMI-CONV.L{lineNumber}_PackML_Execute",
        f"AMI-CONV.L{lineNumber}_Middle_Unwinder_Current_Diameter_Left",
        f"AMI-CONV.L{lineNumber}_Middle_Unwinder_Current_Diameter_Right",
        f"AMI-CONV.L{lineNumber}_Middle_Unwinder_Dancer_Position",
        f"AMI-CONV.L{lineNumber}_Middle_Unwinder_Rotary_Speed_Left",
        f"AMI-CONV.L{lineNumber}_Middle_Unwinder_Rotary_Speed_Right",
        f"AMI-CONV.L{lineNumber}_Middle_Unwinder_SpliceInProgress",
        f"AMI-CONV.L{lineNumber}_middleUnwindleftspliceactive",
        f"AMI-CONV.L{lineNumber}_MiddleUnwindrightspliceactive",
        f"AMI-CONV.L{lineNumber}_Top_Unwinder_Current_Diameter_Left",
        f"AMI-CONV.L{lineNumber}_Top_Unwinder_Current_Diameter_Right",
        f"AMI-CONV.L{lineNumber}_Top_Unwinder_Dancer_Position",
        f"AMI-CONV.L{lineNumber}_Top_Unwinder_Rotary_Speed_Left",
        f"AMI-CONV.L{lineNumber}_Top_Unwinder_Rotary_Speed_Right",
        f"AMI-CONV.L{lineNumber}_Top_Unwinder_SpliceInProgress",
        f"AMI-CONV.L{lineNumber}_TopUnwindleftspliceactive",
        f"AMI-CONV.L{lineNumber}_TopUnwindrightspliceactive",
        f"AMI-CONV.L{lineNumber}_Bottom_Unwinder_Current_Diameter_Left",
        f"AMI-CONV.L{lineNumber}_Bottom_Unwinder_Current_Diameter_Right",
        f"AMI-CONV.L{lineNumber}_Bottom_Unwinder_Dancer_Position",
        f"AMI-CONV.L{lineNumber}_Bottom_Unwinder_SpliceInProgress",
        f"AMI-CONV.L{lineNumber}_Bottom_Unwinder_Rotary_Speed_Left",
        f"AMI-CONV.L{lineNumber}_Bottom_Unwinder_Rotary_Speed_Right",
        f"AMI-CONV.L{lineNumber}_BottomUnwindleftspliceactive",
        f"AMI-CONV.L{lineNumber}_BottomUnwindrightspliceactive",
        f"AMI-CONV.L{lineNumber}_VEC_WebHandling_Bottom_PVA_GCAS_Actual_#",
        f"AMI-CONV.L{lineNumber}_VEC_WebHandling_Middle_PVA_GCAS_Actual_#",
        f"AMI-CONV.L{lineNumber}_VEC_WebHandling_Top_PVA_GCAS_Actual_#",
        f"AMI-CONV.L{lineNumber}_VEC_WebHandling_Bottom_PVA_Roll_Actual_#",
        f"AMI-CONV.L{lineNumber}_VEC_WebHandling_Middle_PVA_Roll_Actual_#",
        f"AMI-CONV.L{lineNumber}_VEC_WebHandling_Top_PVA_Roll_Actual_#",
        f"AMI-CONV.L{lineNumber}_VEC_Distribution_CPI_ProductAtDiverter_TimerStatus_State_#",
        f"AMI-CONV.L{lineNumber}_VEC_Distribution_CPI_ProductAtDiverter_DelayedTravelTime_State_0",
    ]

# tag_names may contain duplicates, so remove them
tag_names = list(set(tag_names))

# Build df_historian
dfhistorian_all = spark.sql(
    f"""SELECT TagName, Samples_Timestamp, Value_Double, Value_String, Samples_Quality
        FROM groupdb_fhc_sud_leakers.feb_23_ph2_converter_historian_mauricio t
        WHERE Samples_Timestamp > "{historian_start_datetime}" 
            AND Samples_Timestamp < "{end_datetime}"
    """
)
df_historian = dfhistorian_all.filter(dfhistorian_all.TagName.isin(tag_names))

if df_historian.count() > 0:
    df_historian = df_historian.select("*", unix_timestamp(df_historian["Samples_Timestamp"]).alias("epoch_Samples_Timestamp")).orderBy("Samples_Timestamp")
    
    df_historian = df_historian.repartition(spark.sparkContext.defaultParallelism)
    df_historian.persist(pyspark.StorageLevel.MEMORY_ONLY).count()

    ## TODO: Check permissions to read/write to dbfs in case is needed - Idea is having df_historian parttioned by TagName as it is a predicate used in almost all functions...
    df_historian.write.mode("overwrite").partitionBy("TagName").parquet("/dbfs/tmp/historian.parquet") 
    
else:
    dbutils.notebook.exit(
        f"The Historian table does not contain the target tags for {lines} between {start_datetime} and {end_datetime}"
    )

df_historian.show(n=5, truncate=False)
print(
    f"Dimension of df_historian is: {(df_historian.count(),len(df_historian.columns))}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate df_historian schema

# COMMAND ----------

# Validate df_historian schema
from pyspark.sql.types import *

# Define the expected schema for df_historian
expected_schema = StructType(
    [
        StructField("TagName", StringType(), True),
        StructField("Samples_Timestamp", TimestampType(), True),
        StructField("Value_Double", DoubleType(), True),
        StructField("Value_String", StringType(), True),
        StructField("Samples_Quality", LongType(), True),
        StructField("epoch_Samples_Timestamp", LongType(), True)
    ]
)

# Get the schema of the DataFrame
actual_schema = df_historian.schema

# Compare the actual schema with the expected schema and print the result
if actual_schema == expected_schema:
    print("df_historian schema validation passed!")
else:
    dbutils.notebook.exit(f"The df_historian data frame does not have proper schema")

# COMMAND ----------

# dataframe rows and partitions #TODO: Clean-up remove for job
print ("df_historian rows: " + str(df_historian.count()) + ", historian partitions: " + str(df_historian.rdd.getNumPartitions())) 
print ("df_buckets rows: " + str(df_buckets.count()) + ", buckets partitions: " + str(df_buckets.rdd.getNumPartitions()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Contextualization
# MAGIC

# COMMAND ----------

import pyspark
# TODO: Verify if this new column is really needed
df_historian = df_historian.select("*", lit("contextualization").alias("groupType"))
print(
    f"Dimension of df_historian is: {(df_historian.count(),len(df_historian.columns))}"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Define df_contextualization schema

# COMMAND ----------

# Define the schema for the contextualized data dataframe
schema = StructType(
    [
        StructField("contextualization_id", StringType(), True),
        StructField("image_timestamp", TimestampType(), True),
        StructField("line", StringType(), True),
        StructField("machine", StringType(), True),
        StructField("bucket_id", StringType(), True),
        StructField("infeed_chute_timestamp", TimestampType(), True),
        StructField("machine_speed_at_infeed", DoubleType(), True),
        StructField("bottom_dosing_timestamp", TimestampType(), True),
        StructField("top_dosing_timestamp", TimestampType(), True),
        StructField("top_combining_timestamp", TimestampType(), True),
        StructField("bottom_solvent_timestamp", TimestampType(), True),
        StructField("nd_combining_timestamp", TimestampType(), True),
        StructField("md_cutter_timestamp", TimestampType(), True),
        StructField("cd_cutter_timestamp", TimestampType(), True),
        StructField("reject_valves_timestamp", TimestampType(), True),
        StructField("splice_box_bottom_timestamp", TimestampType(), True),
        StructField("splice_box_middle_timestamp", TimestampType(), True),
        StructField("splice_box_top_timestamp", TimestampType(), True),
        StructField("start_dosing_timestamp", TimestampType(), True),
        StructField("time_since_start_dosing", StringType(), True),
        StructField("distance_since_start_dosing", DoubleType(), True),
        StructField("time_since_start_machine", StringType(), True),
        StructField("distance_since_start_machine", StringType(), True),
        StructField("time_since_last_bottom_splice", StringType(), True),
        StructField("time_since_last_middle_splice", StringType(), True),
        StructField("time_since_last_top_splice", StringType(), True),
        StructField("distance_since_last_bottom_splice", DoubleType(), True),
        StructField("distance_since_last_middle_splice", DoubleType(), True),
        StructField("distance_since_last_top_splice", DoubleType(), True),
        StructField("diameter_bottom_roll", DoubleType(), True),
        StructField("diameter_middle_roll", DoubleType(), True),
        StructField("diameter_top_roll", DoubleType(), True),
        StructField("pva_gcas_bottom", StringType(), True),
        StructField("pva_gcas_middle", StringType(), True),
        StructField("pva_gcas_top", StringType(), True),
        StructField("pva_roll_number_bottom", StringType(), True),
        StructField("pva_roll_number_middle", StringType(), True),
        StructField("pva_roll_number_top", StringType(), True),
    ]
)

# Create empty data frame for contextualized data
df_contextualized_data = spark.createDataFrame([], schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 

# COMMAND ----------

# Test on sample of df_buckets for benchmarking based on chunk_number 
import pyspark.sql.functions as f

df_buckets_sample = df_buckets.filter((f.col("chunk_number") == 0) & (f.col("Machine") == "VEPDA")).orderBy("row_id", ascending=True)
df_buckets_sample.show(n=10,truncate=False)

# COMMAND ----------

# # Contextualization process wrapper 2 - 
def process_row(row):
    row_id = int(row.row_id)
    tt = str(row.Trigger_Timestamp)
    id = int(row.id)
    machine = str(row.Machine)
    line = str(row.line)
    
    infeed_chute_timestamp = _get_infeed_chute_timestamp(tt,id,machine,line)
    machine_speed_at_infeed = _get_machine_speed_setpoint(tt,id,machine,line,infeed_chute_timestamp)
    #bottom_dosing_timestamp = _get_conv_all_events_timestamp (infeed_chute_timestamp, machine_speed_at_infeed)

    print(id)
    print(str(infeed_chute_timestamp))
    print(str(machine_speed_at_infeed))

    contextualized_data = {
        "contextualization_id": "",
        "image_timestamp": tt,
        "bucket_image_cccr_id": str(id),
        "line": line,
        "machine": machine,
        "bucket_id": str(id),
        "infeed_chute_timestamp": infeed_chute_timestamp,
        "machine_speed_at_infeed": machine_speed_at_infeed,
        #"bottom_dosing_timestamp": bottom_dosing_timestamp
        # "top_dosing_timestamp": top_dosing_timestamp,
        # "top_combining_timestamp": top_combining_timestamp,
        # "bottom_solvent_timestamp": bottom_solvent_timestamp,
        # "nd_combining_timestamp": nd_combining_timestamp,
        # "md_cutter_timestamp": md_cutter_timestamp,
        # "cd_cutter_timestamp": cd_cutter_timestamp,
        # "reject_valves_timestamp": reject_valves_timestamp,
        # "splice_box_bottom_timestamp": splice_box_bottom_timestamp,
        # "splice_box_middle_timestamp": splice_box_middle_timestamp,
        # "splice_box_top_timestamp": splice_box_top_timestamp,
        # "start_dosing_timestamp": start_dosing_timestamp
    }

    return contextualized_data


# COMMAND ----------

# # Test - multiprocessing - TODO: Check if any performance improvement
from multiprocessing.pool import ThreadPool
from functools import reduce

data_collect = df_buckets_sample.collect()

# # Single Thread: 
# # Use a for loop to apply the function to each row
# df_contextualized_data_list = [process_row(row) for row in data_collect]

# # Union all the dataframes
# df_contextualized_data = reduce(DataFrame.unionAll, df_contextualized_data_list)


#================================
# Multiple Threads:
# Create a ThreadPool TODO: Verify if needed based on performance testing
pool = ThreadPool(8)  # Adjust the number of threads as needed
    
# Use the map method to apply the function to each row
df_contextualized_data_list = pool.map(process_row, data_collect)

# Convert list of dictionaries to DataFrame
df_contextualized_data = spark.createDataFrame(df_contextualized_data_list)

# display contextualized data
display(df_contextualized_data)

# # TODO: Cleanup: remove 
# print(
#     f"Dimension of df_contextualized_data is: {(df_contextualized_data.count(),len(df_contextualized_data.columns))}"
# )


# COMMAND ----------


## This would be processing 
# # Convert DataFrame to RDD
# rdd = df_buckets_sample.rdd

# # Define a function to apply to each row
# def process_row(row):
#     # Convert Row to dict
#     row_dict = row.asDict()
    
#     # Apply _get_infeed_chute_timestamp function to each row
#     timestamp = _get_infeed_chute_timestamp(row_dict['tt'], row_dict['id'], row_dict['machine'], row_dict['line'])
    
#     # Add new timestamp to row dict
#     row_dict['new_timestamp'] = timestamp
    
#     # Convert dict back to Row
#     new_row = Row(**row_dict)
    
#     return new_row

# # Apply function to each row
# new_rdd = rdd.map(process_row)

# # Convert RDD back to DataFrame
# new_df = spark.createDataFrame(new_rdd)

# display(new_df)

# COMMAND ----------

# # Test to avoid Python multithreading - Fail: " Method public org.apache.spark.rdd.RDD org.apache.spark.api.java.JavaRDD.rdd() is not whitelisted on class class org.apache.spark.api.java.JavaRDDRDD map operation is not whitelisted" - Check if there is an option to handle using job cluster config

# from pyspark.sql import Row

# # Assuming data_collect is a PySpark DataFrame
# data_rdd = df_buckets_sample.rdd

# # Use map to apply the function to each row
# contextualized_data_rdd = data_rdd.map(process_row)

# # Convert RDD of Row to DataFrame
# df_contextualized_data = spark.createDataFrame(contextualized_data_rdd)

# # Display contextualized data
# df_contextualized_data.show()

# print(
#     f"Dimension of df_contextualized_data is: {(df_contextualized_data.count(), len(df_contextualized_data.columns))}"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unit Test

# COMMAND ----------

# # Fn1
# # # _get_infeed_chute_timestamp
# #def _get_infeed_chute_timestamp(tt, id, machine, line):

# # Unit test for Fn1 and Fn2
# tt = "2023-02-05 08:57:36"
# id = 377866
# machine = "VEPDB"
# line = "28"

# tag_name = "AMI-CONV.L" + line + "_" + machine + "_Primary_BucketID_AtInfeedChute"
# buckets_per_current_line = 834
# max_missing_buckets = 5

# df_infeed_chute = (df_historian
#     .filter((F.col('TagName') == tag_name) & (F.col('Samples_Timestamp') < tt))
#     .sort(F.col('Samples_Timestamp').desc())
#     .dropDuplicates(subset=['Value_Double'])
#     .persist())

# df_infeed_chute.explain()

# # Find the range of possible bucket IDs to search for
# if (id + max_missing_buckets) <= (buckets_per_current_line - 1):
#     nearest_max_bucket_id = (id + max_missing_buckets)
#     min_bucket_id = id
#     max_bucket_id = nearest_max_bucket_id
# else:
#     nearest_max_bucket_id = max_missing_buckets - (buckets_per_current_line - id)
#     min_bucket_id = id if id >= 0 else 0
#     max_bucket_id = buckets_per_current_line - 1

# # Filter Historian data for relevant bucket IDs
# df_matching_or_nearest_bucket = (df_infeed_chute.filter(
#     ((F.col('Value_Double').between(min_bucket_id, max_bucket_id)) |
#     (F.col('Value_Double').between(0, nearest_max_bucket_id)))
# ).orderBy(F.col('Value_Double').desc()).persist())

# #df_matching_or_nearest_bucket.explain()

# # Find the timestamp closest to the rejected bucket's timestamp
# first_matching_bucket = df_matching_or_nearest_bucket.first()
# first_matching_timestamp = first_matching_bucket['Samples_Timestamp']
# first_matching_bucket_id = first_matching_bucket['Value_Double']

# timestamp_to_keep = first_matching_timestamp
# bucket_id_to_keep = first_matching_bucket_id

# for row in df_matching_or_nearest_bucket.collect():
#     bucket_id = row['Value_Double']
#     potential_bucket_timestamp = df_infeed_chute.filter(F.col('Value_Double') == bucket_id).select(
#         'Samples_Timestamp').first()['Samples_Timestamp']
#     potential_bucket_id = df_infeed_chute.filter(F.col('Value_Double') == bucket_id).select(
#         'Value_Double').first()['Value_Double']

#     if potential_bucket_timestamp - first_matching_timestamp > timedelta(minutes=5):
#         timestamp_to_keep = potential_bucket_timestamp
#         bucket_id_to_keep = potential_bucket_id
#         break

# return timestamp_to_keep

# COMMAND ----------

# # Unit test for Fn1 and Fn2

# from pyspark.sql.functions import col
# from pyspark.sql import functions as F

# tt = "2023-02-05 08:57:36"
# id = 377866
# machine = "VEPDB"
# line = "28"

# infeed_chute_timestamp = _get_infeed_chute_timestamp(tt, id, machine, line)
# print("infeed_chute_timestamp: " + str(infeed_chute_timestamp))

# # infeed_chute_timestamp = "2023-02-05 08:57:35.293000"

# machine_speed_at_infeed = _get_machine_speed_setpoint(tt, id, machine, line, infeed_chute_timestamp)
# print ("machine_speed_at_infeed: " + str(machine_speed_at_infeed))

# # infeed_chute_timestamp: 2023-02-05 08:57:35.293000
# # machine_speed_at_infeed: None

# # tag_name = "AMI-CONV.L28_Machine_Speed_Setpoint"

# # machine_speed_at_infeed = filtered_df.agg(F.max('Value_Double')).collect()[0][0]
# # print(str(machine_speed_at_infeed))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Contextualized data 

# COMMAND ----------

# from datetime import datetime
# import pytz

# contextualized_data_table = "groupdb_fhc_sud_leakers.mam_23_ph2_converter_contextualized_data_original_test_"
# time_zone = pytz.timezone('UTC')
# timestamp = datetime.now(time_zone).strftime("%Y%m%d_%H%M%S")
# output_table = contextualized_data_table + timestamp

# df_contextualized_data_spark = sampleDF
# df_contextualized_data_spark.write.mode("overwrite").saveAsTable(output_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean up 

# COMMAND ----------

# Remove both dataframes from memory
df_buckets.unpersist()
df_historian.unpersist()
