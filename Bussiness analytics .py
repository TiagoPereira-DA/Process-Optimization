from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, date_format, sum, avg

# Initialize the Spark Session for Business Analytics

spark = SparkSession.builder \
    .appName("CorporateBusinessAnalytics") \
    .getOrCreate()

print("\n[INFO] Spark Session started for Business Intelligence Analysis.")

# Load the cleaned dataset we generated in Task 3

df = spark.read.csv("/Users/tiagopereira/Desktop/industrial-process-optimization/data/processed/*.csv", header=True, inferSchema=True)

# Hourly Efficiency Profiling (Analyzing patterns by hour of the day). Extract the hour from the Timestamp and calculate average efficiency

hourly_profile = df \
    .withColumn("Hour_of_Day", hour(col("Timestamp"))) \
    .groupBy("Hour_of_Day") \
    .agg(
        avg("Energy_Efficiency_MWh_per_kg").alias("Avg_Energy_Cost_Per_Unit"),
        sum("Product_Yield_kg").alias("Total_Production_kg")
    ) \
    .orderBy("Hour_of_Day")

print("\n------- Executive Summary: Hourly Performance Profile -------")

hourly_profile.show(24)

# Daily Operations Report (Aggregating data by Date). Convert Timestamp to a clean YYYY-MM-DD format

daily_report = df \
    .withColumn("Date", date_format(col("Timestamp"), "yyyy-MM-dd")) \
    .groupBy("Date") \
    .agg(
        sum("Product_Yield_kg").alias("Daily_Total_Production_kg"),
        sum("Energy_Consumption_MWh").alias("Daily_Total_Energy_MWh"),
        avg("Reactor_Temp_C").alias("Daily_Avg_Temperature")
    ) \
    .orderBy("Date")

print("\n------- Executive Summary: Daily Operations Report -------")

daily_report.show()

# Export the Business Reports for Power BI

hourly_output = "/Users/tiagopereira/Desktop/industrial-process-optimization/data/business_reports/hourly_profile"

daily_output = "/Users/tiagopereira/Desktop/industrial-process-optimization/data/business_reports/daily_report"

hourly_profile.coalesce(1).write.mode("overwrite").csv(hourly_output, header=True)

daily_report.coalesce(1).write.mode("overwrite").csv(daily_output, header=True)

print(f"\n[SUCCESS] Business Intelligence reports exported successfully to 'data/business_reports/'")

# Stop the session

spark.stop()
