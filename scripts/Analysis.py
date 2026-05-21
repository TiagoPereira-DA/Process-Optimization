from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean

# Initialize Spark Session

spark = SparkSession.builder \
    .appName("IndustrialProcessOptimization") \
    .getOrCreate()

print("\nSpark Session started successfully")

# Load the raw CSV data we generated

df = spark.read.csv("/Users/tiagopereira/Desktop/industrial-process-optimization/data/raw_reactor_data.csv", header=True, inferSchema=True)

print("\n-------Data Schema------")

df.printSchema()

print("\n------- First 5 Rows of the Dataset -------")

df.show(5)

# Verify the injected anomalies 

print("\n------- Finding Missing Data (Pressure Drops) -------")

df.filter(col("Reactor_Pressure_bar").isNull()).show()

print("\n------ Finding Temperature Spikes (> 210°C) -------")

df.filter(col("Reactor_Temp_C") > 210).show()

# Calculate process averages

print("\n------- Global Process Metrics -------")
df.select(
    mean("Reactor_Temp_C").alias("Avg_Temperature_C"),
    mean("Energy_Consumption_MWh").alias("Avg_Energy_MWh"),
    mean("Product_Yield_kg").alias("Avg_Yield_kg")
).show()

# Close the session

spark.stop()
