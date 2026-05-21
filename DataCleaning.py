from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean

# Initialize the Spark Session

spark = SparkSession.builder \
        .appName("IndustrialProcessOptimization") \
        .getOrCreate()

print("\nSpark Session started successfully")

# Load the raw CSV data we generated

df = spark.read.csv("/Users/tiagopereira/Desktop/industrial-process-optimization/data/raw_reactor_data.csv", header=True, inferSchema=True)

# Calculate Mean Pressure (to handle and impute missing NaNs)

mean_pressure_val = df.select(mean("Reactor_Pressure_bar")).collect()[0][0]

print(f"[INFO] Mean Pressure calculated: {mean_pressure_val:.2f} bar")

# Apply Data Pipelines (Data Imputation, Anomaly Flagging, and Efficiency Engineering)

df_cleaned = df \
             .fillna({"Reactor_Pressure_bar": mean_pressure_val}) \
             .withColumn("Is_Anomaly", when(col("Reactor_Temp_C") > 210, 1).otherwise(0)) \
             .withColumn("Energy_Efficiency_MWh_per_kg", col("Energy_Consumption_MWh") / col("Product_Yield_kg"))

print("\n[INFO] Data transformations completed successfully.")

# Validate Pipeline Results in the Terminal

print("\n------- Verification: Null Values in Pressure Column -------")

df_cleaned.filter(col("Reactor_Pressure_bar").isNull()).show()

print("\n------- Verification: Newly Engineered Features (First 5 Rows) -------")

df_cleaned.select("Timestamp", "Reactor_Temp_C", "Reactor_Pressure_bar", "Is_Anomaly", "Energy_Efficiency_MWh_per_kg").show(5)

# Export the Cleaned Dataset to a single CSV file

output_dir = "/Users/tiagopereira/Desktop/industrial-process-optimization/data/processed"

df_cleaned.coalesce(1).write.mode("overwrite").csv(output_dir, header=True)

print(f"[SUCCESS] Cleaned dataset saved to: {output_dir}")

# Stop the Spark Session and release resources

spark.stop()
