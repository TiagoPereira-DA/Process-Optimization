# Process-Optimization
A data analytics project focused on optimizing chemical process efficiency and energy consumption using Python, SQL, and Power BI.

*Chemical Process Optimization Analysis*

- Project Overview
This project focuses on analyzing historical sensor data from a continuous chemical reactor to optimize production yield and minimize energy consumption. As a Chemical Engineer transitioning to Data Analytics, I leverage domain knowledge to detect process anomalies, identify inefficiencies, and deliver actionable insights for plant operations.

- Tools Utilized

	-Data Processing: PySpark (Apache Spark)

	-Data Analysis: Python (Pandas, NumPy)

	-Database Management: SQL

	-Data Visualization: Power BI


Task 1: Creating a DataSet for analysis 

The dataset generation logic is broken down below:
	
- Reproducibility: The numbers were generated using "numpy.random". A random "seed(42)" was set to ensure that the exact same dataset is generated every time the script is executed. A total of 720 observations (representing 720 hours/30 days) were created.
	
- Timeline Generation: The recording period begins on April 1st of 2026. Using a Python list comprehension ("[start_date + timedelta(hours=i) for i in range(720)]"), a time loop was created to sequentially add one hour to each row for 30 consecutive days.
	
- Reactor Baseline Parameters: To simulate standard operational data, a normal (gaussian) distribution was applied to generate realistic fluctuations for "Reactor Temperature", "Pressure", and "Catalyst Flow". A uniform distribution was applied to the "Raw Material Purity" percentage to ensure an even spread between 90% and 99%.
	
- DataFrame Creation: The generated data dictionary was structured into a tabular format using "df = pd.DataFrame(data)".

- Energy Consumption Logic: In a real chemical plant, heating, cooling, and compression systems draw more power as process severity increases. To reflect this physical law, a linear equation was created where "Reactor_Temp_C" and "Reactor_Pressure_bar" are multiplied by scaling factors (0.15 and 0.2, respectively). Normal statistical noise was added to simulate ambient environmental impacts and measurement fluctuations.

- Product Yield Logic: The final output mass depends directly on the hourly catalyst flow rate and the purity of the incoming raw material. The code applies specific weights to these variables to inject a hidden correlation. This ensures that future exploratory graphs and models will show realistic dependencies. Statistical noise was also included here to prevent an artificial, perfectly linear relationship.

- Anomaly Injection (Process Failure): To simulate a real-world cooling system failure, abnormal values were artificially added to both temperature (+45°C) and energy consumption (+15 MWh) between indices 200 and 210 (a 10-hour window).

- Sensor Dropouts (Missing Data): To simulate common instrument or telemetry errors, 5 random rows in the "Reactor_Pressure_bar" column were replaced with null values ("NaN").

- Data Export: Finally, the structured, "dirty" dataset was exported as a local CSV file ("raw_reactor_data.csv") using the "to_csv()" function.

Task 2: Data Ingestion (PySpark)

The data loading, schema validation, and anomaly detection phase using distributed computing is broken down below:

- Spark Session Initialization: A local Spark Cluster environment was established using the "SparkSession.builder" API under the folder name "IndustrialProcessOptimization".

- Data Ingestion: The raw sensor dataset ("raw_reactor_data.csv") was loaded into a Spark DataFrame using an absolute file system path. By setting "header=True", the column names were automatically mapped, and "inferSchema=True" forced PySpark to evaluate the records and automatically bind appropriate data types (timestamps and decimal values) to each sensor column.

- Data Schema: The "df.printSchema()" function was executed to audit the structural blueprint of the dataset. This step ensures that the system correctly interpreted the data types (e.g., confirming that temperature and pressure are registered as numeric fields) before running heavy operations.

- Missing Data: To isolate instrument or communication failures in the plant, the "df.filter(col().isNull())" function was applied. This instantly scanned the dataset to isolate and display the 5 specific timestamps where the pressure sensor experienced a data dropout ("NaN").

- Temperature Spikes: To detect critical operational risks, a filtering condition ("col() > 210") was applied to the reactor temperature column. This isolated the exact 10-hour window where the cooling system failure occurred, confirming that the historical data accurately captured the emergency event.

- Baseline Metrics: Using the "df.select(mean())" function, process averages were calculated for temperature, energy consumption, and product yield. These statistical baselines serve as the foundation for future process optimization, allowing plant operations to compare normal behavior against anomalous events.


Task 3: Data Cleaning (PySpark)

The data transformation pipeline, missing value imputation, and operational feature engineering phase using distributed computing is broken down below:

- Missing Value Imputation: To resolve the 5 random instrument data dropouts ("NaN") in the pressure column without losing historical context, the "mean()" aggregation function was combined with a ".collect()[0][0]" data extraction sequence. This isolated the exact global baseline average of the process, which was then injected into the missing rows via the ".fillna()" API, filling the full dataset.

- Process Anomaly: To turn raw missing records into actionable operational insights, conditional logic was applied using the "when(col() > 210, 1).otherwise(0)" framework. This created a new binary feature column ("Is_Anomaly") that automatically flags the 10-hour cooling system failure window, preparing the dataset for future predictive maintenance models.

- Operational Efficiency Modeling: In chemical plant logistics, assessing energy draw against actual product mass is critical for margin optimization. A mathematical column transformation ("withColumn") was built to divide "Energy_Consumption_MWh" by "Product_Yield_kg" on a rolling hourly basis, creating a custom diagnostic metric named "Energy_Efficiency_MWh_per_kg".

- Pipeline Output Consolidation: To ensure that the final cleaned and engineered dataset is ready for analytics and business intelligence dashboards, the data stream was funneled through a ".coalesce(1)" partition manager. This forces Spark's parallel architecture to safely merge all data blocks back into a single, clean partition before exporting it to the local target path ("data/processed") using the "overwrite" file-writing protocol.

Task 4: Business Analysis (PySpark)

The business analytics phase focused on transforming granular operational records into executive-level performance metrics and aggregated reports using PySpark data frames:

- Hourly Efficiency Profiling: Developed a time-series aggregation pipeline using the "hour()" function to segment operations across a 24-hour cycle. By grouping data by hour of the day and applying the "avg()" and "sum()" aggregations, the model maps daily utility cost peaks and productivity troughs, providing a direct breakdown of hourly efficiency profiles for operational cost control.

- Corporate Daily Performance Summaries: Implemented structured date transformation workflows using the "date_format(col(), 'yyyy-MM-dd')" schema. Granular timestamps were consolidated into automated daily ledger summaries ("Daily_Total_Production_kg" and "Daily_Total_Energy_MWh"), streamlining broad historical data streams into high-level business reports.

- Analytics Delivery: To facilitate executive decision-making and cross-departmental reporting, the generated metrics were split into specialized operational tables. Using ".coalesce(1)", the processed business insights were exported as clean, single-partition CSV assets into a new enterprise-ready directory ("data/business_reports/"), establishing a seamless data feed for Power BI and Tableau dashboards.
