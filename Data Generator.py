import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set seed for reproducibility

np.random.seed(42)

# Generate hourly timestamps for 30 days (720 hours)

start_date = datetime(2026, 4, 1, 0, 0)
timestamps = [start_date + timedelta(hours=i) for i in range(720)]

# Initialize empty dictionary for our data

data = {
    'Timestamp': timestamps,
    'Reactor_Temp_C': np.random.normal(180, 5, 720), # Mean 180°C, standard deviation 5
    'Reactor_Pressure_bar': np.random.normal(15, 1.2, 720),
    'Catalyst_Flow_kg_h': np.random.normal(50, 4, 720),
    'Raw_Material_Purity_Pct': np.random.uniform(90, 99, 720)
}

df = pd.DataFrame(data) #Creat a table


# Injection of anomalies


# Correlation of Temperature and random noise

df['Energy_Consumption_MWh'] = (df['Reactor_Temp_C'] * 0.15) + (df['Reactor_Pressure_bar'] * 0.2) + np.random.normal(5, 0.5, 720)

# Correlation of Catalyst, Temperature, Purity and random noise

df['Product_Yield_kg'] = (df['Catalyst_Flow_kg_h'] * 12) + (df['Raw_Material_Purity_Pct'] * 4) + np.random.normal(200, 20, 720)

# Spike temperature and energy consumption between rows 200 and 210 (Simulating a cooling failure)

df.loc[200:210, 'Reactor_Temp_C'] += 45
df.loc[200:210, 'Energy_Consumption_MWh'] += 15

# Drop pressure to zero for 5 random rows (Simulating sensor dropouts/errors)

missing_pressure_idx = np.random.choice(df.index, size=5, replace=False)
df.loc[missing_pressure_idx, 'Reactor_Pressure_bar'] = np.nan

# Save to the data/ folder

df.to_csv('data/raw_reactor_data.csv', index=False)
