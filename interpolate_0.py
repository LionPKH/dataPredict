import pandas as pd
import numpy as np
from pathlib import Path

consolidated_csv_path = Path('output')


location_file = consolidated_csv_path / "all_location.csv"
acceleration_file = consolidated_csv_path / 'all_acceleration.csv'


locations_df = pd.read_csv(location_file, names=("timestamp", "latitude", "longitude", "speed", "course"), parse_dates=['timestamp'])
accelerations_df = pd.read_csv(acceleration_file, names=("timestamp", "accel_x", "accel_y", "accel_z"), parse_dates=['timestamp'])

locations_df.set_index('timestamp', inplace=True)
accelerations_df.set_index('timestamp', inplace=True)

locations_df = locations_df[~locations_df.index.duplicated(keep='first')]

# Переиндексировать df_low на временные точки из df_high
df_interp = locations_df.reindex(accelerations_df.index, method=None)  # создаст NaN между точками

df_interp = df_interp.interpolate(method='time')

final_output_path = consolidated_csv_path / "speed_interpolated.csv"

df_interp.to_csv(final_output_path, index=True, encoding='utf-8-sig', mode='w')
