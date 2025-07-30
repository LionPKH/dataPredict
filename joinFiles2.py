import pandas as pd
import zipfile
import os
from datetime import datetime
import shutil
import re
from pathlib import Path


def merge_sensor_data(location_file, motion_file, acceleration_file):
    try:
        locations_df = pd.read_csv(location_file, names=("timestamp", "latitude", "longitude", "speed", "course"))
        motions_df = pd.read_csv(motion_file, names=("timestamp", "gyro_x", "gyro_y", "gyro_z"))
        accelerations_df = pd.read_csv(acceleration_file, names=("timestamp", "accel_x", "accel_y", "accel_z"))
        print("Файлы 'location.csv', 'motion.csv' и 'acceleration.csv' успешно загружены.")
    except FileNotFoundError as e:
        print(f"Ошибка: файл не найден. Убедитесь, что {e.filename} находится в правильной директории.")
        return None

    try:
        def parse_timestamp(ts_series):
            ts_series = ts_series.astype(str).str.replace(r'[+-]\d{4}$', '', regex=True)
            ts_series = ts_series.apply(
                lambda x: x + '0' * (6 - len(x.split('.')[-1])) if '.' in x and len(x.split('.')[-1]) < 6 else x)
            return pd.to_datetime(ts_series, format="%Y-%m-%dT%H:%M:%S.%f", errors='coerce')

        locations_df['timestamp'] = parse_timestamp(locations_df['timestamp'])
        motions_df['timestamp'] = parse_timestamp(motions_df['timestamp'])
        accelerations_df['timestamp'] = parse_timestamp(accelerations_df['timestamp'])

        locations_df['latitude'] = pd.to_numeric(locations_df['latitude'], errors='coerce')
        locations_df['longitude'] = pd.to_numeric(locations_df['longitude'], errors='coerce')
        locations_df['speed'] = pd.to_numeric(locations_df['speed'], errors='coerce')
        motions_df['gyro_x'] = pd.to_numeric(motions_df['gyro_x'], errors='coerce')
        motions_df['gyro_y'] = pd.to_numeric(motions_df['gyro_y'], errors='coerce')
        motions_df['gyro_z'] = pd.to_numeric(motions_df['gyro_z'], errors='coerce')
        accelerations_df['accel_x'] = pd.to_numeric(accelerations_df['accel_x'], errors='coerce')
        accelerations_df['accel_y'] = pd.to_numeric(accelerations_df['accel_y'], errors='coerce')
        accelerations_df['accel_z'] = pd.to_numeric(accelerations_df['accel_z'], errors='coerce')

        locations_df.dropna(subset=['timestamp', 'latitude', 'longitude', 'speed'], inplace=True)
        motions_df.dropna(subset=['timestamp', 'gyro_x', 'gyro_y', 'gyro_z'], inplace=True)
        accelerations_df.dropna(subset=['timestamp', 'accel_x', 'accel_y', 'accel_z'], inplace=True)

    except Exception as e:
        print(f"Ошибка при преобразовании времени или числовых данных: {e}")
        print(
            "Пожалуйста, убедитесь, что формат времени в файлах соответствует стандарту ISO 8601 (например, 'гггг-ММ-ддTЧЧ:мм:сс.ffffff' или 'гггг-ММ-ддTЧЧ:мм:сс.ffffff+ЧЧММ'), а числовые столбцы содержат только числа.")
        return None

    locations_df = locations_df.sort_values(by='timestamp').reset_index(drop=True)
    motions_df = motions_df.sort_values(by='timestamp').reset_index(drop=True)
    accelerations_df = accelerations_df.sort_values(by='timestamp').reset_index(drop=True)

    results = []

    column_names = [
        'временной_промежуток_сек',
        'изменение_широты',
        'изменение_долготы',
        'изменение_скорости',
        'сумма_gyro_x',
        'сумма_gyro_y',
        'сумма_accel_x',
        'сумма_accel_y',
        'сумма_accel_z',
    ]

    print("Начинается обработка данных...")

    for i in range(1, len(locations_df)):
        start_time = locations_df.loc[i - 1, 'timestamp']
        end_time = locations_df.loc[i, 'timestamp']

        time_delta = (end_time - start_time).total_seconds()
        lat_change = locations_df.loc[i, 'latitude'] - locations_df.loc[i - 1, 'latitude']
        lon_change = locations_df.loc[i, 'longitude'] - locations_df.loc[i - 1, 'longitude']
        speed_change = locations_df.loc[i, 'speed'] - locations_df.loc[i - 1, 'speed']

        motions_in_interval = motions_df[
            (motions_df['timestamp'] > start_time) & (motions_df['timestamp'] <= end_time)
            ]
        gyro_x_sum = motions_in_interval['gyro_x'].sum()
        gyro_y_sum = motions_in_interval['gyro_y'].sum()

        accels_in_interval = accelerations_df[
            (accelerations_df['timestamp'] > start_time) & (accelerations_df['timestamp'] <= end_time)
            ]
        accel_x_sum = accels_in_interval['accel_x'].sum()
        accel_y_sum = accels_in_interval['accel_y'].sum()
        accel_z_sum = accels_in_interval['accel_z'].sum()

        results.append([
            time_delta,
            lat_change,
            lon_change,
            speed_change,
            gyro_x_sum,
            gyro_y_sum,
            accel_x_sum,
            accel_y_sum,
            accel_z_sum,
        ])

    result_df = pd.DataFrame(results, columns=column_names)
    print("Обработка успешно завершена.")

    return result_df


if __name__ == '__main__':
    output_base_dir = Path("extracted_data")
    consolidated_csv_path = Path('output')
    output_base_dir.mkdir(exist_ok=True)

    merged_df = merge_sensor_data(
        consolidated_csv_path / "all_location.csv",
        consolidated_csv_path / 'all_motion.csv',
        consolidated_csv_path / 'all_acceleration.csv'
    )

    if merged_df is not None:
        final_output_path = os.path.join(output_base_dir, "final_merged_data.csv")
        try:
            merged_df.to_csv(final_output_path, index=False, encoding='utf-8-sig', mode='w')
            print(f"\nИтоговые объединенные данные сохранены в: {final_output_path}")
        except Exception as e:
            print(f"Не удалось сохранить итоговый объединенный файл: {e}")
    else:
        print("\nНе удалось выполнить окончательное объединение данных.")
