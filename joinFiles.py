import pandas as pd
from datetime import datetime


def merge_location_and_motion_data(location_file, motion_file):
    try:
        locations_df = pd.read_csv(location_file, names=("timestamp", "latitude", "longitude", "speed", "course" ))
        motions_df = pd.read_csv(motion_file, names=("timestamp", "gyro_x", "gyro_y", "gyro_z"))
        print("Файлы 'location.csv' и 'motion.csv' успешно загружены.")
    except FileNotFoundError as e:
        print(f"Ошибка: файл не найден. Убедитесь, что {e.filename} находится в той же директории, что и скрипт.")
        return None


    try:
        # 2025-06-13T16:09:23.0030
        datetime_format = "%Y-%m-%dT%H:%M:%S.%f"
        print(locations_df)
        locations_df['timestamp'] = pd.to_datetime(locations_df['timestamp']+"00", format=datetime_format)
        motions_df['timestamp'] = pd.to_datetime(motions_df['timestamp']+"00", format=datetime_format)
    except Exception as e:
        print(f"Ошибка при преобразовании времени: {e}")
        print(
            "Пожалуйста, убедитесь, что формат времени в файлах соответствует стандарту ISO 8601 (например, 'гггг-ММ-ддTЧЧ:мм:сс.ffffff')")
        return None

    locations_df = locations_df.sort_values(by='timestamp').reset_index(drop=True)
    motions_df = motions_df.sort_values(by='timestamp').reset_index(drop=True)

    results = []

    column_names = [
        # 'временной_промежуток_сек',
        # 'изменение_широты',
        # 'изменение_долготы',
        'изменение_скорости',
        'скорость',
        'сумма_gyro_x',
        'сумма_gyro_y',
        'сумма_gyro_z',
    ]

    print("Начинается обработка данных...")

    for i in range(1, len(locations_df)):
        start_time = locations_df.loc[i - 1, 'timestamp']
        end_time = locations_df.loc[i, 'timestamp']

        time_delta = (end_time - start_time).total_seconds()
        lat_change = locations_df.loc[i, 'latitude'] - locations_df.loc[i - 1, 'latitude']
        lon_change = locations_df.loc[i, 'longitude'] - locations_df.loc[i - 1, 'longitude']
        speed_change = locations_df.loc[i, 'speed'] - locations_df.loc[i - 1, 'speed']
        speed = locations_df.loc[i, 'speed']

        motions_in_interval = motions_df[
            (motions_df['timestamp'] > start_time) & (motions_df['timestamp'] <= end_time)
            ]

        gyro_x_sum = motions_in_interval['gyro_x'].sum()
        gyro_y_sum = motions_in_interval['gyro_y'].sum()
        gyro_z_sum = motions_in_interval['gyro_z'].sum()

        results.append([
            # time_delta,
            # lat_change,
            # lon_change,
            speed_change,
            speed,
            gyro_x_sum,
            gyro_y_sum,
            gyro_z_sum,
        ])

    result_df = pd.DataFrame(results, columns=column_names)
    print("Обработка успешно завершена.")

    return result_df


if __name__ == '__main__':
    directory = "tracking_data_2025-06-13_13-58-09"
    location_filename = f'{directory}/location.csv'
    motion_filename = f'{directory}/motion.csv'

    merged_df = merge_location_and_motion_data(location_filename, motion_filename)

    if merged_df is not None:
        print("\nИтоговая объединенная таблица:")
        print(merged_df)

        output_filename = f'{directory}/merged_data.csv'
        try:
            merged_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            print(f"\nРезультат успешно сохранен в файл: {output_filename}")
        except Exception as e:
            print(f"Не удалось сохранить файл: {e}")
