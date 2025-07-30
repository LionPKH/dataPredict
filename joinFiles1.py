import pandas as pd
import zipfile
import os
from datetime import datetime
import shutil


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

        locations_df.dropna(subset=['timestamp'], inplace=True)
        motions_df.dropna(subset=['timestamp'], inplace=True)
        accelerations_df.dropna(subset=['timestamp'], inplace=True)

    except Exception as e:
        print(f"Ошибка при преобразовании времени: {e}")
        print(
            "Пожалуйста, убедитесь, что формат времени в файлах соответствует стандарту ISO 8601 (например, 'гггг-ММ-ддTЧЧ:мм:сс.ffffff' или 'гггг-ММ-ддTЧЧ:мм:сс.ffffff+ЧЧММ')")
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


def _recursive_unzip(zip_path, extract_to_dir):
    """Распаковывает один ZIP-архив и возвращает путь к распакованной директории."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            extract_dir_name = os.path.splitext(os.path.basename(zip_path))[0]
            extraction_target_path = os.path.join(extract_to_dir, extract_dir_name)
            os.makedirs(extraction_target_path, exist_ok=True)
            zip_ref.extractall(extraction_target_path)
            print(f"Архив '{zip_path}' успешно распакован в '{extraction_target_path}'.")
            return extraction_target_path
    except zipfile.BadZipFile:
        print(f"Ошибка: '{zip_path}' не является действительным ZIP-файлом.")
    except FileNotFoundError:
        print(f"Ошибка: Файл '{zip_path}' не найден.")
    except Exception as e:
        print(f"Произошла ошибка при распаковке '{zip_path}': {e}")
    return None


def unzip_and_process_all_archives(main_zip_path):
    output_base_dir = "extracted_data"

    if os.path.exists(output_base_dir):
        shutil.rmtree(output_base_dir)
        print(f"Удалена предыдущая директория извлечения: {output_base_dir}")
    os.makedirs(output_base_dir, exist_ok=True)
    print(f"Создана базовая директория для извлечения: {output_base_dir}")

    # --- Этап 1: Рекурсивная распаковка всех ZIP-архивов ---
    processed_zip_paths = set()
    queue = [main_zip_path]  # Очередь для обработки ZIP-файлов

    # Распаковываем основной архив в его собственную подпапку
    extracted_main_path = _recursive_unzip(main_zip_path, output_base_dir)
    if not extracted_main_path:
        print("Не удалось распаковать основной архив. Завершение работы.")
        return
    processed_zip_paths.add(main_zip_path)  # Добавляем основной архив в обработанные

    # Теперь ищем все ZIP-файлы внутри только что распакованного основного архива
    # и добавляем их в очередь
    for root, _, files in os.walk(extracted_main_path):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                if zip_path not in processed_zip_paths:
                    queue.append(zip_path)
                    processed_zip_paths.add(zip_path)

    # Обрабатываем очередь ZIP-файлов, включая те, что будут найдены после распаковки
    idx = 0
    while idx < len(queue):
        current_zip_path = queue[idx]
        idx += 1

        if current_zip_path == main_zip_path:  # Основной архив уже распакован, пропускаем его повторную обработку
            continue

        # Определяем директорию, куда будет распакован текущий вложенный ZIP
        # Это будет родительская директория текущего ZIP-файла
        extract_target_for_nested_zip = os.path.dirname(current_zip_path)

        extracted_path = _recursive_unzip(current_zip_path, extract_target_for_nested_zip)
        if extracted_path:
            # Ищем новые ZIP-файлы внутри только что распакованного архива
            for root_new, _, files_new in os.walk(extracted_path):
                for file_new in files_new:
                    if file_new.endswith('.zip'):
                        new_zip_path = os.path.join(root_new, file_new)
                        if new_zip_path not in processed_zip_paths:
                            queue.append(new_zip_path)
                            processed_zip_paths.add(new_zip_path)

    print("\n--- Все архивы распакованы. Начинается обработка CSV файлов ---")

    # --- Этап 2: Обработка распакованных данных ---
    all_merged_dfs = []

    # Теперь ищем папки tracking_data_* внутри main_extract_path
    # Это гарантирует, что мы обрабатываем только те папки, которые были результатом распаковки
    # tracking_data архивов
    for root, dirs, _ in os.walk(extracted_main_path):
        for dir_name in dirs:
            if dir_name.startswith('tracking_data_'):
                tracking_data_folder_path = os.path.join(root, dir_name)

                found_files = {}
                # Изменены требуемые файлы для поиска по префиксу
                required_prefixes = {
                    'location_': None,
                    'motion_': None,
                    'acceleration_': None
                }

                # Ищем CSV-файлы рекурсивно внутри этой папки tracking_data_*
                print(f"Начинаем рекурсивный поиск CSV файлов в: {tracking_data_folder_path}")
                for sub_root_dir, sub_dirs, sub_files in os.walk(tracking_data_folder_path):
                    print(f"  Текущая директория: {sub_root_dir}")
                    print(f"  Поддиректории: {sub_dirs}")
                    print(f"  Файлы в текущей директории: {sub_files}")
                    for sub_file in sub_files:
                        if sub_file.endswith('.csv'):
                            for prefix in required_prefixes:
                                if sub_file.startswith(prefix) and required_prefixes[prefix] is None:
                                    required_prefixes[prefix] = os.path.join(sub_root_dir, sub_file)
                                    print(f"    НАЙДЕН ТРЕБУЕМЫЙ ФАЙЛ: {sub_file} по пути: {required_prefixes[prefix]}")
                                    break  # Нашли файл для этого префикса, переходим к следующему

                # Проверяем, все ли требуемые файлы были найдены (по их префиксам)
                if not all(value is not None for value in required_prefixes.values()):
                    missing_files = [p for p, path in required_prefixes.items() if path is None]
                    print(
                        f"Ошибка: Не все необходимые CSV файлы найдены в '{tracking_data_folder_path}'. Отсутствуют: {', '.join(missing_files)}. Пропускаем эту папку.")
                    continue

                location_file = required_prefixes['location_']
                motion_file = required_prefixes['motion_']
                acceleration_file = required_prefixes['acceleration_']

                merged_df = merge_sensor_data(location_file, motion_file, acceleration_file)

                if merged_df is not None:
                    output_filename = os.path.join(tracking_data_folder_path, 'merged_data.csv')
                    try:
                        merged_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
                        print(f"Результат объединенной таблицы для '{dir_name}' сохранен в файл: {output_filename}")
                        all_merged_dfs.append(merged_df)
                    except Exception as e:
                        print(f"Не удалось сохранить файл '{output_filename}': {e}")

    if all_merged_dfs:
        final_combined_df = pd.concat(all_merged_dfs, ignore_index=True)
        final_output_path = os.path.join(output_base_dir, "combined_all_tracking_data.csv")
        try:
            final_combined_df.to_csv(final_output_path, index=False, encoding='utf-8-sig')
            print(f"\nВсе объединенные данные сохранены в: {final_output_path}")
        except Exception as e:
            print(f"Не удалось сохранить итоговый объединенный файл: {e}")
    else:
        print("Не найдено данных для объединения.")


if __name__ == '__main__':
    main_zip_filename = 'data/export_2025-07-04_12-12-20.zip'  # Замените на имя вашего файла

    if os.path.exists(main_zip_filename):
        unzip_and_process_all_archives(main_zip_filename)
    else:
        print(
            f"Ошибка: Основной ZIP-файл '{main_zip_filename}' не найден. Пожалуйста, убедитесь, что он находится в той же директории, что и скрипт.")
