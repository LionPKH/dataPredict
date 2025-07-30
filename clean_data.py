import pandas as pd
import numpy as np
import os
import pywt
import matplotlib.pyplot as plt

# --- 1. НАСТРОЙКА ---
INPUT_DIR = 'output'
CLEANED_OUTPUT_DIR = 'output_cleaned'
ACCELERATION_FILE = 'all_acceleration.csv'
LOCATION_FILE = 'all_location.csv'
MOTION_FILE = 'all_motion.csv'

TIMESTAMP_COLUMN = 'timestamp'
ACC_COLUMN_NAMES = ['timestamp', 'ax', 'ay', 'az']
COLUMNS_TO_FILTER = ['ax', 'ay', 'az']

# <<< НОВЫЕ ПАРАМЕТРЫ: ОГРАНИЧЕНИЕ ВЫБРОСОВ (КЛИППИНГ) ---
# Установите в True, чтобы активировать шаг ограничения выбросов
ENABLE_CLIPPING = True
# !!! ВАЖНО: Задайте реалистичные пороги для ваших данных.
# Значения зависят от того, что измерялось. Для человека это может быть +/- 5g.
# Для автомобиля или дрона значения могут быть выше.
MIN_ACCELERATION = -0.8  # Нижний порог (например, в g)
MAX_ACCELERATION = -0.2  # Верхний порог (например, в g)


# --- 2. РЕАЛИЗАЦИЯ ВЕЙВЛЕТ-ДЕНОИЗИНГА (без изменений) ---
def apply_wavelet_denoising(data_series, wavelet='sym8', mode='soft'):
    # ... (код функции без изменений)
    signal = data_series.to_numpy()
    coeffs = pywt.wavedec(signal, wavelet, mode='per')
    sigma = np.median(np.abs(coeffs[-1] - np.median(coeffs[-1]))) / 0.6745
    threshold = sigma * np.sqrt(2 * np.log(len(signal)))
    new_coeffs = [coeffs[0]]
    for c in coeffs[1:]:
        new_coeffs.append(pywt.threshold(c, value=threshold, mode=mode))
    denoised_signal = pywt.waverec(new_coeffs, wavelet, mode='per')
    return denoised_signal[:len(signal)]


def main():
    print("Начало процесса очистки данных...")
    os.makedirs(CLEANED_OUTPUT_DIR, exist_ok=True)

    # --- 3. ЗАГРУЗКА И ОБЪЕДИНЕНИЕ ДАННЫХ (без изменений) ---
    try:
        # ... (код загрузки и объединения без изменений)
        acc_path = os.path.join(INPUT_DIR, ACCELERATION_FILE)
        loc_path = os.path.join(INPUT_DIR, LOCATION_FILE)
        mot_path = os.path.join(INPUT_DIR, MOTION_FILE)

        df_loc = pd.read_csv(loc_path)
        df_mot = pd.read_csv(mot_path)
        df_acc = pd.read_csv(acc_path, header=None)
        df_acc.columns = ACC_COLUMN_NAMES

        if TIMESTAMP_COLUMN in df_acc.columns and TIMESTAMP_COLUMN in df_loc.columns and TIMESTAMP_COLUMN in df_mot.columns:
            df_merged = pd.merge(df_loc, df_mot, on=TIMESTAMP_COLUMN, how='outer')
            df_merged = pd.merge(df_merged, df_acc, on=TIMESTAMP_COLUMN, how='outer')
            df_merged = df_merged.sort_values(by=TIMESTAMP_COLUMN).reset_index(drop=True)
        else:
            df_merged = pd.concat([df_loc, df_mot, df_acc], axis=1)
            df_merged = df_merged.loc[:, ~df_merged.columns.duplicated()]
    except Exception as e:
        print(f"Ошибка на этапе загрузки данных: {e}")
        return

    print("Данные успешно загружены и объединены.")

    # Проверка на наличие колонок перед обработкой
    if not all(col in df_merged.columns for col in COLUMNS_TO_FILTER):
        print(f"\nОшибка: Не найдены колонки для фильтрации: {COLUMNS_TO_FILTER}")
        return

    # --- 4. ЭТАП 1: КЛИППИНГ (Ограничение выбросов) ---
    if ENABLE_CLIPPING:
        print(f"\nПрименение ограничения (клиппинга) к данным. Порог: [{MIN_ACCELERATION}, {MAX_ACCELERATION}]")
        for col in COLUMNS_TO_FILTER:
            # Создаем новую колонку с "урезанными" данными, сохраняя оригинал
            clipped_col_name = f'{col}_clipped'
            df_merged[clipped_col_name] = df_merged[col].clip(lower=MIN_ACCELERATION, upper=MAX_ACCELERATION)

            # Считаем, сколько значений было изменено
            num_clipped = (df_merged[col] != df_merged[clipped_col_name]).sum()
            print(f" - Колонка '{col}': ограничено {num_clipped} выбросов.")

    # --- 5. ЭТАП 2: ФИЛЬТРАЦИЯ (Вейвлет-деноизинг) ---
    print("\nПрименение вейвлет-деноизинга...")
    for col in COLUMNS_TO_FILTER:
        # <<< ИЗМЕНЕНИЕ: Теперь мы фильтруем данные ПОСЛЕ клиппинга (если он был включен)
        if ENABLE_CLIPPING:
            input_col_for_filtering = f'{col}_clipped'
        else:
            input_col_for_filtering = col  # Если клиппинг отключен, берем исходные данные

        print(f" - Фильтрация колонки '{input_col_for_filtering}'...")

        signal_series = df_merged[input_col_for_filtering].interpolate(method='linear').fillna(method='bfill').fillna(
            method='ffill')

        filtered_col_name = f'{col}_final_filtered'
        df_merged[filtered_col_name] = apply_wavelet_denoising(signal_series)

    print("Фильтрация завершена.")

    # --- 6. СОХРАНЕНИЕ РЕЗУЛЬТАТА ---
    output_filename = os.path.join(CLEANED_OUTPUT_DIR, 'all_data_final_cleaned.csv')
    df_merged.to_csv(output_filename, index=False)
    print(f"\nОчищенные и объединенные данные сохранены в файл: {output_filename}")

    # --- 7. ВИЗУАЛИЗАЦИЯ ---
    try:
        print("Создание графика для сравнения...")
        axis_to_plot = COLUMNS_TO_FILTER[0]

        plt.figure(figsize=(17, 8))
        # Исходный сигнал
        plt.plot(df_merged.index, df_merged[axis_to_plot], 'r-', alpha=0.4, label=f'Исходный сигнал ({axis_to_plot})')

        # <<< ИЗМЕНЕНИЕ: Отображаем финальный результат двухэтапной очистки
        plt.plot(df_merged.index, df_merged[f'{axis_to_plot}_final_filtered'], 'b-', linewidth=2,
                 label=f'Результат (Ограничение + Вейвлет)')

        # Добавим линии порогов, если клиппинг был включен
        if ENABLE_CLIPPING:
            plt.axhline(y=MAX_ACCELERATION, color='k', linestyle='--', label=f'Верхний порог ({MAX_ACCELERATION})')
            plt.axhline(y=MIN_ACCELERATION, color='k', linestyle='--', label=f'Нижний порог ({MIN_ACCELERATION})')

        plt.title('Сравнение исходного сигнала и результата двухэтапной очистки')
        plt.xlabel('Отсчеты (samples)')
        plt.ylabel('Ускорение')
        plt.legend()
        plt.grid(True)

        plot_filename = os.path.join(CLEANED_OUTPUT_DIR, 'final_cleaning_comparison.png')
        plt.savefig(plot_filename)
        print(f"График сохранен в файл: {plot_filename}")

    except Exception as e:
        print(f"\nНе удалось создать график. Ошибка: {e}")


if __name__ == "__main__":
    main()