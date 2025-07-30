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

# Имя колонки для объединения. Должно совпадать с именем в ACC_COLUMN_NAMES
TIMESTAMP_COLUMN = 'timestamp'

# <<< ИЗМЕНЕНИЕ: Задаем имена столбцов для файла all_acceleration.csv
# !!! ВАЖНО: Проверьте и при необходимости измените этот список в соответствии
# с реальным порядком столбцов в вашем файле.
# Например, если у вас нет временной метки, используйте: ['ax', 'ay', 'az']
ACC_COLUMN_NAMES = ['timestamp', 'ax', 'ay', 'az']

# Имена столбцов, которые мы будем фильтровать
COLUMNS_TO_FILTER = ['ax', 'ay', 'az']


# --- 2. РЕАЛИЗАЦИЯ ВЕЙВЛЕТ-ДЕНОИЗИНГА (без изменений) ---

def apply_wavelet_denoising(data_series, wavelet='sym8', mode='soft'):
    """
    Применяет вейвлет-деноизинг к временному ряду (pandas Series).
    """
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
    """
    Главная функция для загрузки, фильтрации вейвлетами и сохранения данных.
    """
    print("Начало процесса очистки данных с помощью вейвлет-преобразования...")
    os.makedirs(CLEANED_OUTPUT_DIR, exist_ok=True)

    # --- 3. ЗАГРУЗКА И ОБЪЕДИНЕНИЕ ДАННЫХ ---
    try:
        acc_path = os.path.join(INPUT_DIR, ACCELERATION_FILE)
        loc_path = os.path.join(INPUT_DIR, LOCATION_FILE)
        mot_path = os.path.join(INPUT_DIR, MOTION_FILE)

        # Предполагаем, что в этих файлах заголовки есть
        df_loc = pd.read_csv(loc_path)
        df_mot = pd.read_csv(mot_path)

        # <<< ИЗМЕНЕНИЕ: Читаем файл акселерометра без заголовка
        print(f"Чтение файла '{ACCELERATION_FILE}' без заголовков...")
        df_acc = pd.read_csv(acc_path, header=None)

        # <<< ИЗМЕНЕНИЕ: Присваиваем имена столбцов
        print(f"Присвоение имен столбцов: {ACC_COLUMN_NAMES}")
        df_acc.columns = ACC_COLUMN_NAMES

    except FileNotFoundError as e:
        print(f"Ошибка: Не найден файл - {e.filename}")
        print("Пожалуйста, убедитесь, что файлы из предыдущего шага находятся в папке 'output'.")
        return
    except Exception as e:
        print(f"Произошла ошибка при загрузке данных: {e}")
        return

    # Объединение данных
    if TIMESTAMP_COLUMN in df_acc.columns and TIMESTAMP_COLUMN in df_loc.columns and TIMESTAMP_COLUMN in df_mot.columns:
        print(f"Объединение данных по колонке '{TIMESTAMP_COLUMN}'...")
        df_merged = pd.merge(df_loc, df_mot, on=TIMESTAMP_COLUMN, how='outer')
        df_merged = pd.merge(df_merged, df_acc, on=TIMESTAMP_COLUMN, how='outer')
        df_merged = df_merged.sort_values(by=TIMESTAMP_COLUMN).reset_index(drop=True)
    else:
        print(f"Внимание: колонка '{TIMESTAMP_COLUMN}' не найдена во всех файлах. Объединение по индексам.")
        df_merged = pd.concat([df_loc, df_mot, df_acc], axis=1)
        df_merged = df_merged.loc[:, ~df_merged.columns.duplicated()]

    print("Данные успешно загружены и объединены.")

    # --- 4. ФИЛЬТРАЦИЯ ДАННЫХ ---
    if not all(col in df_merged.columns for col in COLUMNS_TO_FILTER):
        print("\nОшибка: Не найдены колонки для фильтрации ускорения.")
        print(f"Ожидались колонки с именами: {COLUMNS_TO_FILTER}")
        print(f"Найденные колонки: {df_merged.columns.tolist()}")
        return

    print("\nПрименение вейвлет-деноизинга к данным ускорения...")
    for col in COLUMNS_TO_FILTER:
        print(f" - Фильтрация колонки '{col}'...")
        # Заполняем возможные пропуски в данных перед фильтрацией
        signal_series = df_merged[col].interpolate(method='linear').fillna(method='bfill').fillna(method='ffill')

        filtered_col_name = f'{col}_wavelet_filtered'
        df_merged[filtered_col_name] = apply_wavelet_denoising(signal_series)

    print("Фильтрация завершена.")

    # --- 5. СОХРАНЕНИЕ РЕЗУЛЬТАТА ---
    output_filename = os.path.join(CLEANED_OUTPUT_DIR, 'all_data_wavelet_cleaned.csv')
    df_merged.to_csv(output_filename, index=False)
    print(f"\nОчищенные и объединенные данные сохранены в файл: {output_filename}")

    # --- 6. ВИЗУАЛИЗАЦИЯ ---
    try:
        print("Создание графика для сравнения...")
        axis_to_plot = COLUMNS_TO_FILTER[0]

        plt.figure(figsize=(15, 7))
        plt.plot(df_merged.index, df_merged[axis_to_plot], 'r-', alpha=0.5, label=f'Исходный сигнал ({axis_to_plot})')
        plt.plot(df_merged.index, df_merged[f'{axis_to_plot}_wavelet_filtered'], 'g-', linewidth=2,
                 label=f'Вейвлет-фильтр ({axis_to_plot})')
        plt.title('Сравнение исходного сигнала и результата вейвлет-фильтрации')
        plt.xlabel('Отсчеты (samples)')
        plt.ylabel('Ускорение')
        plt.legend()
        plt.grid(True)

        plot_filename = os.path.join(CLEANED_OUTPUT_DIR, 'wavelet_filter_comparison.png')
        plt.savefig(plot_filename)
        print(f"График сохранен в файл: {plot_filename}")

    except Exception as e:
        print(f"\nНе удалось создать график. Ошибка: {e}")


if __name__ == "__main__":
    main()