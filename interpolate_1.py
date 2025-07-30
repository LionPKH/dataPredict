import pandas as pd
import os

# --- 1. НАСТРОЙКА ---
INPUT_DIR = 'output'
OUTPUT_DIR = 'output_cleaned'
LOCATION_FILE = 'all_location.csv'
ACCELERATION_FILE = 'all_acceleration.csv'
OUTPUT_FILENAME = 'speed_interpolated_1.csv'

# --- КОНФИГУРАЦИЯ КОЛОНОК ---
# Убедитесь, что порядок здесь соответствует вашим файлам
LOC_COLUMN_NAMES = ['timestamp', 'latitude', 'longitude', 'speed', 'accuracy']
ACC_COLUMN_NAMES = ['timestamp', 'ax', 'ay', 'az']

TIMESTAMP_COLUMN = 'timestamp'
SPEED_COLUMN = 'speed'


def main():
    """
    Главная функция для корректной интерполяции данных о скорости по временной шкале.
    """
    print("Начало процесса интерполяции скорости (исправленная версия)...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- 2. ЗАГРУЗКА ДАННЫХ И ПРЕОБРАЗОВАНИЕ ВРЕМЕНИ ---
    try:
        loc_path = os.path.join(INPUT_DIR, LOCATION_FILE)
        acc_path = os.path.join(INPUT_DIR, ACCELERATION_FILE)

        # --- Обработка файла локации (редкие данные) ---
        print(f"Загрузка и обработка '{LOCATION_FILE}'...")
        df_location = pd.read_csv(loc_path, header=None, names=LOC_COLUMN_NAMES)

        # <<< КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Преобразуем текст в настоящую дату и время
        df_location[TIMESTAMP_COLUMN] = pd.to_datetime(df_location[TIMESTAMP_COLUMN])

        # <<< КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Делаем время индексом
        df_location = df_location.set_index(TIMESTAMP_COLUMN)
        # Оставляем только нужную колонку и удаляем дубликаты по индексу (времени)
        df_location = df_location[[SPEED_COLUMN]].loc[~df_location.index.duplicated(keep='first')]

        # --- Обработка файла акселерометра (плотная временная сетка) ---
        print(f"Загрузка и обработка '{ACCELERATION_FILE}'...")
        df_acc = pd.read_csv(acc_path, header=None, names=ACC_COLUMN_NAMES)

        # <<< КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Здесь тоже преобразуем текст во время
        df_acc[TIMESTAMP_COLUMN] = pd.to_datetime(df_acc[TIMESTAMP_COLUMN])

        # <<< КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: И тоже делаем время индексом
        df_acc = df_acc.set_index(TIMESTAMP_COLUMN)

    except Exception as e:
        print(f"Произошла ошибка при загрузке или обработке файлов: {e}")
        return

    # --- 3. ОБЪЕДИНЕНИЕ И ИНТЕРПОЛЯЦИЯ НА ОСНОВЕ ИНДЕКСА ---
    print("Объединение данных по временному индексу...")

    # Создаем объединенный DataFrame, взяв за основу плотный индекс из файла ускорений
    # и присоединив к нему колонку скорости. Там, где время не совпадает, будет NaN.
    # df_merged = df_acc.join(df_location)
    df_merged = df_location.reindex(df_acc.index, method=None)

    # --- 4. КОРРЕКТНАЯ ИНТЕРПОЛЯЦИЯ ПО ВРЕМЕНИ ---
    print("Выполнение интерполяции по временной шкале...")

    # <<< КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: метод 'time' использует индекс для пропорционального расчета
    df_merged[SPEED_COLUMN] = df_merged[SPEED_COLUMN].interpolate(method='time')

    # Обработка крайних случаев (до первого и после последнего значения)
    print("Обработка крайних значений...")
    df_merged[SPEED_COLUMN] = df_merged[SPEED_COLUMN].fillna(method='bfill').fillna(method='ffill')

    if df_merged[SPEED_COLUMN].isnull().any():
        print("Внимание: Не удалось интерполировать значения. Заполняем нулями.")
        df_merged[SPEED_COLUMN] = df_merged[SPEED_COLUMN].fillna(0)

    print(f"Интерполяция завершена. Количество строк: {len(df_merged)}")

    # --- 5. СОХРАНЕНИЕ РЕЗУЛЬТАТА ---
    # Возвращаем timestamp из индекса обратно в колонку для сохранения в CSV
    final_df = df_merged[[SPEED_COLUMN]].reset_index()

    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
    print(f"Сохранение результата в файл: {output_path}")
    final_df.to_csv(output_path, index=False)

    print("\nПроцесс успешно завершен!")


if __name__ == "__main__":
    main()