import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- НАСТРОЙКИ ---
INPUT_DIR = 'output'
OUTPUT_DIR = 'output_cleaned'
LOCATION_FILE = 'all_location.csv'
ACCELERATION_FILE = 'all_acceleration.csv'
OUTPUT_FILENAME = 'speed_interpolated_improved.csv'

# --- НАСТРОЙКИ ИНТЕРПОЛЯЦИИ ---
INTERPOLATE_ORDER = 1  # Порядок сплайна для интерполяции (1=линейная, 2=квадратичная, 3=кубическая)

# --- КОНФИГУРАЦИЯ КОЛОНОК ---
LOC_COLUMN_NAMES = ['timestamp', 'latitude', 'longitude', 'speed', 'course']
ACC_COLUMN_NAMES = ['timestamp', 'x_accel', 'y_accel', 'z_accel']

TIMESTAMP_COLUMN = 'timestamp'
SPEED_COLUMN = 'speed'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def load_and_clean_location_data(file_path):
    """
    Загружает и очищает данные о местоположении, фильтруя некорректные значения скорости.
    """
    print(f"Загрузка данных местоположения из {file_path}...")
    
    # Загружаем данные
    df_location = pd.read_csv(file_path, header=0)
    
    print(f"Загружено {len(df_location)} записей местоположения")
    
    # Преобразуем временные метки
    df_location[TIMESTAMP_COLUMN] = pd.to_datetime(df_location[TIMESTAMP_COLUMN], format=DATETIME_FORMAT)
    
    # Фильтруем некорректные значения скорости (отрицательные и NaN)
    print("Фильтрация некорректных значений скорости...")
    initial_count = len(df_location)
    
    # Удаляем записи с отрицательной скоростью (обычно -1.0 означает отсутствие GPS данных)
    df_location = df_location[df_location[SPEED_COLUMN] >= 0]
    
    # Удаляем записи с NaN значениями скорости
    df_location = df_location.dropna(subset=[SPEED_COLUMN])
    
    filtered_count = len(df_location)
    print(f"Отфильтровано {initial_count - filtered_count} некорректных записей")
    print(f"Осталось {filtered_count} валидных записей скорости")
    
    # Устанавливаем временную метку как индекс
    df_location = df_location.set_index(TIMESTAMP_COLUMN)
    
    # Удаляем дубликаты по времени, оставляя первое значение
    df_location = df_location.loc[~df_location.index.duplicated(keep='first')]
    
    # Сортируем по времени
    df_location = df_location.sort_index()
    
    return df_location[[SPEED_COLUMN]]


def load_acceleration_data(file_path):
    """
    Загружает данные акселерометра.
    """
    print(f"Загрузка данных акселерометра из {file_path}...")
    
    # Загружаем данные
    df_acc = pd.read_csv(file_path, header=0)
    
    print(f"Загружено {len(df_acc)} записей акселерометра")
    
    # Преобразуем временные метки
    df_acc[TIMESTAMP_COLUMN] = pd.to_datetime(df_acc[TIMESTAMP_COLUMN], format=DATETIME_FORMAT)
    
    # Устанавливаем временную метку как индекс
    df_acc = df_acc.set_index(TIMESTAMP_COLUMN)
    
    # Сортируем по времени
    df_acc = df_acc.sort_index()
    
    return df_acc


def interpolate_speed_data(df_acc, df_location):
    """
    Выполняет сплайн-интерполяцию данных скорости на объединенную временную сетку.
    """
    print("Начало интерполяции данных скорости...")
    
    # Создаем объединенную временную сетку из всех доступных временных меток
    print("Создание объединенной временной сетки...")
    
    # Получаем все уникальные временные метки из обоих источников
    all_timestamps = pd.Index(df_acc.index.union(df_location.index)).sort_values()
    
    print(f"Временной диапазон GPS данных: {df_location.index.min()} - {df_location.index.max()}")
    print(f"Временной диапазон акселерометра: {df_acc.index.min()} - {df_acc.index.max()}")
    print(f"Объединенный временной диапазон: {all_timestamps.min()} - {all_timestamps.max()}")
    
    # Создаем DataFrame с объединенной временной сеткой
    df_merged = pd.DataFrame(index=all_timestamps)
    
    # Добавляем данные акселерометра
    df_merged = df_merged.join(df_acc, how='left')
    
    # Добавляем данные скорости
    df_merged = df_merged.join(df_location, how='left')
    
    # Создаем индикатор источника данных скорости СРАЗУ после объединения
    # 1 = оригинальные GPS данные, 0 = требуют интерполяции
    print("Создание индикатора источника данных скорости...")
    df_merged['speed_source'] = df_merged[SPEED_COLUMN].notna().astype(int)
    
    print(f"Объединено {len(df_merged)} записей")
    print(f"Записей с валидными данными скорости: {df_merged[SPEED_COLUMN].notna().sum()}")
    print(f"Записей требующих интерполяции: {df_merged[SPEED_COLUMN].isna().sum()}")
    print(f"Записей с оригинальными GPS данными: {df_merged['speed_source'].sum()}")
    print(f"Записей с интерполированными данными: {(df_merged['speed_source'] == 0).sum()}")
    
    # Проверяем, есть ли валидные данные для интерполяции
    if df_merged[SPEED_COLUMN].notna().sum() < 2:
        print("ОШИБКА: Недостаточно валидных данных скорости для интерполяции!")
        return None
    
    # Выполняем сплайн-интерполяцию для скорости
    print(f"Выполнение сплайн-интерполяции скорости (порядок сплайна: {INTERPOLATE_ORDER})...")
    df_merged[SPEED_COLUMN] = df_merged[SPEED_COLUMN].interpolate(method='spline', order=INTERPOLATE_ORDER)

    # Обрабатываем граничные случаи с использованием современного синтаксиса
    print("Обработка граничных значений...")

    # Заполняем значения в начале (backward fill)
    df_merged[SPEED_COLUMN] = df_merged[SPEED_COLUMN].bfill()

    # Заполняем значения в конце (forward fill)
    df_merged[SPEED_COLUMN] = df_merged[SPEED_COLUMN].ffill()

    # Проверяем результат
    remaining_na = df_merged[SPEED_COLUMN].isna().sum()
    if remaining_na > 0:
        print(f"ВНИМАНИЕ: Осталось {remaining_na} неинтерполированных значений")
        # Заполняем оставшиеся NaN нулями
        df_merged[SPEED_COLUMN] = df_merged[SPEED_COLUMN].fillna(0)
    
    print("Интерполяция завершена успешно!")
    
    return df_merged


def calculate_speed_change(df_merged):
    """
    Вычисляет изменение скорости (ускорение) между соседними временными точками.
    """
    print("Расчет изменения скорости...")
    
    # Вычисляем разность скоростей между соседними точками
    df_merged['speed_change'] = df_merged[SPEED_COLUMN].diff()
    
    # Для первой записи устанавливаем изменение скорости равным 0
    df_merged.loc[df_merged.index[0], 'speed_change'] = 0.0
    
    # Статистика по изменению скорости
    print(f"Статистика изменения скорости:")
    print(f"  Минимум: {df_merged['speed_change'].min():.6f} м/с за интервал")
    print(f"  Максимум: {df_merged['speed_change'].max():.6f} м/с за интервал")
    print(f"  Среднее: {df_merged['speed_change'].mean():.6f} м/с за интервал")
    print(f"  Стандартное отклонение: {df_merged['speed_change'].std():.6f}")
    
    return df_merged


def analyze_interpolation_quality(df_merged, df_location):
    """
    Анализирует качество интерполяции.
    """
    print("\n=== АНАЛИЗ КАЧЕСТВА ИНТЕРПОЛЯЦИИ ===")
    
    # Статистика по исходным данным
    print(f"Исходные данные скорости:")
    print(f"  Минимум: {df_location[SPEED_COLUMN].min():.3f}")
    print(f"  Максимум: {df_location[SPEED_COLUMN].max():.3f}")
    print(f"  Среднее: {df_location[SPEED_COLUMN].mean():.3f}")
    print(f"  Медиана: {df_location[SPEED_COLUMN].median():.3f}")
    
    # Статистика по интерполированным данным
    print(f"\nИнтерполированные данные скорости:")
    print(f"  Минимум: {df_merged[SPEED_COLUMN].min():.3f}")
    print(f"  Максимум: {df_merged[SPEED_COLUMN].max():.3f}")
    print(f"  Среднее: {df_merged[SPEED_COLUMN].mean():.3f}")
    print(f"  Медиана: {df_merged[SPEED_COLUMN].median():.3f}")
    
    # Временной диапазон
    print(f"\nВременной диапазон:")
    print(f"  Начало: {df_merged.index.min()}")
    print(f"  Конец: {df_merged.index.max()}")
    print(f"  Продолжительность: {df_merged.index.max() - df_merged.index.min()}")


def main():
    """
    Главная функция сплайн-интерполяции данных скорости.
    """
    print("=== СПЛАЙН-ИНТЕРПОЛЯЦИЯ ДАННЫХ СКОРОСТИ ===")
    print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Порядок сплайна: {INTERPOLATE_ORDER}")
    
    # Создаем выходную директорию
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        # Пути к файлам
        loc_path = os.path.join(INPUT_DIR, LOCATION_FILE)
        acc_path = os.path.join(INPUT_DIR, ACCELERATION_FILE)
        
        # Проверяем существование файлов
        if not os.path.exists(loc_path):
            print(f"ОШИБКА: Файл {loc_path} не найден!")
            return
        
        if not os.path.exists(acc_path):
            print(f"ОШИБКА: Файл {acc_path} не найден!")
            return
        
        # Загружаем и очищаем данные местоположения
        df_location = load_and_clean_location_data(loc_path)
        
        if df_location.empty:
            print("ОШИБКА: Нет валидных данных скорости для интерполяции!")
            return
        
        # Загружаем данные акселерометра
        df_acc = load_acceleration_data(acc_path)
        
        # Выполняем интерполяцию
        df_merged = interpolate_speed_data(df_acc, df_location)
        
        if df_merged is None:
            print("ОШИБКА: Интерполяция не удалась!")
            return
        
        # Вычисляем изменение скорости
        df_merged = calculate_speed_change(df_merged)
        
        # Анализируем качество интерполяции
        analyze_interpolation_quality(df_merged, df_location)
        
        # Сохраняем результат
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
        print(f"\nСохранение результата в файл: {output_path}")
        
        # Подготавливаем данные для сохранения - включаем timestamp, данные акселерометра, скорость, изменение скорости и источник данных
        columns_to_save = ['x_accel', 'y_accel', 'z_accel', SPEED_COLUMN, 'speed_change', 'speed_source']
        result_df = df_merged[columns_to_save].reset_index()
        result_df.to_csv(output_path, index=False, mode="w")
        
        print(f"Результат сохранен: {len(result_df)} записей")
        print(f"Размер файла: {os.path.getsize(output_path) / (1024*1024):.1f} МБ")
        
        print("\n=== ПРОЦЕСС ЗАВЕРШЕН УСПЕШНО! ===")
        
    except Exception as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()