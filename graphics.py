import pandas as pd
import matplotlib.pyplot as plt

# --- НАСТРОЙКИ ---
# Укажите здесь начальное и конечное время для фильтрации.
# Если оставить строку пустой (''), то будет использоваться самое начало или конец данных.
start_time_str = ''
end_time_str = '2025-07-04T12:09:20.3200+0300'

# Укажите имена ваших файлов
acceleration_file = 'output/all_acceleration.csv'
location_file = 'output/all_location.csv'


# --- КОНЕЦ НАСТРОЕК ---


def load_and_prepare_data(file_path, column_names):
    """Функция для загрузки и базовой подготовки данных из CSV."""
    try:
        df = pd.read_csv(file_path, header=None, names=column_names)
    except FileNotFoundError:
        print(f"Ошибка: Файл '{file_path}' не найден.")
        return None

    if 'timestamp' in df.columns:
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        # Преобразуем все временные метки в UTC и делаем их "наивными" (tz-naive).
        # Это решает проблему сравнения.
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)
    else:
        print(f"Ошибка: В файле {file_path} не удалось найти столбец 'timestamp'.")
        return None

    return df


# 1. Загрузка данных об ускорении
accel_cols = ['timestamp', 'x', 'y', 'z']
df_accel = load_and_prepare_data(acceleration_file, accel_cols)

# 2. Загрузка данных о местоположении и скорости
loc_cols = ['timestamp', 'latitude', 'longitude', 'speed', 'altitude']
df_loc = load_and_prepare_data(location_file, loc_cols)

if df_accel is None or df_loc is None:
    exit()


# 3. Фильтрация данных по временному диапазону
def filter_by_time(df, start_str, end_str):
    """Фильтрует DataFrame по заданному временному диапазону."""
    if df is None:
        return pd.DataFrame()
    data_timezone = df['timestamp'].dt.tz

    filtered_df = df.copy()

    # Преобразуем введенное время в "наивный" объект



    if start_str:
        # Время для фильтра тоже преобразуется в наивный datetime
        naive_start_time = pd.to_datetime(start_time_str)
        # Делаем его "осведомленным", используя часовой пояс из данных
        # start_time = naive_start_time.tz_localize(data_timezone)
        filtered_df = filtered_df[filtered_df['timestamp'] >= naive_start_time]
    if end_str:
        naive_end_time = pd.to_datetime(end_time_str)
        # Делаем его "осведомленным", используя часовой пояс из данных
        # end_time = naive_end_time.tz_localize(data_timezone)
        filtered_df = filtered_df[filtered_df['timestamp'] <= naive_end_time]
    return filtered_df


filtered_accel = filter_by_time(df_accel, start_time_str, end_time_str)
filtered_loc = filter_by_time(df_loc, start_time_str, end_time_str)

if filtered_accel.empty and filtered_loc.empty:
    print("\nВ указанном временном диапазоне нет данных ни по ускорению, ни по скорости.")
    exit()

print(f"\nНайдено точек данных: Ускорение - {len(filtered_accel)}, Скорость - {len(filtered_loc)}.")

# 4. Создаем 4 графика
fig, axs = plt.subplots(4, 1, figsize=(15, 12), sharex=True)

title_start = start_time_str or 'начала'
title_end = end_time_str or 'конца'
fig.suptitle(f'Данные с сенсоров от {title_start} до {title_end} (время в UTC)', fontsize=16)

print(filtered_loc['speed'])
print(filtered_loc['timestamp'])
print(filtered_accel['timestamp'])
# --- Графики ускорения ---
axs[0].plot(filtered_accel['timestamp'], filtered_accel['x'], 'r.-', pd.to_numeric(filtered_loc['speed']), 'm.-',  label='Ускорение X')
axs[0].set_ylabel('Ускорение X (м/с²)')
axs[0].legend(loc='upper right')
axs[0].grid(True)

axs[1].plot(filtered_accel['timestamp'], filtered_accel['y'], 'g.-', pd.to_numeric(filtered_loc['speed']), 'm.-', label='Ускорение Y')
axs[1].set_ylabel('Ускорение Y (м/с²)')
axs[1].legend(loc='upper right')
axs[1].grid(True)

axs[2].plot(filtered_accel['timestamp'], filtered_accel['z'], 'b.-', pd.to_numeric(filtered_loc['speed']), 'm.-',  label='Ускорение Z')
axs[2].set_ylabel('Ускорение Z (м/с²)')
axs[2].legend(loc='upper right')
axs[2].grid(True)

# --- График скорости ---


axs[3].plot(filtered_loc['timestamp'], pd.to_numeric(filtered_loc['speed']), 'm.-', label='Скорость')
axs[3].set_ylabel('Скорость (м/с)')
axs[3].set_xlabel('Время (UTC)')
axs[3].legend(loc='upper right')
axs[3].grid(True)

# 5. Финальная настройка и отображение
fig.autofmt_xdate()
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.show()

