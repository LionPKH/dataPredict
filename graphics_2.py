import pandas as pd
import matplotlib.pyplot as plt

# --- НАСТРОЙКИ ---
start_time_str = ''
end_time_str = '2025-07-04 12:12:00'

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
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)
    else:
        print(f"Ошибка: В файле {file_path} не удалось найти столбец 'timestamp'.")
        return None
    return df


# 1. Загрузка данных
accel_cols = ['timestamp', 'x', 'y', 'z']
df_accel = load_and_prepare_data(acceleration_file, accel_cols)

loc_cols = ['timestamp', 'latitude', 'longitude', 'speed', 'altitude']
df_loc = load_and_prepare_data(location_file, loc_cols)

if df_accel is None or df_loc is None:
    exit()

# 2. Подготовка данных о скорости
df_loc['speed'] = pd.to_numeric(df_loc['speed'], errors='coerce')
df_loc['speed_increment'] = df_loc['speed'].diff()
df_loc.dropna(subset=['speed'], inplace=True)


# 3. Фильтрация данных по временному диапазону
def filter_by_time(df, start_str, end_str):
    """Фильтрует DataFrame по заданному временному диапазону."""
    if df is None or df.empty:
        return pd.DataFrame()

    filtered_df = df.copy()

    if start_str:
        # Сначала создаем "наивный" timestamp
        naive_start_time = pd.to_datetime(start_str)
        # Затем явно указываем, что это время в UTC, чтобы типы совпали
        start_time = naive_start_time.tz_localize('UTC+0300')
        filtered_df = filtered_df[filtered_df['timestamp'] >= start_time]

    if end_str:
        # Делаем то же самое для конечного времени
        naive_end_time = pd.to_datetime(end_str)
        end_time = naive_end_time.tz_localize('UTC+0300')
        filtered_df = filtered_df[filtered_df['timestamp'] <= end_time]

    return filtered_df


filtered_accel = filter_by_time(df_accel, start_time_str, end_time_str)
filtered_loc = filter_by_time(df_loc, start_time_str, end_time_str)

if filtered_accel.empty or filtered_loc.empty:
    print("\nВ указанном временном диапазоне нет данных (ускорение или скорость).")
    exit()

print(f"\nНайдено точек данных: Ускорение - {len(filtered_accel)}, Скорость - {len(filtered_loc)}.")

# 4. Создаем графики
fig, axs = plt.subplots(3, 1, figsize=(16, 14), sharex=True)

title_start = start_time_str or 'начала'
title_end = end_time_str or 'конца'
fig.suptitle(f'Ускорение, скорость и приращение скорости от {title_start} до {title_end}', fontsize=16)

accel_axes_info = [
    {'axis': 'x', 'color': 'red', 'label': 'Ускорение X'},
    {'axis': 'y', 'color': 'green', 'label': 'Ускорение Y'},
    {'axis': 'z', 'color': 'blue', 'label': 'Ускорение Z'}
]

for i, info in enumerate(accel_axes_info):
    ax1 = axs[i]

    color = info['color']
    ax1.set_ylabel(f'{info["label"]} (м/с²)', color=color)
    line1 = ax1.plot(filtered_accel['timestamp'], filtered_accel[info['axis']], color=color, linestyle='-', marker='.',
                     label=info['label'])
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.grid(True, which='both', linestyle='--', linewidth=0.5)

    ax2 = ax1.twinx()
    ax2.set_ylabel('Скорость (м/с)', color='purple')
    line2 = ax2.plot(filtered_loc['timestamp'], filtered_loc['speed'], color='purple', linestyle='-', label='Скорость')
    line3 = ax2.plot(filtered_loc['timestamp'], filtered_loc['speed_increment'], color='cyan', linestyle=':',
                     label='Приращение скорости')
    ax2.tick_params(axis='y', labelcolor='purple')
    ax2.axhline(0, color='cyan', linestyle='--', linewidth=0.7)

    lines = line1 + line2 + line3
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left')

axs[-1].set_xlabel('Время (UTC)')

# 5. Финальная настройка и отображение
fig.autofmt_xdate()
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.show()