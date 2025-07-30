import os
import zipfile
from datetime import datetime
import re
import io  # Используется для работы с архивами в памяти

# --- 1. НАСТРОЙКА ---
INPUT_DIR = 'data'
OUTPUT_DIR = 'output'

# Определяем типы файлов, которые мы ищем, и их итоговые имена
FILE_TYPES = {
    'location': 'all_location.csv',
    'motion': 'all_motion.csv',
    'acceleration': 'all_acceleration.csv'
}


def key_from_filename(filename: str) -> datetime | str:
    if groups := re.search(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})", filename):
        return datetime.strptime(
            groups.group(1),
            '%Y-%m-%d_%H-%M-%S',
        )
    return filename

def prepare_data(data: list[str]) -> list[str]:
    result = []
    for raw_line in data:
        line = raw_line.strip()
        fields = line.split(',')
        timestamp = fields[0].split("+")[0]
        result.append(",".join([timestamp] + fields[1:]))
    return result

def main():
    """
    Главная функция для объединения данных из вложенных архивов.
    """
    print("Начало процесса объединения данных...")

    # Создаем выходную папку, если она не существует
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Выходная папка '{OUTPUT_DIR}' готова.")

    # Словарь для хранения информации о том, был ли уже записан заголовок для каждого типа файла
    headers_written = {file_type: False for file_type in FILE_TYPES}

    # --- 2. ПОИСК И СОРТИРОВКА ВНЕШНИХ АРХИВОВ ---
    try:
        # Получаем список всех файлов в папке INPUT_DIR
        all_files = os.listdir(INPUT_DIR)
        # Фильтруем, оставляя только нужные архивы, и сразу сортируем
        # Сортировка по имени файла обеспечивает хронологический порядок (YYYY-MM-DD_HH-MM-SS)
        top_level_zips = sorted([f for f in all_files if f.startswith('export_') and f.endswith('.zip')])

        if not top_level_zips:
            print(f"В папке '{INPUT_DIR}' не найдены архивы вида 'export_*.zip'.")
            return

    except FileNotFoundError:
        print(f"Ошибка: Папка '{INPUT_DIR}' не найдена. Пожалуйста, создайте ее и поместите туда архивы.")
        return

    # --- 3. ВЛОЖЕННЫЕ ЦИКЛЫ ДЛЯ ОБРАБОТКИ АРХИВОВ ---
    # Открываем итоговые файлы для записи. 'a' - режим дозаписи (append).
    with open(os.path.join(OUTPUT_DIR, FILE_TYPES['location']), 'w', encoding='utf-8', newline='') as f_loc, \
            open(os.path.join(OUTPUT_DIR, FILE_TYPES['motion']), 'w', encoding='utf-8', newline='') as f_mot, \
            open(os.path.join(OUTPUT_DIR, FILE_TYPES['acceleration']), 'w', encoding='utf-8', newline='') as f_acc:

        output_file_handlers = {
            'location': f_loc,
            'motion': f_mot,
            'acceleration': f_acc
        }

        # Перебираем внешние архивы в хронологическом порядке
        for top_zip_filename in top_level_zips:
            top_zip_path = os.path.join(INPUT_DIR, top_zip_filename)
            print(f"\n[1] Обработка внешнего архива: {top_zip_filename}")

            try:
                # Открываем внешний архив для чтения
                with zipfile.ZipFile(top_zip_path, 'r') as top_zip:
                    # Находим и сортируем вложенные архивы

                    raw_nested_zips = [name for name in top_zip.namelist() if name.startswith('tracking_data_')]
                    nested_zips = sorted(raw_nested_zips, key=key_from_filename)

                    # Перебираем вложенные архивы
                    for nested_zip_filename in nested_zips:
                        print(f"  [2] Обработка вложенного архива: {nested_zip_filename}")

                        # Читаем вложенный архив в память, чтобы не извлекать его на диск
                        nested_zip_data = top_zip.read(nested_zip_filename)

                        # Работаем с данными архива в памяти как с файлом
                        with zipfile.ZipFile(io.BytesIO(nested_zip_data), 'r') as nested_zip:
                            # Находим и сортируем CSV-файлы внутри вложенного архива
                            csv_files = sorted([name for name in nested_zip.namelist() if name.endswith('.csv')])

                            # --- 4. ОБРАБОТКА И ЗАПИСЬ CSV ---
                            for csv_filename in csv_files:
                                file_type = None
                                print("Обработка csv файла ", csv_filename)

                                # Определяем тип файла по его имени
                                if 'location' in csv_filename:
                                    file_type = 'location'
                                elif 'motion' in csv_filename:
                                    file_type = 'motion'
                                elif 'acceleration' in csv_filename:
                                    file_type = 'acceleration'

                                if file_type:
                                    print(f"    [3] Найден файл: {csv_filename}, тип: {file_type}")

                                    # Открываем CSV-файл из архива
                                    with nested_zip.open(csv_filename, 'r') as csv_file:
                                        # Используем TextIOWrapper для корректного чтения текста из бинарного потока
                                        csv_reader = io.TextIOWrapper(csv_file, 'utf-8')

                                        output_handler = output_file_handlers[file_type]

                                        _raw_data = csv_reader.readlines()
                                        _data = "\n".join(prepare_data(_raw_data))

                                        # Если заголовок для этого типа файла еще не был записан
                                        if not headers_written[file_type]:
                                            # Читаем и записываем весь файл целиком (вместе с заголовком)

                                            output_handler.write(_data + "\n")
                                            headers_written[file_type] = True
                                            print(f"      -> Записан заголовок и данные в {FILE_TYPES[file_type]}")
                                        else:
                                            # Пропускаем первую строку (заголовок)
                                            # next(csv_reader)
                                            # Записываем оставшиеся данные
                                            output_handler.write(_data + "\n")
                                            print(f"      -> Добавлены данные в {FILE_TYPES[file_type]}")

            except zipfile.BadZipFile:
                print(f"Ошибка: Архив '{top_zip_filename}' поврежден или не является ZIP-архивом. Пропускаем.")
            except Exception as e:
                print(f"Произошла непредвиденная ошибка при обработке {top_zip_filename}: {e}")

    print("\nПроцесс успешно завершен!")
    print("Итоговые файлы находятся в папке 'output':")
    for final_file in FILE_TYPES.values():
        print(f"- {final_file}")


if __name__ == "__main__":
    main()
