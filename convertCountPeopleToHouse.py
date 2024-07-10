import sqlite3
import pandas as pd

# Шаг 1: Подключаемся к базе данных
conn = sqlite3.connect('data.db')
cursor = conn.cursor()

# Шаг 2: Добавляем новый столбец count_people в таблицу House (если его еще нет)
try:
    cursor.execute('ALTER TABLE House ADD COLUMN count_people INTEGER')
    conn.commit()
except sqlite3.OperationalError:
    # Столбец уже существует
    pass

# Шаг 3: Читаем CSV файл
df = pd.read_csv(filepath_or_buffer='class_full.csv', delimiter=';',
                 dtype={'volume_cold': float, 'volume_hot': float, 'volume_electr': float})

# Проверка названий столбцов
print(df)

# Убедимся, что столбец существует в DataFrame
if 'final_class' not in df.columns:
    raise ValueError("Столбец 'final_class' не найден в CSV файле")

# Шаг 4: Обновляем только первые 10 значений в таблице House
for index, row in df.iterrows():
    count_people = row['final_class']
    cursor.execute('UPDATE House SET count_people = ? WHERE rowid = ?', (count_people, index))
    print(index)

conn.commit()

# Закрываем соединение с базой данных
conn.close()
