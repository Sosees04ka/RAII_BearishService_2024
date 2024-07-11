import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Шаг 1: Подключаемся к базе данных
conn = sqlite3.connect('data.db')
cursor = conn.cursor()

# Шаг 2: Добавляем столбец class_debt в таблицу Flat, если он еще не добавлен
try:
    cursor.execute("ALTER TABLE Flat ADD COLUMN class_debt INTEGER")
except sqlite3.OperationalError:
    # Если столбец уже существует, игнорируем ошибку
    pass

# Шаг 3: Извлекаем данные из debtAverage
query = "SELECT flatId, debtAverage FROM Flat"
df = pd.read_sql_query(query, conn)

print(df.shape[0])

# Получение столбца debtAverage
debt_average = df['debtAverage']

# Определение центроидов
min_debt = debt_average.min()
max_debt = debt_average.max()

centroid_0 = 0.1 * min_debt
centroid_1 = 0.05 * min_debt
centroid_2 = 0
centroid_3 = 0.05 * max_debt
centroid_4 = 0.1 * max_debt

centroids = np.array([centroid_0, centroid_1, centroid_2, centroid_3, centroid_4])

# Функция для классификации данных относительно центроидов
def classify_debt(value, centroids):
    if value == 0:
        return 2
    elif value < 0:
        distances = np.abs(centroids[:2] - value)
        return np.argmin(distances)
    else:
        distances = np.abs(centroids[3:] - value)
        return np.argmin(distances) + 3

# Применение функции классификации к столбцу debtAverage
df['class_debt'] = debt_average.apply(lambda x: classify_debt(x, centroids))

# Шаг 5: Вывод значений для каждого класса
for cluster_label in range(5):
    cluster_data = df[df['class_debt'] == cluster_label][['flatId', 'debtAverage']]
    print(f"Class {cluster_label} len:")
    print(len(cluster_data))
    print()

# Шаг 6: Обновляем базу данных с новыми значениями столбца class_debt
for index, row in df.iterrows():
    cursor.execute("UPDATE Flat SET class_debt = ? WHERE flatId = ?", (row['class_debt'], row['flatId']))

conn.commit()

# Шаг 7: Визуализация кластеризации
plt.scatter(df['debtAverage'], [0]*len(df), c=df['class_debt'], cmap='viridis')
plt.xlabel('Debt Average')
plt.title('Clustering')
plt.show()

# Шаг 8: Вывод всех значений с указанием класса
query_all = "SELECT flatId, debtAverage, class_debt FROM Flat"
df_all = pd.read_sql_query(query_all, conn)

print("All values with their assigned class:")
print(df_all)

# Закрываем соединение с базой данных
conn.close()
