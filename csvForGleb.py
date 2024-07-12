import numpy as np
import pandas as pd
import math

# Загрузка файла class_full.csv
# Шаг 3: Читаем CSV файл
df = pd.read_csv(filepath_or_buffer='class_full.csv', delimiter=';',
                 dtype={'volume_cold': float, 'volume_hot': float, 'volume_electr': float})

# Шаг 2: Группировка по столбцу flat_tkn и расчет среднего значения final_class
grouped_df = df.groupby('flat_tkn')['final_class'].mean()

# Шаг 3: Округление средних значений в большую сторону
rounded_df = np.ceil(grouped_df).astype(int)

print(rounded_df)

# Шаг 4: Преобразование в DataFrame для сохранения
final_df = rounded_df.reset_index()
final_df.columns = ['flat_tkn', 'residents_avg_count']

# Шаг 5: Сохранение результата в final.csv
final_df.to_csv('final.csv', index=False, sep=';')

print("Файл final.csv успешно сохранен.")


