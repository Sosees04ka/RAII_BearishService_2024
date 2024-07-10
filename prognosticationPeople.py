import pandas as pd
from scipy.spatial import cKDTree
import numpy as np

pd.set_option('display.max_columns', None)

# Загрузка данных из файла CSV с указанием типов столбцов
df = pd.read_csv(filepath_or_buffer='raai_school_2024.csv', delimiter=';',
                 dtype={'volume_cold': float, 'volume_hot': float, 'volume_electr': float})

# Находим максимальное значение в столбце volume_cold, исключая NaN
max_value_cold = df['volume_cold'].max()
max_value_hot = df['volume_hot'].max()
max_value_electr = df['volume_electr'].max()

# Определяем количество классов
base_value_cold = 6.935
base_value_hot = 4.747
base_value_electr = 70

num_classes_cold = int(np.ceil(max_value_cold / base_value_cold))
num_classes_hot = int(np.ceil(max_value_hot / base_value_hot))
num_classes_electr = int(np.ceil(max_value_electr / base_value_electr))

# Определяем ключевые точки
key_points_cold = np.array([base_value_cold * i for i in range(1, num_classes_cold + 1)])
key_points_hot = np.array([base_value_hot * i for i in range(1, num_classes_hot + 1)])
key_points_electr = np.array([base_value_electr * i for i in range(1, num_classes_electr + 1)])

# Функция для определения класса с использованием cKDTree
def classify_volumes(volumes, key_points):
    tree = cKDTree(key_points[:, None])
    _, idx = tree.query(volumes[:, None])
    return idx + 1

# Применяем функцию к столбцу volume_hot и создаем новые столбцы class_volume_*, игнорируя NaN
valid_indices_cold = df['volume_cold'].notna()
df.loc[valid_indices_cold, 'class_volume_cold'] = classify_volumes(df.loc[valid_indices_cold, 'volume_cold'].values, key_points_cold)

valid_indices_hot = df['volume_hot'].notna()
df.loc[valid_indices_hot, 'class_volume_hot'] = classify_volumes(df.loc[valid_indices_hot, 'volume_hot'].values, key_points_hot)

valid_indices_electr = df['volume_electr'].notna()
df.loc[valid_indices_electr, 'class_volume_electr'] = classify_volumes(df.loc[valid_indices_electr, 'volume_electr'].values, key_points_electr)

# Функция для вычисления финального класса с пропуском NaN
def compute_final_class(row):
    if pd.notna(row['class_volume_cold']) and pd.notna(row['class_volume_hot']) and pd.notna(row['class_volume_electr']):
        return int(round(
            row['class_volume_cold'] * 0.35 +
            row['class_volume_hot'] * 0.45 +
            row['class_volume_electr'] * 0.2
        ))
    elif pd.notna(row['class_volume_cold']) and pd.notna(row['class_volume_hot']):
        return int(round(
            row['class_volume_cold'] * 0.4 +
            row['class_volume_hot'] * 0.6
        ))
    elif pd.notna(row['class_volume_cold']) and pd.notna(row['class_volume_electr']):
        return int(round(
            row['class_volume_cold'] * 0.7 +
            row['class_volume_electr'] * 0.3
        ))
    elif pd.notna(row['class_volume_hot']) and pd.notna(row['class_volume_electr']):
        return int(round(
            row['class_volume_hot'] * 0.8 +
            row['class_volume_electr'] * 0.2
        ))
    elif pd.notna(row['class_volume_hot']):
        return int(round(
            row['class_volume_hot']
        ))
    elif pd.notna(row['class_volume_cold']):
        return int(round(
            row['class_volume_cold']
        ))
    elif pd.notna(row['class_volume_electr']):
        return int(round(
            row['class_volume_electr']
        ))
    else:
        return int(0)

# Вычисляем взвешенную сумму классов
df['final_class'] = df.apply(compute_final_class, axis=1)
print(df.shape[0])

# Сохраняем результат в новый CSV файл
df.to_csv('class_full.csv', sep=';', index=False)
