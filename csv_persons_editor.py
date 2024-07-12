import numpy as np
import pandas as pd

dataframe = pd.read_csv('class_full.csv', sep=';')

dataframe = dataframe.fillna(0.0)

dataframe = dataframe[['flat_tkn', 'final_class']]

dataframe = dataframe[dataframe['final_class'] < 10]

dataframe = dataframe.groupby(['flat_tkn'], as_index=False).agg([lambda x: np.ceil(np.mean(x))])

dataframe.columns = [head for head, *_ in dataframe.columns]

dataframe['residents_avg_count'] = dataframe['final_class'].astype(np.int64)

dataframe = dataframe[['flat_tkn', 'residents_avg_count']]

print(dataframe)
dataframe.to_csv('task.csv', sep=';', index=False)
