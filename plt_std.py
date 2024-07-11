import numpy as np

from processing import csv_to_unix_time_df
import pandas as pd
import matplotlib.pyplot as plt

column_to_research = "volume_cold"
treshold = .65
house_tkn = 2


def rel_std(x: np.float64, y: np.float64) -> np.float64:
    """
    Относительное отклонение
    """
    if y != 0:
        return (x - y) / y
    return np.float64(.0)


if __name__ == '__main__':
    filename = "raai_school_2024.csv"
    data = csv_to_unix_time_df(filename)

    statistics_by_house: pd.DataFrame = data.fillna(.0)
    statistics_by_house = statistics_by_house.drop(['payment_period'], axis=1)
    statistics_by_house = statistics_by_house[['volume_cold', 'volume_hot', 'volume_electr', 'house_tkn']]

    statistics_by_house_grouped = statistics_by_house.groupby(['house_tkn'], as_index=True)
    statistics_by_house = statistics_by_house_grouped.agg(['min', 'max', 'mean', 'std'])
    statistics_by_house.columns = ["_".join(head) for head in statistics_by_house.columns]

    house = statistics_by_house[statistics_by_house['house_tkn_'] == house_tkn].iloc[0]

    cold_water_min = house[f'{column_to_research}_min']
    cold_water_max = house[f'{column_to_research}_max']
    cold_water_mean = house[f'{column_to_research}_mean']
    cold_water_std = house[f'{column_to_research}_std']

    print(cold_water_min, cold_water_max, cold_water_mean)

    data_for_plot = data[data['house_tkn'] == house_tkn]
    data_for_plot = data_for_plot[['volume_cold']]
    data_for_plot = data_for_plot.dropna()
    data_for_plot = data_for_plot[data_for_plot['volume_cold'] > 0.0]

    data_for_plot['volume_cold'].plot.line(figsize=(20, 10), alpha=0.7)
    data_for_plot['fil_volume_cold'] = (data_for_plot['volume_cold']
                                        .apply(lambda x: -1 if rel_std(x, cold_water_mean) > treshold else x))
    data_for_plot['fil_volume_cold'].plot.line(figsize=(20, 10), color='purple')

    plt.axhline(cold_water_min, color='c', label="Min")
    plt.axhline(cold_water_mean, color='r', label="Mean")
    plt.axhline(cold_water_max, color='m', label="Max")
    plt.axhline(cold_water_std, color='g', label="Std")
    plt.axhline(cold_water_std * (treshold + 1), color='g', label="Std")

    plt.legend(('Холодная вода', 'min', 'mean', 'max', 'std'))
    plt.show()
