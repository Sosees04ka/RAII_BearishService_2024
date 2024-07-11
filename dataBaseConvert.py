import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from processing import csv_to_unix_time_df
import sqlite3

connection = sqlite3.connect('data.db')
cursor = connection.cursor()

drop_sql = "DROP TABLE IF EXISTS House;"

table_sql = """
    CREATE TABLE House (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        house_tkn INTEGER,
        flat_tkn INTEGER,
        unix_payment_period INTEGER,
        income FLOAT,
        debt FLOAT,
        raised FLOAT,
        volume_cold FLOAT,
        volume_hot FLOAT,
        volume_electr FLOAT,
        anomaly BOOLEAN DEFAULT FALSE
    )
"""

insert_sql = """
    INSERT INTO House 
    (id, house_tkn, flat_tkn, unix_payment_period, income, debt, raised, volume_cold, volume_hot, volume_electr, anomaly) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

select_sql = "SELECT * FROM House LIMIT 10;"

get_count_sql = "SELECT COUNT(*) FROM House;"

if __name__ == "__main__":
    try:
        cursor.execute(drop_sql)
        connection.commit()
        cursor.execute(table_sql)
        connection.commit()

        filename = "raai_school_2024.csv"
        data = csv_to_unix_time_df(filename)

        statistics_by_house: pd.DataFrame = data.fillna(0.0)
        statistics_by_house = statistics_by_house.drop(['payment_period'], axis=1)
        statistics_by_house = statistics_by_house[['volume_cold', 'volume_hot', 'volume_electr', 'house_tkn']]

        statistics_by_house_grouped = statistics_by_house.groupby(['house_tkn'], as_index=False)

        statistics_by_house = statistics_by_house_grouped.agg(['min', 'max', 'mean'])

        statistics_by_house.columns = ["_".join(head) for head in statistics_by_house.columns]

        house = statistics_by_house.iloc[0]

        cold_water_min = house['volume_cold_min']
        cold_water_max = house['volume_cold_max']
        cold_water_mean = house['volume_cold_mean']

        print(cold_water_min, cold_water_max, cold_water_mean)

        data_for_plot = data[data['house_tkn'] == 2]
        data_for_plot = data_for_plot[['volume_cold']]
        print(data_for_plot)

        data_for_plot.plot.line(figsize=(20, 6))
        plt.axhline(cold_water_min, color='c')
        plt.axhline(cold_water_mean, color='r')
        plt.axhline(cold_water_max, color='m')
        plt.show()

        # print(statistics_by_house)
        # statistics_by_house.to_csv('test.csv')

        # for index, row in data.iterrows():
        #     print(row)
        # record = (
        #     index,
        #     row['house_tkn'],
        #     row['flat_tkn'],
        #     row['unix_payment_period'],
        #     row['income'],
        #     row['debt'],
        #     row['raised'],
        #     row['volume_cold'],
        #     row['volume_hot'],
        #     row['volume_electr'],
        #     False
        # )
        #
        # cursor.execute(insert_sql, record)
        # if index % 10_000 == 0:
        #     print(f"Commited {index}!")
        #     connection.commit()

    except Exception as e:
        connection.rollback()
        print(f"Error occurred: {e}")
    finally:
        cursor.execute(get_count_sql)
        records = cursor.fetchall()

        print(records)

        connection.close()
