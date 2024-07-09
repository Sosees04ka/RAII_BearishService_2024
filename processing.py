import pandas as pd
import math


def csv_to_unixtime_df(file_name):
    df = pd.read_csv(file_name, delimiter=';')
    df['unix_payment_period'] = pd.to_datetime(df['payment_period']).astype(int) // math.pow(10, 6)
    return df
