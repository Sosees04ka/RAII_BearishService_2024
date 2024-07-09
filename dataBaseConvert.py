from processing import csv_to_unixtime_df
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
    (id, house_tkn, flat_tkn, unix_payment_period, income, debt, raised, volume_cold, volume_hot, volume_electr) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

select_sql = "SELECT * FROM House LIMIT 10;"

get_count_sql = "SELECT COUNT(*) FROM House;"

if __name__ == "__main__":
    try:
        cursor.execute(drop_sql)
        connection.commit()
        cursor.execute(table_sql)
        connection.commit()

        filename = "raai_school_2024.csv"  # Ensure the CSV file is in the correct format
        data = csv_to_unixtime_df(filename)

        for index, row in data.iterrows():
            record = (index, *row)

            cursor.execute(insert_sql, record)
            if index % 10_000 == 0:
                print(f"Commited {index}!")
                connection.commit()

    except Exception as e:
        connection.rollback()
        print(f"Error occurred: {e}")
    finally:
        cursor.execute(get_count_sql)
        records = cursor.fetchall()

        print(records)

        connection.close()
