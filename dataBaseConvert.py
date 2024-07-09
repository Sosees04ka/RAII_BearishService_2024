import datetime
from numpy import genfromtxt
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Float, Column, Integer, String
from processing import csv_to_unixtime_df  # Assuming this function exists in processing module

# Use the regular SQLite driver instead of aiosqlite
sqlite_database = "sqlite:///tasks.db"
engine = create_engine(sqlite_database, echo=True)

Base = declarative_base()


class House(Base):
    __tablename__ = "people"

    house_tkn = Column(Integer, primary_key=True, index=True)
    flat_tkn = Column(Integer)
    unix_payment_period = Column(Integer)
    income = Column(Float)
    debt = Column(Float)
    raised = Column(Float)
    volume_cold = Column(Float)
    volume_hot = Column(Float)
    volume_electr = Column(Float)


Base.metadata.create_all(bind=engine)

Session = sessionmaker(bind=engine)
session = Session()


def load_data(filename):
    return csv_to_unixtime_df(filename)


if __name__ == "__main__":
    try:
        filename = "raai_school_2024.csv"  # Ensure the CSV file is in the correct format
        data = load_data(filename)

        for index, row in data.iterrows():
            record = House(
                house_tkn=row["house_tkn"],
                flat_tkn=row["flat_tkn"],
                unix_payment_period=row["unix_payment_period"],
                income=row["income"],
                debt=row["debt"],
                raised=row["raised"],
                volume_cold=row["volume_cold"],
                volume_hot=row["volume_hot"],
                volume_electr=row["volume_electr"],
            )
            session.add(record)  # Add all the records

        session.commit()  # Attempt to commit all the records
    except Exception as e:
        session.rollback()  # Rollback the changes on error
        print(f"Error occurred: {e}")
    finally:
        records = session.query(House).all()
        for row in records:
            print("House Token:", row.house_tkn)
            print("Flat Token:", row.flat_tkn)
            print("Payment Period:", row.unix_payment_period)
            print("Income:", row.income)
            print("Debt:", row.debt)
            print("Raised:", row.raised)
            print("Volume Cold:", row.volume_cold)
            print("Volume Hot:", row.volume_hot)
            print("Volume Electr:", row.volume_electr)
            print("\n")
        session.close()  # Close the connection
