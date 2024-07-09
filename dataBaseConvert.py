import datetime
from xmlrpc.client import DateTime
from numpy import genfromtxt
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String

sqlite_database = "sqlite+aiosqlite:///tasks.db"
engine = create_engine(sqlite_database, echo=True)
class Base(DeclarativeBase): pass


class House(Base):
    __tablename__ = "people"

    house_tkn = Column(Integer, primary_key=True, index=True)
    flat_tkn = Column(Integer)
    payment_period = Column(DateTime, default=datetime.datetime.utcnow)
    income = Column(float)
    debt = Column(float)
    raised = Column(float)
    volume_cold = Column(float)
    volume_hot = Column(float)
    volume_electr = Column(float)


Base.metadata.create_all(bind=engine)

session = sessionmaker()
session.configure(bind=engine)
s = session()
def Load_Data(file_name):
    data = genfromtxt(file_name, delimiter=';', skip_header=1, converters={0: lambda s: str(s)})
    return data.tolist()
if __name__ == "__main__":
    try:
        file_name = "raai_school_2024.csv" #sample CSV file used:  http://www.google.com/finance/historical?q=NYSE%3AT&ei=W4ikVam8LYWjmAGjhoHACw&output=csv
        data = Load_Data(file_name)

        for i in data:
            record = House(**{
                'house_tkn' : i[0],
                'flat_tkn' : i[1],
                'payment_period' : datetime.strptime(i[2], '%d-%b-%y').date(),
                'income' : i[3],
                'debt' : i[4],
                'raised' : i[5],
                'volume_cold': i[6],
                'volume_hot': i[7],
                'volume_electr': i[8],
            })
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records
    except:
        s.rollback() #Rollback the changes on error
    finally:
        s.close() #Close the connection

#попытка номер 1 (csv добавил по приколу) проверить

