import pandas as pd
from pandas import DataFrame

from connection_cred import ConfigDB
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker


engine = create_engine(
    f'postgresql+psycopg2://{ConfigDB.username}:{ConfigDB.password}@{ConfigDB.host}:{ConfigDB.port}/{ConfigDB.database}')

Base = declarative_base()
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

class Statistics(Base):
    __tablename__ = 'statistics'
    id = Column(Integer, primary_key=True)
    zdania_id = Column(Integer) # relacja (FK)
    lenght = Column(Integer)

    def __repr__(self):
        return f"id = {self.id}, zdania_id = {self.zdania_id}, lenght = {self.lenght}"
    @classmethod
    def display_statistics_table(cls, session):
        print(f"----------------{cls.__tablename__}------------")
        for row in session.query(cls).all()[:100]:
            print(row)
        print("--------------------------------------")
def save_overwrite_table(df, table_name):
    df.to_sql(table_name, engine, if_exists='replace', index=False)

row = Statistics(zdania_id=10, lenght=100)
session.add(row)
session.commit()
print(session.query(Statistics).all())

def display_statistics_table():
    print("----------------STATISTICS------------")
    for row in session.query(Statistics).all()[:100]:
        print(row.lenght, row.zdania_id, row.id)
    print("--------------------------------------")


Statistics.display_statistics_table(session)
# delete from statistics where id = 1

to_remove = session.query(Statistics).filter(Statistics.id == 1).first()
if to_remove:
    session.delete(to_remove)
    session.commit()

to_remove = session.query(Statistics).filter(Statistics.id == 2).all()
for row in to_remove:
    row.lenght = 125
    session.commit()

Statistics.display_statistics_table(session)
#TODO
# zastanowic sie nad analiza i statyska
# w books_zdania przerzucić id na początek(lewo)
# stworzyć model books_zdania ( class BookZdania:     __tablename__ = 'books_zdania' i
# musisz dodac atrybuty ktore maja te same nazwy co kolumny na bazie i typy



