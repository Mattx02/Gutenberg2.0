import pandas as pd
from pandas import DataFrame

from connection_cred import ConfigDB
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

engine = create_engine(
    f'postgresql+psycopg2://{ConfigDB.username}:{ConfigDB.password}@{ConfigDB.host}:{ConfigDB.port}/{ConfigDB.database}')

Base = declarative_base()


class Statistics(Base):
    __tablename__ = 'statistics'
    id = Column(Integer, primary_key=True)
    zdania_id = Column(Integer) # relacja (FK)
    lenght = Column(Integer)


#TODO
# utworzyć tabele 'statistics' za sqlalchemy po uruchomieniu skryptu
# na podstawie tej klasy: Statistics dodaj jeden wiersz to tabeli  (obojetnie jakie dane)
# zastanowic sie nad analiza i statyska



