# sciagamy z gutenberga ksiazke z danym  ID i ladujemy do DF lub lub zapisujemy na dysku.
import requests
import pandas as pd
from pandas import DataFrame

from connection_cred import ConfigDB
from sqlalchemy import create_engine


# wykorzystać Bookszdaniacolumnnames i przetestować całego programu. Podac jakies id -> 3 ksiązki i bez powtórzeń
class TABLE_NAME:
    BOOKS_ZDANIA = 'books_zdania'


class BooksZdaniaColumnNames:
    id = 'id'
    id_gutenberg = 'id_gutenberg'
    zdania = 'zdania'


class BooksColumnNames:
    # wszystkie columny jakie ma ta tabela
    id_gutenberg = 'id_gutenberg'
    zdania = 'zdania'


name = 'Dracula'
id = 1  #


# id = get_book_id(name)
def load_from_gutenberg(id: int) -> DataFrame:
    print(id)  # 1000
    url = f"https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"
    rq = requests.get(url)
    text_mod = rq.text.replace("\r\n", '')
    zdania = text_mod.split(".")
    df = pd.DataFrame({'zdania': zdania})  # czas dodania do bazy
    df[BooksZdaniaColumnNames.id_gutenberg] = id
    return df


engine = create_engine(
    f'postgresql+psycopg2://{ConfigDB.username}:{ConfigDB.password}@{ConfigDB.host}:{ConfigDB.port}/{ConfigDB.database}')


def save_append_table(engine, df, table_name):
    data_added = df.to_sql(table_name, engine, if_exists='append', index=False)
    return data_added


# print(df.columns)
# data_added = save_append_table(engine, df[['id', 'zdania']], 'books_zdania')
# print(data_added)

def load_table_from_db(engine, table_name) -> DataFrame:
    df = pd.read_sql_table(table_name, engine)
    return df


def _filter_by_id(df, book_id):
    # df.query(f'id == {book_id}') # ten sam rezulta
    return df[df[BooksZdaniaColumnNames.id_gutenberg] == book_id]


def count_df(df):
    return df.count()


def check_if_id_is_in_the_table(df, book_id) -> bool:
    df = _filter_by_id(df, book_id)
    if df.empty:
        return False
    else:
        return True


def print_msg():
    print('Sorry, ale id jest w db')


def add_zdania_to_db(engine, book_id):
    df_books_zdania = load_table_from_db(engine, TABLE_NAME.BOOKS_ZDANIA)
    output_bool = check_if_id_is_in_the_table(df_books_zdania, book_id)  # Flase lub True
    if output_bool is True:  # if output_bool:
        print_msg()
    elif output_bool is False:  # if not output_bool: # przechodzi False i None
        book = load_from_gutenberg(id=book_id)  # id = 1000
        save_append_table(engine, book, TABLE_NAME.BOOKS_ZDANIA)


# zmiana na sqlalchemy

# 1 czy  my mamy ten id w bazie books_zdania?
# 2 jak nie to:
# odczytaj z gutenberga
# rozbicie na zdania
# zapis do bazy

# pobranie ksiazki lub odczytanie od razu z gunetnberga o danym id
# df => ksiazka z podzilaem na zdania (schema?) # # utworzyc klase ktora bedzie miala nazwy kolumn i moze typy
# dodanie ksiazki do db

id = 33
add_zdania_to_db(engine, id)

# 8,1000, 965,25

# next steps: ->

# sqlalchemy -> create schema -> increatmenta id for zdania and create table base on this class
