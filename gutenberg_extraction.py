# sciagamy z gutenberga ksiazke z danym  ID i ladujemy do DF lub lub zapisujemy na dysku.
import requests
import pandas as pd
from pandas import DataFrame

from connection_cred import ConfigDB
from sqlalchemy import create_engine

name = 'Dracula'
# id = get_book_id(name)
id = 13635
url = f"https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"

rq = requests.get(url)
print(rq)
# rozbijamy na kroki:
# 1 zapisać do pliku< raw data
# 2 zapisać do df <- to robimy + raw data + some manipualtion
#
text_mod = rq.text.replace("\r\n", '')
zdania = text_mod.split(".")
df = pd.DataFrame({'zdania': zdania})  # czas dodania do bazy
df['id'] = id
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


def filter_by_id(df, book_id):
    # df.query(f'id == {book_id}') # ten sam rezulta
    return df[df['id'] == book_id]


def count_df(df):
    return df.count()


def check_if_id_is_in_the_table(df, book_id) -> bool:
    df = filter_by_id(df, book_id)
    if df.empty:
        return False
    else:
        return True


def print_msg():
    print('Sorry, ale id jest w db')


def add_zdania_to_db(engine, table_name, book_id):
    df = load_table_from_db(engine, table_name)
    output_bool = check_if_id_is_in_the_table(df, book_id)  # Flase lub True
    if output_bool is True:  # if output_bool:
        print_msg()
    elif output_bool is False:  # if not output_bool: # przechodzi False i None
        save_append_table(engine, df, table_name)


add_zdania_to_db(engine, 'books_zdania', id)

df = load_table_from_db(engine, 'books_zdania')
print(df)
