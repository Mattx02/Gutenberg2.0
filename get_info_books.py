import pandas
from bs4 import BeautifulSoup
import requests
import pandas as pd
from pandas import DataFrame
import psycopg2
from psycopg2 import connect
import sqlalchemy
from sqlalchemy import create_engine
from connection_cred import ConfigDB

link_csv = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv"
path_csv = "data/pg_catalog.csv" # relative path
# path_csv = f"C:\Users\macie\OneDrive\Pulpit\projekty_python\data\pg_catalog.csv" # abolute
# df = pd.read_csv(path_csv)



# TODO
# zrobić webscraping pierwszej książki/metainformacje
# użyć beautifulsoup
# używać xpatha
# zrobić klase, aby miała atrybuty wszystkie
# przy inicjalizacji





headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }



# url = "https://www.gutenberg.org/ebooks/72347"
# req = requests.get(url, headers)
# soup = BeautifulSoup(req.content, 'html.parser')
# print(soup.prettify())


# TODO
# tabela gatunkow #
# tabela ksiazki # csv
# tabela wiele ksiazek wiele gatunkow

# df['Bookshelves_list'] = df['Bookshelves'].str.split(';')
# df_exploded = df['Bookshelves_list'].explode()
# wiele gatunkow -> unique zeby storzyc tabele gatunkow. unique(lista) -> liste id (musi byc wielkosci liscie gatunkow) ->  nowy df_gatunki ktory ma columny id, gatunek
# df -> te kolumny -> id_ksiazki, id_gatunku # df['Bookshelves_list'].explode() wyczyscic NAN ale nie koniecznie
#

# >>> pd.unique(df['col_name']))
# array([2, 1, 3])
def read_csv(path_csv: str) -> DataFrame:
    df = pd.read_csv(path_csv)
    return df

def get_genre(df: DataFrame) -> DataFrame:
    # id genre
    df['Bookshelves_new'] = df['Bookshelves'].str.split(';')
    df_exploded = df['Bookshelves_new'].explode()
    df_new = df_exploded.astype('str')
    df_new = df_new.apply(lambda x: x.strip())
    return pd.DataFrame({'genre_name': pd.unique(df_new)})
def get_books(df: DataFrame) -> DataFrame:
    # id, 'Text#'-> gutenberg_id, 'Type', 'Issued', 'Title', 'Language', 'Authors', 'Subjects'
    df = df[['Text#', 'Type', 'Issued', 'Title', 'Language', 'Authors', 'Subjects', 'Bookshelves']] # sql -> Bookshelves czy string czy lista ok

    df = df.rename(columns={'Text#': 'id_gutenberg'})
    return df

# >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
# >>> df.rename(columns={"A": "a", "B": "c"})
#    a  c
# 0  1  4
# 1  2  5
# 2  3  6
def get_book_genre(df: DataFrame) -> DataFrame:
    # id, id_books, id_genre # wiele do wielu ->
    df['Bookshelves_new'] = df['Bookshelves'].str.split(';')
    df_exploded = df[['id_gutenberg', 'Bookshelves_new']].explode('Bookshelves_new')
    df_exploded['Bookshelves_new'] = df_exploded['Bookshelves_new'].astype('str')
    df_exploded['Bookshelves_new'] = df_exploded['Bookshelves_new'].apply(lambda x: x.strip())
    return df_exploded


df = read_csv(path_csv)
df_books = get_books(df)
df_genre = get_genre(df)
df_book_genre = get_book_genre(df_books)
print(df)

# odczytanie ze strony ksiazki o danym id
# zapisanie lokalnie w data/books optional (*pdf,txt)
# zapisanie na bazie: przygotwanie df: podzielic ksiazke na zdania i dodanie id do columny
#

class Tables:
    GENRE = 'Genre'
    BOOK = 'book'
    BOOK_GENRE = 'book_genre'



engine = create_engine(f'postgresql+psycopg2://{ConfigDB.username}:{ConfigDB.password}@{ConfigDB.host}:{ConfigDB.port}/{ConfigDB.database}')


def save_overwrite_table(df, table_name):
    df.to_sql(table_name, engine, if_exists='replace', index=False)

save_overwrite_table(df_genre, Tables.GENRE)
save_overwrite_table(df_book_genre, Tables.BOOK_GENRE)
save_overwrite_table(df_books, Tables.BOOK)

con = connect(
    database=ConfigDB.database,
    user=ConfigDB.username,
    password=ConfigDB.password,
    host=ConfigDB.host,
    port=ConfigDB.port,
)
cursor = con.cursor()

#być moze użycie selenium do sprawdzenia zczytywania
#class Book:
    #def __init__(self, url):
        # self.url = url
        # self.author = self.get_author()
    #def get_author(self):
        #pass


#book1 = Book(url)
