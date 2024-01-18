#sciagamy z gutenberga ksiazke z danym  ID i ladujemy do DF lub lub zapisujemy na dysku.
import requests
import pandas as pd
from connection_cred import ConfigDB
from sqlalchemy import create_engine


name = 'Dracula'
# id = get_book_id(name)
id = 13635
url = f"https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"


rq = requests.get(url)
print(rq)
#rozbijamy na kroki:
#1 zapisać do pliku< raw data
#2 zapisać do df <- to robimy + raw data + some manipualtion
#
text_mod = rq.text.replace("\r\n", '')
zdania = text_mod.split(".")
df = pd.DataFrame({'zdania': zdania}) #czas dodania do bazy
df['id'] = id
engine = create_engine(f'postgresql+psycopg2://{ConfigDB.username}:{ConfigDB.password}@{ConfigDB.host}:{ConfigDB.port}/{ConfigDB.database}')
def save_append_table(engine, df, table_name):
    df.to_sql(table_name, engine, if_exists='append', index=False)


save_append_table(engine, df, 'books_zdania')
