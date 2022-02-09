from asyncio import create_task
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, cursor

import os
import pandas as pd
import yaml


def date_validation(date_text: str):
    try:
        while date_text != datetime.strptime(date_text, '%Y-%m-%d').strftime('%Y-%m-%d'):
            Exception('*** Input Date does not match format yyyy-mm-dd ***')
        else:
            return datetime.strptime(date_text, '%Y-%m-%d').date()
    except:
        raise Exception('\n *** Function (date_validation) Failed ***')


def exclude_duplicates(df: pd.DataFrame, exclude: pd.DataFrame) -> pd.DataFrame:
    # convert columns data types
    for column in df.columns:
        df[column] = df[column].astype(exclude[column].dtypes.name)

    df_combined = pd.merge(df, exclude, on=list(
        df.columns), how='outer', indicator=True)
    return df_combined.loc[df_combined._merge == 'left_only'].drop(columns=['_merge', 'source', 'created_at'])


def get_pg_engine_from_env() -> Engine:
    engine = create_engine(
        f'postgresql+psycopg2://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}@{os.environ["DB_CONTAINER_NAME"]}/{os.environ["POSTGRES_DB"]}')
    return engine

def create_db_schema(cursor: cursor, sql_file: str):
    with open(sql_file, 'r', encoding='utf-8') as file:
        query = file.read()
        cursor.execute(query)
        cursor.execute('COMMIT')

def check_engine_connection(engine: Engine) -> None:
    connection = engine.raw_connection()
    with connection.cursor() as cursor:
        try:
            schema_file = os.environ['SCHEMA_FILE']
            create_db_schema(cursor, schema_file)
        except KeyError:
            pass
    connection.close()

def get_credentials_from_config(source_name: str) -> dict:
    with open(os.environ['CREDENTIALS'], 'r') as f:
        credentials = yaml.safe_load(f)[source_name]
    return credentials

# def exclude_duplicates(func):
#     def decorator(engine: Engine, table_name: str):
#         def wrapper(*args, **kwargs):
#             df = func(*args, **kwargs)
#             exclude = pd.read_sql_table(table_name, con=engine)
#             df_combined = pd.merge(df, exclude, on=list(df.columns), how='outer', indicator=True)

#             return df_combined.loc[df_combined._merge == 'left_only'].drop(columns=['_merge', 'source', 'created_at'])
#         return wrapper
#     return decorator
