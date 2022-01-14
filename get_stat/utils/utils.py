from datetime import datetime
from pandas.core import api
from sqlalchemy.engine import Engine

import pandas as pd


def date_validation(date_text: str):
    try:
        while date_text != datetime.strptime(date_text, '%Y-%m-%d').strftime('%Y-%m-%d'):
            Exception('*** Input Date does not match format yyyy-mm-dd ***')
        else:
            return datetime.strptime(date_text, '%Y-%m-%d').date()
    except:
        raise Exception('\n *** Function (date_validation) Failed ***')

def exclude_duplicates(df: pd.DataFrame, exclude: pd.DataFrame) -> pd.DataFrame:
    df_combined = pd.merge(df, exclude, on=list(
        df.columns), how='outer', indicator=True)
    return df_combined.loc[df_combined._merge == 'left_only'].drop(columns=['_merge', 'source', 'created_at'])


# def exclude_duplicates(func):
#     def decorator(engine: Engine, table_name: str):
#         def wrapper(*args, **kwargs):
#             df = func(*args, **kwargs)
#             exclude = pd.read_sql_table(table_name, con=engine)
#             df_combined = pd.merge(df, exclude, on=list(df.columns), how='outer', indicator=True)

#             return df_combined.loc[df_combined._merge == 'left_only'].drop(columns=['_merge', 'source', 'created_at'])
#         return wrapper
#     return decorator