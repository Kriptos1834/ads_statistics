from datetime import datetime

import pandas as pd


def date_validation(date_text: str):
    try:
        while date_text != datetime.strptime(date_text, '%Y-%m-%d').strftime('%Y-%m-%d'):
            Exception('*** Input Date does not match format yyyy-mm-dd ***')
        else:
            return datetime.strptime(date_text, '%Y-%m-%d').date()
    except:
        raise Exception('\n *** Function (date_validation) Failed ***')


def convert_date(date) -> str:
    try:
        date_iso = date.isoformat().replace("-", "")
        return date_iso
    except:
        raise Exception('adword_processing : date format conversion failed')


def exclude_duplicates(df: pd.DataFrame, exclude: pd.DataFrame) -> pd.DataFrame:
    df_combined = pd.merge(df, exclude, on=list(df.columns), how='outer', indicator=True)
    return df_combined.loc[df_combined._merge == 'left_only'].drop(columns=['_merge', 'source', 'created_at'])