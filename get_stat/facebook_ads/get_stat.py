from datetime import datetime, timedelta
from tabnanny import check
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from pandas.io.sql import read_sql_table
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from fb_utils import get_account_name, check_credentials
from alive_progress import alive_bar
from config import REQUIRED_CREDENTIALS, API_VERSION, SOURCE

import pandas as pd
import argparse
import json

from sys import path
import os
path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', )))
from utils.utils import exclude_duplicates, date_validation, get_pg_engine_from_env, check_engine_connection, get_credentials_from_config

def get_stat_report(ad_account: AdAccount, date_from: str, date_to: str) -> pd.DataFrame:
    fields = ('campaign_id', 'ad_id', 'adset_id', 'impressions', 'clicks', 'spend', 'account_currency',)
    params = {'time_range': {'since': date_from, 'until': date_to}, 'time_increment': 1}

    rename_columns = {'date_start': 'dt', 'account_currency': 'currency', 'spend': 'spends'}
    data = []
    ads = ad_account.get_ads()
    for ad in ads:
        insights = ad.get_insights(fields=fields, params=params)
        data += list(map(lambda insight: insight.export_all_data(), insights))
    if len(data):
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame(columns=fields)
    return df.drop(columns=['date_stop'], errors='ignore').rename(columns=rename_columns)

def get_campaign_report(ad_account: AdAccount) -> pd.DataFrame:
    fields = ('id', 'name', 'smart_promotion_type', 'status', 'account_id')
    rename_columns = {'id': 'campaign_id', 'name': 'campaign_name', 'smart_promotion_type': 'campaign_type', 'status': 'campaign_status'}

    campaigns = ad_account.get_campaigns(fields=fields)
    data = list(map(lambda campaign: campaign.export_all_data(), campaigns))
    for row in data:
        row['account_name'] = get_account_name(row['account_id'])

    df_report = pd.DataFrame(data).rename(columns=rename_columns)
    df_report.rename({
        'ACTIVE': 'active',
        'PAUSED': 'stoped',
        'DELETED': 'deleted',
        'ARCHIVED': 'archived',
    }, inplace=True)
    return pd.DataFrame(data).rename(columns=rename_columns)

def get_ad_report(ad_account: AdAccount) -> pd.DataFrame:
    fields = ('id', 'name', 'campaign_id',)
    rename_columns = {'id': 'ad_id', 'name': 'ad_name'}

    ads = ad_account.get_ads(fields=fields)
    data = list(map(lambda ad: ad.export_all_data(), ads))

    return pd.DataFrame(data).rename(columns=rename_columns)



def main(ad_account: AdAccount, date_from: str, date_to: str, engine: Engine) -> None:
    print(f'Loading facebook margeting stat ({date_from} - {date_to}).', end=' ')
    with alive_bar(6) as pbar:    
        df_stat = exclude_duplicates(
            df=get_stat_report(ad_account=ad_account, date_from=date_from, date_to=date_to),
            exclude=pd.read_sql_table(f'{SOURCE}_stat', engine))
        pbar()
        df_campaigns = exclude_duplicates(
            df=get_campaign_report(ad_account=ad_account),
            exclude=pd.read_sql_table(f'{SOURCE}_campaigns', engine))
        pbar()
        df_ads = exclude_duplicates(
            df=get_ad_report(ad_account=ad_account),
            exclude=pd.read_sql_table(f'{SOURCE}_ads', engine))
        pbar()
        df_stat.to_sql(f'{SOURCE}_stat', engine, if_exists='append', index=False)
        pbar()
        df_campaigns.to_sql(f'{SOURCE}_campaigns', engine, if_exists='append', index=False)
        pbar()
        df_ads.to_sql(f'{SOURCE}_ads', engine, if_exists='append', index=False)
        pbar()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

    credentials_file_schema = "\n\t".join([arg_name+" - "+desc for arg_name, desc in REQUIRED_CREDENTIALS.items()])
    parser.add_argument('-c', '--credentials', type=str, metavar='',
                        help='path to JSON file with Facebook Marketing API credentials.\nSchema:\n\t' +
                        credentials_file_schema)

    parser.add_argument('-db', '--database', type=str, metavar='',
                        help='path to JSON file with base config.\nSchema:' +
                        '\nhost - <string> base host.' +
                        '\nport - <string> base port.' +
                        '\nbase_name - <string> base name.' +
                        '\nuser - <string> base user.' + 
                        '\nuser - <string> user password.')

    parser.add_argument('-df', '--date_from', type=str, metavar='',
                        help='Date to collect statistic from.', required=True)
    parser.add_argument('-dt', '--date_to', type=str, metavar='', help='Date to collect statistics to. Default: yesterday date',
                        default=datetime.strftime(datetime.today()-timedelta(days=1), '%Y-%m-%d'))
    
    args = parser.parse_args()
    
    # Parse credentials
    if args.credentials:
        with open(args.credentials, 'r') as f:
            credentials = json.loads(f.read())
    else:
        try:
            credentials = get_credentials_from_config(SOURCE)
        except KeyError:
            raise ValueError('--credentials was not specified')

    # Check values
    check_credentials(credentials)
    
    # Get engine
    if args.database:
        with open(args.database, 'r') as f:
            database = json.loads(f.read())        
        try:
            engine = create_engine(f'postgresql+psycopg2://{database["user"]}:{database["password"]}@{database["host"]}:{database["port"]}/{database["base_name"]}')
        except KeyError:
            raise ValueError('databse config file is missing reqired arguments, use -h to show JSON schema')
    else:
        try:
            engine = get_pg_engine_from_env()
        except KeyError:
            raise ValueError('--database was not specified')
            
    # Check engine connection
    check_engine_connection(engine)
    
    # Get api
    FacebookAdsApi.init(**credentials, api_version=API_VERSION)
    ad_account = AdAccount(FacebookAdsApi.get_default_account_id())

    # Get date range
    date_from = date_validation(args.date_from).isoformat()
    date_to = date_validation(args.date_to).isoformat()

    main(ad_account=ad_account, date_from=date_from, date_to=date_to, engine=engine)