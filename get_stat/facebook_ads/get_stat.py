from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from pandas.io.sql import read_sql_table
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from fb_utils import get_account_name
from config import REQUIRED_CREDENTIALS, API_VERSION

import pandas as pd
import argparse
import json

from sys import path
import os
path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', )))
from utils.utils import exclude_duplicates, date_validation

def get_stat_report(ad_account: AdAccount, date_from: str, date_to: str) -> pd.DataFrame:
    fields = ('campaign_id', 'ad_id', 'adset_id', 'impressions', 'clicks', 'spend', 'account_currency',)
    params = {'time_range': {'since': date_from, 'until': date_to}, 'time_increment': 1}

    rename_columns = {'date_start': 'dt', 'account_currency': 'currency', 'spend': 'spends'}
    data = []
    ads = ad_account.get_ads()
    for ad in ads:
        insights = ad.get_insights(fields=fields, params=params)
        data += list(map(lambda insight: insight.export_all_data(), insights))
    
    return pd.DataFrame(data).drop(columns=['date_stop']).rename(columns=rename_columns)

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
    df_stat = exclude_duplicates(
        df=get_stat_report(ad_account=ad_account, date_from=date_from, date_to=date_to),
        exclude=pd.read_sql_table('facebook_stat', engine))
    df_campaigns = exclude_duplicates(
        df=get_campaign_report(ad_account=ad_account),
        exclude=pd.read_sql_table('facebook_campaigns', engine))
    df_ads = exclude_duplicates(
        df=get_ad_report(ad_account=ad_account),
        exclude=pd.read_sql_table('facebook_ads', engine))
    
    df_stat.to_sql('facebook_stat', engine, if_exists='append', index=False)
    df_campaigns.to_sql('facebook_campaigns', engine, if_exists='append', index=False)
    df_ads.to_sql('facebook_ads', engine, if_exists='append', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

    credentials_file_schema = "\n\t".join([arg_name+" - "+desc for arg_name, desc in REQUIRED_CREDENTIALS.items()])
    parser.add_argument('-c', '--credentials', type=str, metavar='', required=True,
                        help='path to JSON file with Facebook Marketing API credentials.\nSchema:\n\t' +
                        credentials_file_schema)

    parser.add_argument('-db', '--database', type=str, metavar='', required=True,
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
    
    # Parse credentials JSON
    with open(args.credentials, 'r') as f:
        credentials = json.loads(f.read())

    credentials = {name: credentials.get(name) for name in REQUIRED_CREDENTIALS.keys()}

    # Check values
    if not all(credentials.values()):
        missing_args = [key for key, value in credentials.items() if not value]
        raise ValueError(f'credentials file is missing reqirement arguments: {", ".join(missing_args)}')
    
    # Get engine and check it connection
    with open(args.database, 'r') as f:
        database = json.loads(f.read())        
    try:
        engine = create_engine(f'postgresql+psycopg2://{database["user"]}:{database["password"]}@{database["host"]}:{database["port"]}/{database["base_name"]}')
    except KeyError:
        raise ValueError('databse config file is missing reqired arguments, use -h to show JSON schema')
    with engine.connect() as connection:
        pass

    # Get api
    FacebookAdsApi.init(**credentials, api_version=API_VERSION)
    ad_account = AdAccount(FacebookAdsApi.get_default_account_id())

    # Get date range
    date_from = date_validation(args.date_from).isoformat()
    date_to = date_validation(args.date_to).isoformat()

    main(ad_account=ad_account, date_from=date_from, date_to=date_to, engine=engine)