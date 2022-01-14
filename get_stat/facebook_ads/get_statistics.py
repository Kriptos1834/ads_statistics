from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from pandas.io.sql import read_sql_table
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from fb_utils import get_account_name

import pandas as pd
import argparse
import sys

sys.path.append('../')
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
    parser = argparse.ArgumentParser()

    parser.add_argument('-app', '--app_id', type=str, metavar='',
                        help='Facebook App Id.', required=True)
    parser.add_argument('-as', '--app_secret', type=str, metavar='',
                        help='Facebook App Secret.', required=True)
    parser.add_argument('-t', '--token', type=str, metavar='',
                        help='Facebook access token.', required=True)
    parser.add_argument('-acc', '--account', type=str, metavar='',
                        help='Facebook account id.', required=True)
                        
    parser.add_argument('-df', '--date_from', type=str, metavar='',
                        help='Date to collect statistic from.', required=True)
    parser.add_argument('-dt', '--date_to', type=str, metavar='', help='Date to collect statistics to. Default: today date',
                        default=datetime.strftime(datetime.today(), '%Y-%m-%d'))

    parser.add_argument('--pg_host', type=str, metavar='',
                        help='Postgres base host.', required=True)
    parser.add_argument('--pg_port', type=str, metavar='',
                        help='Postgres base port.', required=True)
    parser.add_argument('--db_name', type=str, metavar='',
                        help='Database name. Default: "ads_statistics".', default='ads_statistics')
    parser.add_argument('--pg_user', type=str, metavar='',
                        help='Postgres user.', required=True)
    parser.add_argument('--pg_password', type=str, metavar='',
                        help='Postgres password.', required=True)
    
    args = parser.parse_args()
    
    FacebookAdsApi.init(app_id=args.app_id, app_secret=args.app_secret, access_token=args.token)
    ad_account = AdAccount('act_' + args.account)

    date_from = date_validation(args.date_from).isoformat()
    date_to = date_validation(args.date_to).isoformat()
    engine = create_engine(f'postgresql+psycopg2://{args.pg_user}:{args.pg_password}@{args.pg_host}:{args.pg_port}/{args.db_name}')

    main(ad_account=ad_account, date_from=date_from, date_to=date_to, engine=engine)