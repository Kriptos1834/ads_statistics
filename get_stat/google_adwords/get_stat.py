from googleads.adwords import AdWordsClient, ReportQueryBuilder
from typing import Iterable
from datetime import datetime, timedelta
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sys import exit, path
from ga_utils import convert_date
from config import RENAME_COLUMNS, VERSION

import pandas as pd
import traceback
import argparse

path.append('../')
from utils.utils import exclude_duplicates, date_validation



def get_report(client: AdWordsClient, report_type: str, columns: Iterable['str'], date_from: str = None, date_to: str = datetime.today()) -> pd.DataFrame:
    try:
        output = StringIO()
        date_range = convert_date(date_from) + ',' + convert_date(date_to) if date_from else None
        report_downloader = client.GetReportDownloader(version=VERSION)

        report_query = ReportQueryBuilder().Select(*columns).From(report_type)
        report_query = report_query.During(date_range).Build() if date_from else report_query.Build()

        report_downloader.DownloadReportWithAwql(
            report_query, 'CSV', output, skip_report_header=True,
            skip_column_header=False, skip_report_summary=True,
        )
        output.seek(0)

        df_report = pd.read_csv(output)

        return df_report

    except Exception as e:
        print('\n*** Function (get_campaign_report) Failed ***',
              traceback.format_exc())
        exit()


def get_campaign_performance_report(client: AdWordsClient) -> pd.DataFrame:
    df_report = get_report(client=client, report_type='CAMPAIGN_PERFORMANCE_REPORT',
                            columns=['CampaignId', 'CampaignName', 'AdvertisingChannelType', 'AdvertisingChannelSubType', 'CampaignStatus'])
    
    df_report.rename(columns=RENAME_COLUMNS, errors='ignore', inplace=True)
    df_report.replace({
        'enabled': 'active',
        'paused': 'stoped',
        'removed': 'archived',
    }, inplace=True)
    df_report['campaign_type'] = df_report['Advertising Channel'] + ':' + df_report['Advertising Sub Channel']
    df_report.drop(columns=['Advertising Channel', 'Advertising Sub Channel'], inplace=True)

    df_report['account_id'] = client.int_client_customer_id
    return df_report


def get_campaign_stat(client: AdWordsClient, date_from: datetime, date_to: datetime) -> pd.DataFrame:
    df_campaign_stat = get_report(client=client, report_type='CAMPAIGN_PERFORMANCE_REPORT',
                            columns=['CampaignId', 'Date', 'Impressions', 'Cost', 'Clicks', 'AccountCurrencyCode'],
                            date_from=date_from, date_to=date_to)

    df_campaign_stat.rename(columns=RENAME_COLUMNS, errors='ignore', inplace=True)
    df_campaign_stat['spends'] = df_campaign_stat['spends']/1000000
    return df_campaign_stat


def get_ad_performance_report(client: AdWordsClient, date_from: datetime, date_to: datetime) -> pd.DataFrame:
    df_report = get_report(client=client, report_type='AD_PERFORMANCE_REPORT',
                            columns=['Id', 'BusinessName', 'AdType', 'CreativeFinalUrls'],
                            date_from=date_from, date_to=date_to)
    df_report.rename(columns=RENAME_COLUMNS, errors='ignore', inplace=True)
    return df_report


def get_critetia_performance_report(client: AdWordsClient, date_from: datetime, date_to: datetime) -> pd.DataFrame:
    df_report = get_report(client=client, report_type='CRITERIA_PERFORMANCE_REPORT',
                            columns=['Id', 'Criteria', 'CriteriaType', 'CampaignId', 'FinalUrls'],
                            date_from=date_from, date_to=date_to)
    df_report.rename(columns=RENAME_COLUMNS, errors='ignore', inplace=True)
    return(df_report)

def main(client: AdWordsClient, date_from: datetime, date_to: datetime, engine: Engine) -> None:

    df_campaign_performance_report = get_campaign_performance_report(client=client)
    df_campaign_stat = exclude_duplicates(get_campaign_stat(client=client, date_from=date_from, date_to=date_to),
                            pd.read_sql_table('google_adwords_stat', con=engine))
    df_ad_performance_report = exclude_duplicates(get_ad_performance_report(client=client, date_from=date_from, date_to=date_to),
                            pd.read_sql_table('google_adwords_ads', con=engine))

    df_campaign_performance_report.to_sql('google_adwords_campaigns', engine, if_exists='append', index=False)
    df_campaign_stat.to_sql('google_adwords_stat', engine, if_exists='append', index=False)
    df_ad_performance_report.to_sql('google_adwords_ads', engine, if_exists='append', index=False)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--credentials', type=str, metavar='',
                        help='Path to .yaml file that contains credentials.', required=True)
    parser.add_argument('-df', '--date_from', type=str, metavar='',
                        help='Date collect stat from.', required=True)
    parser.add_argument('-dt', '--date_to', type=str, metavar='', help='Date collect stats to. Default: yesterday date',
                        default=datetime.strftime(datetime.today()-timedelta(days=1), '%Y-%m-%d'))

    parser.add_argument('-e', '--engine', type=str, metavar='', required=True,
                        help='Database engine string. Postgres engine string format: ' \
                            'postgresql+psycopg2://<pg user>:<password>@<host>:<port>/<base name>')

    # parser.add_argument('--pg_host', type=str, metavar='',
    #                     help='Postgres base host.', required=True)
    # parser.add_argument('--pg_port', type=str, metavar='',
    #                     help='Postgres base port.', required=True)
    # parser.add_argument('--db_name', type=str, metavar='',
    #                     help='Database name. Default: "ads_stats".', default='ads_stats')
    # parser.add_argument('--pg_user', type=str, metavar='',
    #                     help='Postgres user.', required=True)
    # parser.add_argument('--pg_password', type=str, metavar='',
    #                     help='Postgres password.', required=True)

    args = parser.parse_args()

    # Get engine and check it connection
    engine = create_engine(args.engine)
    with engine.connect() as connection:
        pass

    client = AdWordsClient.LoadFromStorage(args.credentials)
    client.int_client_customer_id = int(client.client_customer_id.replace('-', ''))
    
    main(client=client, date_from=date_validation(args.date_from),
        date_to=date_validation(args.date_to), engine=engine)
