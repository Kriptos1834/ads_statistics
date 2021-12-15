from datetime import datetime
from typing import Iterable
from sqlalchemy.engine import Engine
from googleads.errors import AdWordsReportError

import pandas as pd
import json

class LogStatus:
    FAIL = 0
    SUCCESS = 1
    AWAITING = 2

class PgLogger:
    status = LogStatus()

    def __init__(self, app: str, date_from: datetime, date_to: datetime, fields: Iterable, token: str, engine: Engine, table_name: str = 'logs') -> None:
        self.app = app
        self.date_from = date_from
        self.date_to = date_to
        self.fields = fields
        self.token = token
        self.engine = engine
        self.table_name = table_name

    def write_log(self, status: int, request_url: str, request_params: str = None, code: int = None, message: str = None, json: dict = None) -> None:
        kwargs = locals()
        kwargs['service_name'] = self.app
        del kwargs['self']

        pd.DataFrame(kwargs).to_sql(self.table_name, self.engine)


def logfunction(func):
    def decorator(logger: PgLogger):
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except AdWordsReportError as e:
                logger.write_log(status=LogStatus.FAIL, request_url='https://google.com/adwords/api', message=)
            else:
                logger.write_log(status=LogStatus.SUCCESS, request_url='https://google.com/adwords/api', )