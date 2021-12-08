from datetime import datetime
from typing import Iterable
from sqlalchemy.engine import Engine

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

    def write_log(self, request_params: str, status: int, request_url: str, code: int, message: str, service_name: str, json: dict) -> None:
        kwargs = locals()
        del kwargs['self']
        pd.DataFrame(kwargs).to_sql(self.table_name, self.engine)
        