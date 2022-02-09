from facebook_business.adobjects.adaccount import AdAccount
from config import REQUIRED_CREDENTIALS

def get_account_name(account_id: str) -> str:
    return AdAccount('act_' + account_id).api_get(fields=('name',)).get('name')

def check_credentials(credentials: dict) -> None:
    credentials = {name: credentials.get(name) for name in REQUIRED_CREDENTIALS.keys()}
    if not all(credentials.values()):
        missing_args = [key for key, value in credentials.items() if not value]
        raise ValueError(f'credentials file is missing reqirement arguments: {", ".join(missing_args)}')