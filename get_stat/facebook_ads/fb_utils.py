from facebook_business.adobjects.adaccount import AdAccount

def get_account_name(account_id: str) -> str:
    return AdAccount('act_' + account_id).api_get(fields=('name',)).get('name')