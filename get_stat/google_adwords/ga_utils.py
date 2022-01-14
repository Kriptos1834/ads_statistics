def convert_date(date) -> str:
    try:
        date_iso = date.isoformat().replace("-", "")
        return date_iso
    except:
        raise Exception('adword_processing : date format conversion failed')
