from datetime import datetime, date as dt
import hmac
import hashlib
import urllib.parse
from typing import Optional


def create_timestamp(date: Optional[str] = None, time: Optional[str] = None):
    '''
    Returns UNIX timestamp (miliseconds) from parameters:
    date: "%d/%m/%Y" (optional)
    time: "%H:%M"    (optional)
    If parameters not set, returns current time timestamp.
    '''
    if date and time:
        date_time = datetime.strptime(f"{date}, {time}", "%d/%m/%Y, %H:%M")
    elif date and not time:
        date_time = datetime.strptime(f"{date}", "%d/%m/%Y")
    elif not date and time:
        today = dt.today().strftime("%d/%m/%Y")
        date_time = datetime.strptime(f"{today}, {time}", "%d/%m/%Y, %H:%M")
    elif not date and not time:
        date_time = datetime.now()
    
    return round(datetime.timestamp(date_time) * 1000)


def create_signature(secret_key, payload):

    url_string = urllib.parse.urlencode(payload)    
    return hmac.new(secret_key.encode("utf-8"), url_string.encode("utf-8"), 
                    hashlib.sha256).hexdigest()

