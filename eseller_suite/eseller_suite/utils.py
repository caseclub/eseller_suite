from datetime import datetime
from pytz import timezone

def foramt_date_time_to_ist(utc_time_str):
    # Parse the UTC time string
    utc_time = datetime.strptime(utc_time_str, '%Y-%m-%dT%H:%M:%SZ')

    # Convert to IST
    utc_zone = timezone('UTC')
    ist_zone = timezone('Asia/Kolkata')

    # Localize the UTC time
    utc_time = utc_zone.localize(utc_time)

    # Convert to IST
    ist_time = utc_time.astimezone(ist_zone)
    return ist_time.strftime('%Y-%m-%d %H:%M:%S')
