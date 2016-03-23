from datetime import datetime

unix_epoch = datetime.utcfromtimestamp(0)
epics_epoch = "1/1/90"

def time_from_epoch(dt):
    return (dt - epoch).total_seconds()
