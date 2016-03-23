from datetime import datetime

epoch = datetime.utcfromtimestamp(0)


def time_from_epoch(dt):
    return (dt - epoch).total_seconds()
