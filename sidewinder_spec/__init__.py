from datetime import datetime

unix_epoch = datetime.utcfromtimestamp(0)
epics_epoch = datetime.strptime("1/1/90 : 0:0:0.0", '%m/%d/%Y : %H:%M:%S.%f')


def time_from_epoch(dt, epoch=epics_epoch):
    return (dt - epoch).total_seconds()
