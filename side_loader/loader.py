from metadatastore.api import insert_event, insert_descriptor, insert_run_start, insert_run_stop
from uuid import uuid4
from filestore.api import insert_resource, insert_datum, register_handler
from filestore.handlers_base import HandlerBase


run_start_uid = insert_run_start(time=SPEC_START_TIME, scan_id=1,
                                 beamline_id='example',
                                 uid=str((uuid4()), **kwargs)

data_keys1 = {'I0': dict(source='IO', dtype='number'),
              'img': dict(source='det', dtype='array', shape=(2048, 2048), external='FILESTORE:')}
data_keys2 = {'T': dict(source='T', dtype='number'),}

descriptor1_uid = insert_event_descriptor(
    run_start=run_start_uid, data_keys=data_keys1, time=0.,
    uid=str(uuid4())

descriptor2_uid = insert_event_descriptor(
    run_start=run_start_uid, data_keys=data_keys2, time=0.,
    uid=str(uuid4())

# read in the temperature data up here
#temperature_data, time_data = read_temp_data()

# insert all the temperature data
for idx, (temp, t) in enumerate(zip(temperature_data, time_data)):
    insert_event(descriptor=descriptor2_uid, time=t, data={'T': temp}, uid=str(uuid4()),
                 timestamps={'T': t}, seq_num=idx)

# read in spec data
#img_names, I0, timestamps = read_spec_data()

for idx, (img_name, I, timestamp) in enumerate(zip(img_names, I0, timestamps)):
    fs_uid = uui4()
    resource = insert_resource('TIFF', img_name)
    insert_datum(resource, fs_uid)
    data = {'img': fs_uid, 'I0': I}
    timestamps = {'img': timestamp, 'I0': timestamp}
    insert_event(descriptor=descriptor1_uid, time=timestamp, data=data,
                 uid=str(uuid4()), timestamps=timestamps, seq_num=idx)


insert_run_stop(run_start=run_start_uid, time=np.max(timestamps), uid=str(uuid4()))

class TiffHandler(HandlerBase):
    specs = {'TIFF'} | HandlerBase.specs

    def __init__(self, name):
        self._name = name

    def __call__(self):
        return tifffile.read(self._name)




from databroker import db, get_events
register_handler('TIFF', TiffHandler)
hdr = db[-1]
events = get_events(hdr)
ev0 = next(events)
ev0['data']['img']  # array data



# databroker has two methods of accessing data
# this returns "headers" which are:
# header = {'start': RunStart, 'descriptors': [Descriptor1, Descriptor2], 'stop': RunStop}
#header.descriptors[0]
db[] # search for run_numbers, uids and slicing
db[10] # scan 10
db[-5] # 5 ago
db['10ssdf93'] # uid
db[5:15]
db[-10:-5]

db() # takes kwargs that should match top level keys in the RunStart
db(start_time='2016-01-01', stop_time='2016-02-01')