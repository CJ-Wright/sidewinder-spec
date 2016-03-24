"""
The plan:
1. Load up all the SPEC metadata
2. Load each run_start configuration file, which we write currently in each dir
3. Create the run_start document which should include:
    [General]
    starttime
    [Distance 1]
    Distance
    File_Stem
    StartMask
    Configuration_img (fs_uuid?)
    Configuration_poni(fs_uuid?) (also write a pyFAI.geo handler)
    Background_img (File or folder if T dependent) (fs_uuid?)
    Background_chi (File or folder if T dependent) (fs_uuid?)
4. Make the data descriptors
5. Insert the data that goes with the image
5a. Match data with SPEC metadata
6. Insert runstop
7. Repeat for all data sets

"""
from uuid import uuid4

import numpy as np
from filestore.api import insert_resource, insert_datum, register_handler
from metadatastore.api import insert_event, insert_run_start, insert_run_stop, \
    insert_descriptor

if __name__ == '__main__':
    import os
    from utils.parsers import parse_spec_file, parse_tif_metadata

    # 1. Load up all the SPEC metadata
    spec_file_loc = ''
    spec_Data = parse_spec_file(spec_file_loc)

    # 2. Load each run_start configuration file, which we write currently in each dir
    insert_folder = ''
    config_file = os.path.join(insert_folder, 'config.txt')

    kwargs = 'GIANT BLOB FROM CONFIG FILE'

    # Load all the metadata files in the folder
    tiff_metadata_files = [os.path.join(insert_folder, f) for f in os.listdir()
                           if
                           f.endswith('.tif.metadata')]
    tiff_metadata_data = [parse_tif_metadata(f) for f in tiff_metadata_files]

    # Sort the folder's data by time so we can have the start time
    timestamp_list = [f['timestamp'] for f in tiff_metadata_data]

    # 3. Create the run_start document.
    run_start_uid = insert_run_start(time=min(timestamp_list), scan_id=1,
                                     beamline_id='11-ID-B',
                                     group='Zhou',
                                     project='PNO',
                                     uid=str((uuid4()), **kwargs))

    data_keys1 = {'I0': dict(source='IO', dtype='number'),
                  'img': dict(source='det', dtype='array', shape=(2048, 2048),
                              external='FILESTORE:'),
                  'detz': dict(source='detz', dtype='number')}

    data_keys2 = {'T': dict(source='T', dtype='number'),}

    descriptor1_uid = insert_descriptor(
        run_start=run_start_uid, data_keys=data_keys1, time=0.,
        uid=str(uuid4()))

    descriptor2_uid = insert_descriptor(
        run_start=run_start_uid, data_keys=data_keys2, time=0.,
        uid=str(uuid4()))

    # read in all the remaining data namely image file names
    tiff_file_names = [f[:-9] for f in tiff_metadata_files]

    # sort remaining data by time
    sorted_tiff_metadata_data = [x for (y, x) in sorted(
        zip(timestamp_list, tiff_metadata_data))]

    sorted_tiff_file_names = [x for (y, x) in sorted(
        zip(timestamp_list, tiff_file_names))]

    # make subset of spec data for this run
    ti = sorted_tiff_metadata_data[0]['time_from_date']

    # insert all the temperature data
    temperature_data = []
    for idx, (temp, t) in enumerate(zip(temperature_data, time_data)):
        insert_event(descriptor=descriptor2_uid, time=t, data={'T': temp},
                     uid=str(uuid4()),
                     timestamps={'T': t}, seq_num=idx)

    # read in spec data
    # img_names, I0, timestamps = read_spec_data()

    for idx, (img_name, I, timestamp) in enumerate(
            zip(img_names, I0, timestamps)):
        fs_uid = uui4()
        resource = insert_resource('TIFF', img_name)
        insert_datum(resource, fs_uid)
        data = {'img': fs_uid, 'I0': I}
        timestamps = {'img': timestamp, 'I0': timestamp}
        insert_event(descriptor=descriptor1_uid, time=timestamp, data=data,
                     uid=str(uuid4()), timestamps=timestamps, seq_num=idx)

    insert_run_stop(run_start=run_start_uid, time=np.max(timestamps),
                    uid=str(uuid4()))

    from databroker import db, get_events

    hdr = db[-1]
    events = get_events(hdr)
    ev0 = next(events)
    ev0['data']['img']  # array data

    # databroker has two methods of accessing data
    # this returns "headers" which are:
    # header = {'start': RunStart, 'descriptors': [Descriptor1, Descriptor2], 'stop': RunStop}
    # header.descriptors[0]
    # db[] # search for run_numbers, uids and slicing
    db[10]  # scan 10
    db[-5]  # 5 ago
    db['10ssdf93']  # uid
    db[5:15]
    db[-10:-5]

    db()  # takes kwargs that should match top level keys in the RunStart
    db(start_time='2016-01-01', stop_time='2016-02-01')
