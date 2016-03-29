import numpy as np
from uuid import uuid4
import os
from sidewinder_spec.utils.parsers import parse_spec_file, \
    parse_tif_metadata, \
    parse_tif_metadata, parse_run_config


def temp_dd_loader(run_folder, spec_data, section_start_times, run_kwargs):
    # Load all the metadata files in the folder
    tiff_metadata_files = [os.path.join(run_folder, f) for f in
                           os.listdir(run_folder)
                           if f.endswith('.tif.metadata')]
    tiff_metadata_data = [parse_tif_metadata(f) for f in
                          tiff_metadata_files]

    # Sort the folder's data by time so we can have the start time
    timestamp_list = [f['timestamp'] for f in tiff_metadata_data]

    # read in all the remaining data namely image file names
    tiff_file_names = [f[:-9] for f in tiff_metadata_files]

    # sort remaining data by time
    sorted_tiff_metadata_data = [x for (y, x) in sorted(
        zip(timestamp_list, tiff_metadata_data))]

    sorted_tiff_file_names = [x for (y, x) in sorted(
        zip(timestamp_list, tiff_file_names))]

    # make subset of spec data for this run
    ti = sorted_tiff_metadata_data[0]['time_from_date']

    # make a sub spec list which contains the spec section related to our data
    spec_start_idx = np.argmin(np.abs(section_start_times - ti))
    sub_spec = spec_data[spec_start_idx]

    # 3. Create the run_start document.
    run_start_uid = dict(time=min(timestamp_list), scan_id=1,
                         beamline_id='11-ID-B',
                         uid=str(uuid4()), background=False,
                         calibration=False,
                         **run_kwargs)

    data_keys1 = {'I0': dict(source='IO', dtype='number'),
                  'img': dict(source='det', dtype='array',
                              shape=(2048, 2048),
                              external='FILESTORE:'),
                  'detz': dict(source='detz', dtype='number')}

    data_keys2 = {'T': dict(source='T', dtype='number'),}

    descriptor1_uid = dict(run_start=run_start_uid, data_keys=data_keys1,
                           time=0., uid=str(uuid4()))
    descriptor2_uid = dict(run_start=run_start_uid, data_keys=data_keys2,
                           time=0., uid=str(uuid4()))
    print descriptor1_uid
    print descriptor2_uid

    # insert all the temperature data
    temperature_data = [scan['T'] for scan in sub_spec]
    time_data = [scan['time_from_date'] for scan in sub_spec]

    for idx, (temp, t) in enumerate(zip(temperature_data, time_data)):
        print dict(descriptor=descriptor2_uid, time=t, data={'T': temp},
                   uid=str(uuid4()),
                   timestamps={'T': t}, seq_num=idx)

    # insert the images
    I0 = [scan['I00'] for scan in sub_spec]

    for idx, (img_name, I, timestamp) in enumerate(
            zip(sorted_tiff_file_names, I0, time_data)):
        fs_uid = uuid4()
        # resource = insert_resource('TIFF', img_name)
        # insert_datum(resource, fs_uid)
        dz = float(os.path.split(os.path.splitext(img_name)[0])[-1][1:3])
        data = {'img': fs_uid, 'I0': I, 'detz': dz}
        timestamps = {'img': timestamp, 'I0': timestamp}
        print dict(descriptor=descriptor1_uid, time=timestamp, data=data,
                   uid=str(uuid4()), timestamps=timestamps, seq_num=idx)
    print "Run Stop goes here"


def dd_sample_changer_loader(run_folder, spec_data, section_start_times, run_kwargs):
    pass

run_loaders = {
    'dd_temp':temp_dd_loader,
    'dd_sample_changer': dd_sample_changer_loader
}