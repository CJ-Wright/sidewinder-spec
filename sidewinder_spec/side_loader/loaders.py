from __future__ import print_function
import numpy as np
from uuid import uuid4
import os
from sidewinder_spec.utils.parsers import parse_spec_file, \
    parse_tif_metadata, parse_tif_metadata, parse_run_config
from metadatastore.api import insert_event, insert_run_start, insert_run_stop, \
    insert_descriptor
from filestore.api import insert_resource, insert_datum, register_handler
from filestore.api import db_connect as fs_db_connect
from metadatastore.api import db_connect as mds_db_connect
from databroker import db
import datetime
from collections import ChainMap
from pprint import pprint


def get_tiffs(run_folder, exempt=['test']):
    files = [f for f in os.listdir(run_folder) if all(exempt not in f)]
    # Load all the metadata files in the folder
    tiff_metadata_files = [os.path.join(run_folder, f) for f in
                           files
                           if f.endswith('.tif.metadata')]
    tiff_metadata_data = [parse_tif_metadata(f) for f in
                          tiff_metadata_files]

    # Sort the folder's data by time so we can have the start time
    timestamp_list = [f['timestamp'] for f in tiff_metadata_data]

    # read in all the image file name, using the metadata file name
    # TODO: This may need to change to be more robust
    tiff_file_names = [f[:-9] for f in tiff_metadata_files]

    # sort remaining data by time
    sorted_tiff_metadata_data = [x for (y, x) in sorted(
        zip(timestamp_list, tiff_metadata_data))]
    sorted_tiff_file_names = [x for (y, x) in sorted(
        zip(timestamp_list, tiff_file_names))]
    return sorted_tiff_metadata_data, sorted_tiff_file_names, timestamp_list


def load_run(spec, run_folder, beamline_config, dry_run=True,
             additional_data=None):
    """
    Load a single run into the databroker

    Parameters
    ----------
    spec: list of dict
        The dict which contains the spec log fil data
    run_folder: str
        The folder which contains the run configuration and image files
    beamline_config: dict
        The beamline configuration for this run
    dry_run: bool
        If True don't load the data but print what would have been loaded
    additional_data: list of dicts
        Any additional data to be added to the run not in the spec log

    Returns
    -------

    """
    # Load the run_config from the run folder
    run_config = parse_run_config(run_folder)

    md = ChainMap(beamline_config)
    md.update(run_config)

    # Associate the calibration data/background data
    for folder_kwarg, is_kwarg, md_name in zip(
            ['calibration_folders', 'background_folders' 'dark_folders'],
            ['is_calibration', 'is_background', 'is_dark'],
            ['calibration_uid', 'background_uid', 'dark_uid'],
    ):
        receive_list = []
        # If we have a cal/bg
        if folder_kwarg in run_config:
            # For each cal/bg
            for folder in run_config[folder_kwarg]:
                # if the folder is the run folder make a uuid
                if folder == run_folder:
                    receive_list.append(str(uuid4()))
                # We need to search for both the folder, and it's truth value
                else:
                    search_dict = {folder_kwarg: folder, is_kwarg: True}
                    hdrs = db(**search_dict)
                    # its in the DB use that hdr's cal/bg uuid
                    if len(hdrs) == 0:
                        # Load the data
                        md_uuid = load_run(spec, folder, beamline_config)
                        hdrs = db(**{md_name: md_uuid})
                    receive_list.extend([hdr[md_name] for hdr in hdrs])
        else:
            # THIS IS VERY BAD, except for cals which don't have backgrounds
            receive_list.append(None)
        md[md_name] = receive_list

    # stage the tiff files and metadata
    (sorted_tiff_metadata_data, sorted_tiff_file_names,
     timestamp_list) = get_tiffs(run_folder)

    ti = sorted_tiff_metadata_data[0]['time_from_date']

    # Get the section of spec we care about
    section_start_times = np.asarray(
        [run['start']['time'] for run in spec])
    spec_start_idx = np.argmin(np.abs(section_start_times - ti))
    spec_run = spec[spec_start_idx]

    # Put the header into MD
    md.update(spec_run['start'])
    md['run_id'] = spec_start_idx

    if dry_run:
        print('Run Header')
        pprint(md)
        run_start_uid = str(uuid4())
    else:
        run_start_uid = insert_run_start(**md)

    # load the images
    img_data_keys = {'pe1_img': dict(source='pe1_img', dtype='array',
                                     shape=(2048, 2048),
                                     external='FILESTORE:'),
                     'metadata': dict(source='metadata', dtype='dict')}
    img_descriptor_dict = dict(run_start=run_start_uid,
                               data_keys=img_data_keys,
                               time=0., uid=str(uuid4()))

    if dry_run:
        img_descriptor_uid = img_descriptor_dict['uid']
        print(img_descriptor_uid)
    else:
        img_descriptor_uid = insert_descriptor(**img_descriptor_dict)

    time_data = [s['timestamp'] for s in spec_run]
    for idx, (img_name, timestamp, metadata) in enumerate(
            zip(sorted_tiff_file_names, time_data,
                sorted_tiff_metadata_data)):
        fs_uid = str(uuid4())
        data = {'pe1_img': fs_uid, 'metadata': metadata}
        timestamps = {'img': timestamp, 'metadata': timestamp}
        event_dict = dict(descriptor=img_descriptor_uid, time=timestamp,
                          data=data,
                          uid=str(uuid4()), timestamps=timestamps, seq_num=idx,
                          )
        # print event_dict
        if not dry_run:
            resource = insert_resource('TIFF', img_name)
            insert_datum(resource, fs_uid)
            insert_event(**event_dict)

    for metadata_chunk in [spec_run['events'], additional_data]:
        for key in metadata_chunk[0].keys():
            if key not in ['timestamp', 'filename', 'file_name']:
                # TODO: need to change dtype based on python type; need map
                data_keys = {key: dict(source=key, dtype='number')}
                descriptor_dict = dict(run_start=run_start_uid,
                                       data_keys=data_keys,
                                       time=0., uid=str(uuid4()))
                for idx, scan in enumerate(metadata_chunk):
                    event_dict = dict(descriptor=descriptor_dict,
                                      time=scan['timestamp'],
                                      data={key: scan[key]},
                                      uid=str(uuid4()),
                                      timestamps={key: scan['timestamp']},
                                      seq_num=idx)
                    if not dry_run:
                        insert_event(**event_dict)

    if dry_run:
        print("Run Stop goes here")
    else:
        insert_run_stop(run_start=run_start_uid, time=np.max(time_data),
                        uid=str(uuid4()))
    return run_start_uid


def temp_dd_loader(run_folder, spec_data, section_start_times, run_kwargs,
                   dry_run=True):
    # Get the calibration/background header uid, if it exists or load it
    cal_hdrs = []
    background_hdrs = []
    for key in run_kwargs:
        try:
            cal_hdrs.append(
                general_loader(run_kwargs[key]['calibration_folder'],
                               spec_data, section_start_times, dry_run))
        # That part of the config file didn't have a calibration folder oh well
        except:
            pass
        try:
            background_hdrs.append(
                general_loader(run_kwargs[key]['background_folder'],
                               spec_data, section_start_times, dry_run))
        # That part of the config file didn't have a background folder oh well
        except KeyError:
            pass

    sorted_tiff_metadata_data, sorted_tiff_file_names, timestamp_list = get_tiffs(
        run_folder)

    # make subset of spec data for this run
    print(run_folder)
    ti = sorted_tiff_metadata_data[0]['time_from_date']

    # make a sub spec list which contains the spec section related to our data
    spec_start_idx = np.argmin(np.abs(section_start_times - ti))
    sub_spec = spec_data[spec_start_idx]
    assert len(cal_hdrs) > 0

    # 3. Create the run_start document.
    run_start_dict = dict(time=min(timestamp_list), scan_id=1,
                          beamline_id='11-ID-B',
                          uid=str(uuid4()),
                          is_calibration=False,
                          calibration=cal_hdrs,
                          background=background_hdrs,
                          run_type='temperature_dd',
                          run_folder=run_folder,
                          **run_kwargs)
    if dry_run:
        run_start_uid = run_start_dict['uid']
        print(run_start_dict)
    else:
        run_start_uid = insert_run_start(**run_start_dict)
        print(run_start_uid)

    data_keys1 = {'I0': dict(source='IO', dtype='number'),
                  'img': dict(source='det', dtype='array',
                              shape=(2048, 2048),
                              external='FILESTORE:'),
                  'detz': dict(source='detz', dtype='number'),
                  'metadata': dict(source='metadata', dtype='dict')}

    data_keys2 = {'T': dict(source='T', dtype='number'),}

    descriptor1_dict = dict(run_start=run_start_uid, data_keys=data_keys1,
                            time=0., uid=str(uuid4()))
    descriptor2_dict = dict(run_start=run_start_uid, data_keys=data_keys2,
                            time=0., uid=str(uuid4()))
    if dry_run:
        descriptor1_uid = descriptor1_dict['uid']
        descriptor2_uid = descriptor2_dict['uid']
        print(descriptor1_dict)
        print(descriptor2_dict)
    else:
        descriptor1_uid = insert_descriptor(**descriptor1_dict)
        descriptor2_uid = insert_descriptor(**descriptor2_dict)
        # print descriptor1_uid
        # print descriptor2_uid

    # insert all the temperature data
    temperature_data = [scan['T'] for scan in sub_spec]
    time_data = [scan['time_from_date'] for scan in sub_spec]

    for idx, (temp, t) in enumerate(zip(temperature_data, time_data)):
        event_dict = dict(descriptor=descriptor2_uid, time=t, data={'T': temp},
                          uid=str(uuid4()),
                          timestamps={'T': t}, seq_num=idx)

        # print event_dict
        if not dry_run:
            insert_event(**event_dict)

    # insert the images
    I0 = [scan['I00'] for scan in sub_spec]

    for idx, (img_name, I, timestamp, metadata) in enumerate(
            zip(sorted_tiff_file_names, I0, time_data,
                sorted_tiff_metadata_data)):
        fs_uid = str(uuid4())
        dz = float(os.path.split(os.path.splitext(img_name)[0])[-1][1:3])
        data = {'img': fs_uid, 'I0': I, 'detz': dz, 'metadata': metadata}
        timestamps = {'img': timestamp, 'I0': timestamp, 'detz': timestamp,
                      'metadata': timestamp}
        event_dict = dict(descriptor=descriptor1_uid, time=timestamp,
                          data=data,
                          uid=str(uuid4()), timestamps=timestamps, seq_num=idx,
                          )
        # print event_dict
        if not dry_run:
            resource = insert_resource('TIFF', img_name)
            insert_datum(resource, fs_uid)
            insert_event(**event_dict)

    if dry_run:
        print("Run Stop goes here")
    else:
        insert_run_stop(run_start=run_start_uid, time=np.max(timestamps),
                        uid=str(uuid4()))
    return run_start_uid


def dd_sample_changer_loader(run_folder, spec_data, section_start_times,
                             run_kwargs, dry_run=True):
    """
    Load the data from a sample changer run
    Note that this is tricky as it includes its own calibration

    Parameters
    ----------
    run_folder
    spec_data
    section_start_times
    run_kwargs
    dry_run

    Returns
    -------

    """
    # Get the calibration/background header uid, if it exists or load it
    cal_hdrs = []
    background_hdrs = []
    for key in run_kwargs:
        try:
            cal_hdrs.append(
                general_loader(run_kwargs[key]['calibration_folder'],
                               spec_data, section_start_times, dry_run))
        # That part of the config file didn't have a calibration folder oh well
        except:
            pass
        try:
            background_hdrs.append(
                general_loader(run_kwargs[key]['background_folder'],
                               spec_data, section_start_times, dry_run))
        # That part of the config file didn't have a background folder oh well
        except KeyError:
            pass

    sorted_tiff_metadata_data, sorted_tiff_file_names, timestamp_list = get_tiffs(
        run_folder)

    # make subset of spec data for this run
    print(run_folder)
    ti = sorted_tiff_metadata_data[0]['time_from_date']

    # make a sub spec list which contains the spec section related to our data
    spec_start_idx = np.argmin(np.abs(section_start_times - ti))
    sub_spec = spec_data[spec_start_idx]
    print(sub_spec)

    poni_files = [os.path.join(run_folder, f) for f in
                  os.listdir(run_folder) if f.endswith('.poni')]

    poni_uuids = []

    for f in poni_files:
        fs_uid = str(uuid4())
        if not dry_run:
            resource = insert_resource('pyFAI-geo', f)
            insert_datum(resource, fs_uid)
        poni_uuids.append(fs_uid)

    # 3. Create the run_start document.
    run_start_dict = dict(time=min(timestamp_list), scan_id=1,
                          beamline_id='11-ID-B',
                          uid=str(uuid4()),
                          is_calibration=False,
                          calibration=cal_hdrs,
                          background=background_hdrs,
                          run_type='sample changer dd',
                          run_folder=run_folder,
                          sample_names=run_kwargs['sample positions'],
                          poni=poni_uuids,
                          **run_kwargs)
    if dry_run:
        run_start_uid = run_start_dict['uid']
        print(run_start_dict)
    else:
        run_start_uid = insert_run_start(**run_start_dict)
        print(run_start_uid)

    data_keys1 = {'I0': dict(source='IO', dtype='number'),
                  'img': dict(source='det', dtype='array',
                              shape=(2048, 2048),
                              external='FILESTORE:'),
                  'detz': dict(source='detz', dtype='number'),
                  'metadata': dict(source='metadata', dtype='dict')}

    descriptor1_dict = dict(run_start=run_start_uid, data_keys=data_keys1,
                            time=0., uid=str(uuid4()))
    if dry_run:
        descriptor1_uid = descriptor1_dict['uid']
        print(descriptor1_dict)
    else:
        descriptor1_uid = insert_descriptor(**descriptor1_dict)

    time_data = [scan['time_from_date'] for scan in sub_spec]

    # insert the images
    I0 = [scan['I00'] for scan in sub_spec]

    for idx, (img_name, I, timestamp, metadata) in enumerate(
            zip(sorted_tiff_file_names, I0, time_data,
                sorted_tiff_metadata_data)):
        fs_uid = str(uuid4())
        dz = float(os.path.split(os.path.splitext(img_name)[0])[-1][1:3])
        data = {'img': fs_uid, 'I0': I, 'detz': dz, 'metadata': metadata}
        timestamps = {'img': timestamp, 'I0': timestamp, 'detz': timestamp,
                      'metadata': timestamp}
        event_dict = dict(descriptor=descriptor1_uid, time=timestamp,
                          data=data,
                          uid=str(uuid4()), timestamps=timestamps, seq_num=idx,
                          )
        print(img_name)
        print(event_dict)
        if not dry_run:
            resource = insert_resource('TIFF', img_name)
            insert_datum(resource, fs_uid)
            insert_event(**event_dict)

    if dry_run:
        print("Run Stop goes here")
    else:
        insert_run_stop(run_start=run_start_uid, time=np.max(time_data),
                        uid=str(uuid4()))
    return run_start_uid


def dd_cell_sample_changer_loader(run_folder, spec_data, section_start_times,
                                  run_kwargs, dry_run=True):
    # Get the calibration/background header uid, if it exists or load it
    cal_hdrs = []
    background_hdrs = []
    for key in run_kwargs:
        try:
            cal_hdrs.append(
                general_loader(run_kwargs[key]['calibration_folder'],
                               spec_data, section_start_times, dry_run))
        # That part of the config file didn't have a calibration folder oh well
        except:
            pass
        try:
            background_hdrs.append(
                general_loader(run_kwargs[key]['background_folder'],
                               spec_data, section_start_times, dry_run))
        # That part of the config file didn't have a background folder oh well
        except KeyError:
            pass

    sorted_tiff_metadata_data, sorted_tiff_file_names, timestamp_list = get_tiffs(
        run_folder)
    sn = [os.path.split(f)[-1].split('_')[0] for f in sorted_tiff_file_names]
    # make subset of spec data for this run
    print(run_folder)
    ti = sorted_tiff_metadata_data[0]['time_from_date']

    poni_files = [os.path.join(run_folder, f) for f in
                  os.listdir(run_folder) if f.endswith('.poni')]

    poni_uuids = []

    for f in poni_files:
        fs_uid = str(uuid4())
        if not dry_run:
            resource = insert_resource('pyFAI-geo', f)
            insert_datum(resource, fs_uid)
        poni_uuids.append(fs_uid)

    # make a sub spec list which contains the spec section related to our data

    spec_start_idx = np.argmin(np.abs(section_start_times - ti))
    sub_spec = spec_data[spec_start_idx]
    print(sorted_tiff_metadata_data[0]['filebase'], sub_spec[0]['stem'])
    print(
        'td:', datetime.datetime.utcfromtimestamp(ti),
        'ts:', datetime.datetime.utcfromtimestamp(
            sorted_tiff_metadata_data[0]['time']),
        'spec:', datetime.datetime.utcfromtimestamp(
            section_start_times[spec_start_idx]),
    )
    AAA

    # 3. Create the run_start document.
    run_start_dict = dict(time=min(timestamp_list), scan_id=1,
                          beamline_id='11-ID-B',
                          uid=str(uuid4()),
                          is_calibration=False,
                          calibration=cal_hdrs,
                          background=background_hdrs,
                          run_type='cell sample changer dd',
                          run_folder=run_folder,
                          sample_names=sn,
                          poni=poni_uuids,
                          **run_kwargs)
    if dry_run:
        run_start_uid = run_start_dict['uid']
        print(run_start_dict)
    else:
        run_start_uid = insert_run_start(**run_start_dict)
        print(run_start_uid)

    data_keys1 = {'I0': dict(source='IO', dtype='number'),
                  'img': dict(source='det', dtype='array',
                              shape=(2048, 2048),
                              external='FILESTORE:'),
                  'detz': dict(source='detz', dtype='number'),
                  'metadata': dict(source='metadata', dtype='dict')}

    descriptor1_dict = dict(run_start=run_start_uid, data_keys=data_keys1,
                            time=0., uid=str(uuid4()))
    if dry_run:
        descriptor1_uid = descriptor1_dict['uid']
        print(descriptor1_dict)
    else:
        descriptor1_uid = insert_descriptor(**descriptor1_dict)

    # TODO: Need stronger checks for spec to image. Check the file name and time
    time_data = [scan['time_from_date'] for scan in sub_spec]

    # insert the images
    I0 = [scan['I00'] for scan in sub_spec]

    spec_mask = []
    # for i in range(len(sub_spec)):
    #     if sub_spec[i]['stem'] in

    print(len(I0), len(sorted_tiff_file_names))
    print(sub_spec)
    print([a['stem'] for a in sub_spec])
    assert len(I0) == len(sorted_tiff_file_names)
    for idx, (img_name, I, timestamp, metadata) in enumerate(
            zip(sorted_tiff_file_names, I0, time_data,
                sorted_tiff_metadata_data)):
        fs_uid = str(uuid4())
        print(img_name)
        ddz = os.path.split(os.path.splitext(img_name)[0])[-1].split('d')[-1][
              :2]
        print(ddz)
        dz = float(ddz)
        data = {'img': fs_uid, 'I0': I, 'detz': dz, 'metadata': metadata}
        timestamps = {'img': timestamp, 'I0': timestamp, 'detz': timestamp,
                      'metadata': timestamp}
        event_dict = dict(descriptor=descriptor1_uid, time=timestamp,
                          data=data,
                          uid=str(uuid4()), timestamps=timestamps, seq_num=idx,
                          )
        print(img_name)
        print(event_dict)
        if not dry_run:
            resource = insert_resource('TIFF', img_name)
            insert_datum(resource, fs_uid)
            insert_event(**event_dict)

    if dry_run:
        print("Run Stop goes here")
    else:
        insert_run_stop(run_start=run_start_uid, time=np.max(time_data),
                        uid=str(uuid4()))
    return run_start_uid


def calibration_loader(run_folder, spec_data, section_start_times,
                       run_kwargs, dry_run=True):
    # Load all the metadata files in the folder
    sorted_tiff_metadata_data, sorted_tiff_file_names, timestamp_list = get_tiffs(
        run_folder)

    # make subset of spec data for this run
    ti = sorted_tiff_metadata_data[0]['time_from_date']

    # make a sub spec list which contains the spec section related to our data
    spec_start_idx = np.argmin(np.abs(section_start_times - ti))
    sub_spec = spec_data[spec_start_idx]

    poni_files = [os.path.join(run_folder, f) for f in
                  os.listdir(run_folder) if f.endswith('.poni')]
    poni_uuids = []

    for f in poni_files:
        fs_uid = str(uuid4())
        if not dry_run:
            resource = insert_resource('pyFAI-geo', f)
            insert_datum(resource, fs_uid)
        poni_uuids.append(fs_uid)

    # 3. Create the run_start document.
    # Note we need to associate any background run headers with this run header
    run_start_dict = dict(time=min(timestamp_list), scan_id=1,
                          beamline_id='11-ID-B',
                          group='Zhou',
                          owner='CJ-Wright',
                          project='PNO',
                          uid=str(uuid4()),
                          is_calibration=True,
                          poni=poni_uuids,  # Filestore save all the Poni files
                          run_folder=run_folder,
                          run_type='calibration',
                          **run_kwargs)
    if dry_run:
        run_start_uid = run_start_dict['uid']
        print(run_start_dict)
    else:
        run_start_uid = insert_run_start(**run_start_dict)
        print(run_start_uid)

    data_keys1 = {'I0': dict(source='IO', dtype='number'),
                  'img': dict(source='det', dtype='array',
                              shape=(2048, 2048),
                              external='FILESTORE:'),
                  'detz': dict(source='detz', dtype='number'),
                  'metadata': dict(source='metadata', dtype='dict')}

    descriptor1_dict = dict(run_start=run_start_uid, data_keys=data_keys1,
                            time=0., uid=str(uuid4()))
    if dry_run:
        descriptor1_uid = descriptor1_dict['uid']
        print(descriptor1_dict)
    else:
        descriptor1_uid = insert_descriptor(**descriptor1_dict)
        # print descriptor1_uid

    # insert all the temperature data
    time_data = [scan['time_from_date'] for scan in sub_spec]

    # insert the images
    I0 = [scan['I00'] for scan in sub_spec]

    for idx, (img_name, I, timestamp) in enumerate(
            zip(sorted_tiff_file_names, I0, time_data)):
        fs_uid = str(uuid4())
        dz = run_kwargs['general']['distance']
        data = {'img': fs_uid, 'I0': I, 'detz': dz}
        timestamps = {'img': timestamp, 'detz': timestamp, 'I0': timestamp,
                      'metadata': timestamp}
        event_dict = dict(descriptor=descriptor1_uid, time=timestamp,
                          data=data,
                          uid=str(uuid4()), timestamps=timestamps, seq_num=idx)
        # print event_dict
        if not dry_run:
            resource = insert_resource('TIFF', img_name)
            insert_datum(resource, fs_uid)
            insert_event(**event_dict)

    if dry_run:
        print("Run Stop goes here")
    else:
        insert_run_stop(run_start=run_start_uid, time=np.max(timestamps),
                        uid=str(uuid4()))
        print('Calibration inserted')
    return run_start_uid


run_loaders = {
    'temp_dd': temp_dd_loader,
    'dd_sample_changer': dd_sample_changer_loader,
    'calibration': calibration_loader,
    'dd_cell_sample_changer': dd_cell_sample_changer_loader,
}


def general_loader(run_folder, spec_data, section_start_times, dry_run=True):
    config_file = os.path.join(run_folder, 'config.txt')

    # To load a folder we need to make certain it has a config file
    # and is not already in the DB
    if os.path.exists(config_file) and db(run_folder=run_folder) == []:
        print('loading {}'.format(run_folder))
        run_config = parse_run_config(config_file)
        if run_config and 'general' in run_config.keys():
            loader = run_loaders[run_config['general']['loader_name']]
            uid = loader(run_folder, spec_data, section_start_times,
                         run_config, dry_run)
            print('finished loading {} uid={}'.format(run_folder, uid))
            return uid
    elif db(run_folder=run_folder):
        print('{} is already loaded'.format(run_folder))
        return db(run_folder=run_folder)[0]['start']['uid']
    else:
        print('Not valid run folder, not in DB and no valid config file')
        return None
