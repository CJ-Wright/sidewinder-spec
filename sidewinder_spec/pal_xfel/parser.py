import h5py
from bluesky.utils import new_uid
import numpy as np
from shed.savers import NpyWriter

from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
import time
from pprint import pprint


def format_keys(*args):
    return '/'.join(args)


key_data_map = {'RayMX': 'image', 'photodiode': 'I0', 'eh1_qbpm1': 'eh1_qbpm1',
                'oh_qbpm2': 'oh_qbpm2'}

ai = AzimuthalIntegrator(wavelength=(12.398 / 9.70803 * 1.0e-10))
ai.setFit2D(60.550, 1440.364, (2880 - -26.378), tilt=(-0.024),
            tiltPlanRotation=54.442, pixelX=78, pixelY=78)
calib_config_dict = dict(ai.getPyFAI())


def parse_hdf5(fn):
    f = h5py.File(fn, 'r')
    shot_number = 0
    suid = new_uid()
    # loop through the scans
    for scans in f.keys():
        # Create start doc
        start_doc = {
            'uid': suid,
            'shot_number': shot_number,
            'i0thresh': 2.e9,
            'i0amp': 1.e10,
            'calibration_md': calib_config_dict,
            'time': time.time(),
            'sideloaded': True,
            'composition_string': 'Au',
            'experimenters': ['Tadesse', 'Assefa', 'Jane', 'Doe', 'Richard',
                              'Roe'],
            'bt_wavelength': (12.398 / 9.70803 * 1.0e-10)
        }
        yield 'start', start_doc
        # Create most of the descriptor
        duid = new_uid()
        descriptor_doc = {'uid': duid,
                          'name': 'primary',
                          'run_start': suid,
                          'data_keys': {'delay_timestamp': {
                              'source': 'delay_stage', 'dtype': 'float',
                              'shape': [], 'unit': 'ps'},
                              'shot_number': {
                                  'source': 'sidewinder', 'dtype': 'int',
                                  'shape': [], 'unit': 'NA'}},
                          'time': time.time()}
        yielded_descriptor = False

        # loop through the delay points
        for i, delay_point in enumerate(f[scans].keys()):
            events = []

            timestamps = list(
                f[format_keys(scans, delay_point, 'RayMX')].keys())

            # loop through the shots per delay point
            for shot_number, timestamp in enumerate(timestamps):
                ts = timestamp.strip('timestamp_')
                event = {'uid': new_uid(),
                         'descriptor': duid,
                         'filled': {'image': True},
                         'data': {'shot_number': shot_number},
                         'timestamps': {'shot_number': ts},
                         'seq_num': i,
                         'time': time.time(),
                         }
                # loop through the detectors
                for data_source in [ff for ff in
                                    f[format_keys(scans, delay_point)].keys()
                                    if
                                    ff in ['RayMX', 'photodiode']]:

                    data_key = format_keys(scans, delay_point, data_source,
                                           timestamp)
                    v = f[data_key].value

                    # fill in missing descriptor info
                    if isinstance(v, np.ndarray):
                        s = v.shape
                        if s == (1,):
                            v = float(v)
                            s = []
                        else:
                            v = v.astype('float32')
                    else:
                        s = []

                    event['data'][key_data_map[data_source]] = v
                    event['timestamps'][key_data_map[data_source]] = int(ts)

                    if not yielded_descriptor:
                        dtype = str(getattr(v, 'dtype', type(v)))
                        descriptor_doc['data_keys'][
                            key_data_map[data_source]] = {
                            'source': data_source,
                            'dtype': dtype,
                            'shape': s}
                events.append(event)

            if not yielded_descriptor:
                yielded_descriptor = True
                yield 'descriptor', descriptor_doc

            # yield the events
            for i, e in enumerate(events):
                print(i, e['data']['shot_number'])
                pprint(e)
                assert i == e['data']['shot_number']
                yield 'event', e

        yield 'stop', {'uid': new_uid(), 'run_start': suid,
                       'time': time.time()}


if __name__ == '__main__':
    # from bluesky.callbacks.broker import LiveImage
    from databroker.broker import Broker
    # import matplotlib.pyplot as plt

    # li = LiveImage('image', cmap='viridis',
    #                limit_func=lambda im: (
    #                    np.nanpercentile(im, 1),
    #                    np.nanpercentile(im, 99)
    #                ))
    db_path = '/media/christopher/DATA/Research/Columbia/pal/db'
    config = {'description': 'lightweight personal database',
              'metadatastore': {'module': 'databroker.headersource.sqlite',
                                'class': 'MDS',
                                'config': {'directory': db_path,
                                           'timezone': 'US/Eastern'}},
              'assets': {'module': 'databroker.assets.sqlite',
                         'class': 'Registry',
                         'config': {'dbpath': db_path + '/database.sql'}}}

    db = Broker.from_config(config)
    writer = NpyWriter(db.fs, db_path)

    for n, d in parse_hdf5('/media/christopher/DATA/Research/'
                           'Columbia/pal/20170919_run420.h5'):
        if n == 'descriptor':
            d['data_keys']['image']['external'] = True
        if n == 'event':
            d['data']['image'] = writer.write(d['data']['image'])
            d['filled']['image'] = False
        print(n)
        pprint(d)
        db.insert(n, d)
        # li(n, d)
        # plt.pause(1)
        # if n == 'event':
        #     print(d['data']['image'].shape)
