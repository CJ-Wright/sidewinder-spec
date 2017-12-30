import h5py
from bluesky.utils import new_uid
import numpy as np


def format_keys(*args):
    return '/'.join(args)


key_data_map = {'RayMX': 'image', 'photodiode': 'I0', 'eh1_qbpm1': 'eh1_qbpm1',
                'oh_qbpm2': 'oh_qbpm2'}


# source, dtype, shape

def parse_hdf5(fn):
    f = h5py.File(fn, 'r')
    for n in f.keys():
        for nn in f[n].keys():
            suid = new_uid()
            # TODO: put in calibration parameters
            start_doc = {'uid': suid, 'delay': f[format_keys(n, nn)].attrs['delay_time_readback']}
            yield 'start', start_doc
            duid = new_uid()
            descriptor_doc = {'uid': duid, 'start_uid': suid, 'data_keys': {}}
            d = {}
            dts = {}
            for nnn in f[format_keys(n, nn)].keys():
                for nnnn in f[format_keys(n, nn, nnn)].keys():
                    k = format_keys(n, nn, nnn, nnnn)
                    # print(k)

                    ts = nnnn.strip('timestamp_')
                    if ts not in d:
                        d[ts] = {}
                        dts[ts] = {}
                    v = f[k].value
                    d[ts][key_data_map[nnn]] = f[k].value
                    dts[ts][key_data_map[nnn]] = int(ts)
                    if nnn not in descriptor_doc['data_keys']:
                        s = None
                        if isinstance(v, np.ndarray):
                            s = v.shape
                        else:
                            s = []
                        descriptor_doc['data_keys'][nnn] = {'source': nnn,
                                                            'dtype': type(v),
                                                            'shape': s}
            yield 'descriptor', descriptor_doc
            for t in sorted(d.keys()):
                yield 'event', {'uid': new_uid(), 'descriptor_uid': duid,
                                'data': d[t], 'timestamps': dts[t]}
            yield 'stop', {'uid': new_uid(), 'start_uid': suid}


if __name__ == '__main__':
    for n, d in parse_hdf5('/media/christopher/DATA/Research/Columbia/20170919_run420.h5'):
        if n == 'event':
            print(d['data']['image'][0, 0])