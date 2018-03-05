"""This stores data from a PAL measurement in a databroker"""
from pprint import pprint

from databroker.broker import Broker
from shed.savers import NpyWriter

from shed_sidewinder.pal_xfel import parse_hdf5

db_path = '/path/to/db'
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

for n, d in parse_hdf5('/path/to/hdf5.file'):
    if n == 'descriptor':
        d['data_keys']['image']['external'] = True
    if n == 'event':
        d['data']['image'] = writer.write(d['data']['image'])
        d['filled']['image'] = False
    print(n)
    pprint(d)
    db.insert(n, d)


# from bluesky.callbacks.broker import LiveImage
# import matplotlib.pyplot as plt

# li = LiveImage('image', cmap='viridis',
#                limit_func=lambda im: (
#                    np.nanpercentile(im, 1),
#                    np.nanpercentile(im, 99)
#                ))
# li(n, d)
# plt.pause(1)
# if n == 'event':
#     print(d['data']['image'].shape)