"""This stores data from a NOMAD measurement in a databroker"""
from pprint import pprint

from databroker.broker import Broker
from shed.savers import NpyWriter

from shed_sidewinder.nomad import parse

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

for n, d in parse('/path/to/folder'):
    if n == 'descriptor':
        for k in ['tof', 'intensity', 'error']:
            d['data_keys'][k]['external'] = True
    if n == 'event':
        for k in ['tof', 'intensity', 'error']:
            d['data'][k] = writer.write(d['data'][k])
            d['filled'][k] = False
    print(n)
    pprint(d)
    db.insert(n, d)
