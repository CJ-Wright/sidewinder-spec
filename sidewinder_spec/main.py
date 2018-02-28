from argparse import ArgumentParser
from pprint import pprint
import os

from databroker import Broker
from shed.savers import NpyWriter
import yaml

from sidewinder_spec.nomad import parse as nomad_parse
from sidewinder_spec.pal_xfel import parse_hdf5 as pal_parse

FACILITY_PARSER_MAP = {'nomad': {'cmd': nomad_parse,
                                 'externals': ['tof', 'intensity', 'error']},
                       'pal': {'cmd': pal_parse,
                               'externals': ['image']}
                       }


def create_parser():
    p = ArgumentParser()
    subp = p.add_subparsers(title='cmd', dest='cmd')
    init = subp.add_parser('init', help='Initialize databroker')
    init.add_argument('name', help='name for databroker')
    init.add_argument('path', help='path to store the db')

    for facility in FACILITY_PARSER_MAP:
        a = subp.add_parser(facility,
                            help='parse data from {}'.format(facility))
        a.add_argument('db_name', help='name of databroker to add data to')
        a.add_argument('input_data',
                       help='path to data on disk (potentially including'
                            'a single file')
    return p


def main(args=None):
    parser = create_parser()
    ns = parser.parse_args(args)
    d = ns.__dict_
    if ns.cmd in FACILITY_PARSER_MAP:
        db = Broker.named(d['db_name'])
        db_path = db.get_config()['metadatastore']['config']['directory']
        writer = NpyWriter(db.fs, db_path)
        for n, di in FACILITY_PARSER_MAP[ns.cmd]['cmd'](ns['input_data']):
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
    else:
        db_config_path = os.path.expanduser('~/.config/databroker/'
                                            '{}.yaml'.format(ns['name']))
        path = os.path.expanduser(ns['path'])
        config = {'description': 'lightweight personal database',
                  'metadatastore': {'module': 'databroker.headersource.sqlite',
                                    'class': 'MDS',
                                    'config': {'directory': path,
                                               'timezone': 'US/Eastern'}},
                  'assets': {'module': 'databroker.assets.sqlite',
                             'class': 'Registry',
                             'config': {'dbpath': os.path.join(
                                 path, 'database.sql')}}}
        with open(db_config_path, 'w', encoding='utf8') as f:
            yaml.dump(config, f)
