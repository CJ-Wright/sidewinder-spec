import re
from pprint import pprint
import numpy as np
import os
from bluesky.utils import new_uid
import time


files_types = ['.gsa', '.dat']

'NOM_Li4SiO4_gas_CO2_dry_at_25C_cycle2'

gsas_parser_list = [
    ('bt_wavelength', 0, 'Wavelength: (.+?) Angstrom', ('Wavelength:', 'Angstrom'), float),
    ('run', 0, 'Sample Run: (.+?) ', ('Sample Run: ', ), int),
    ('IPTS', 7, 'IPTS-(.+?)', ('IPTS-', ), int),
    ('primary flight path', 11, 'Primary flight path (.+?)m', ('Primary flight path ', 'm'), float),
    ('total flight path', 12, 'Total flight path   (.+?)m', ('Total flight path   ', 'm'), float),
    ('tth', 12, 'tth   (.+?)deg', ('tth   ', 'deg'), float),


]


def gsas_subparser(string):
    output = {}
    gsas_data = string.split('\n')[:14]
    gsas_data = [s.strip('# ').strip() for s in gsas_data]
    for name, line, re_string, strips, dtype in gsas_parser_list:
        re_res = re.search(re_string, gsas_data[line])
        if re_res:
            data = re_res.group()
            for strip in strips:
                data = data.strip(strip)
            data = data.strip()
            if data:
                data = dtype(data)
                output[name] = data
    output.update({'wavelength unit': 'A',
                   'primary flight path unit': 'm',
                   'total flight path unit': 'm',
                   'tth unit': 'deg'
                   })

    return output


def parse(file_dir):
    start_uid = new_uid()
    start_doc = {'facility': 'NOMAD', 'uid': start_uid, 'sideloaded': True,
                 'time': time.time()}
    gsas_root = os.path.join(file_dir, 'GSAS')
    gsas_files = [f for f in os.listdir(gsas_root) if f.endswith('.gsa')]
    full_prof_root = os.path.join(file_dir, 'fullprof')
    full_prof_files = [f for f in os.listdir(gsas_root) if f.endswith('.dat')]
    a = gsas_files[0].split('_')
    print(a)
    AAA
    with open(os.path.join(gsas_root, gsas_files[0]), 'r') as f:
        start_doc.update(gsas_subparser(f.read()))
    print(start_doc)
    descriptors = []
    # for bank in range(6):





if __name__ == '__main__':
    parse('/home/christopher/dev/17ly_NOMADpipeline/data')
