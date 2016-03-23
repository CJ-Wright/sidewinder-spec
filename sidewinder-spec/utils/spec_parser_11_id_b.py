import os
from datetime import datetime


def parse_spec_scan(scan):
    if scan.startswith('#U'):
        scan_dict = {}
        if "T = " in scan:
            a, T, i00 = scan.split(',')
            T = float(T.strip('T=C '))
            scan_dict['T'] = T
        else:
            a, i00 = scan.split(',')
        i00 = float(i00.strip('i00= '))
        scan_dict['I00'] = i00
        b, c = a.split(' at ')
        _, _, stem, shots = b.split()
        scan_dict['stem'] = stem
        scan_dict['shot_number'] = int(shots)
        c = c.strip()
        scan_dict['time_from_date'] = datetime.strptime(c,
                                                        '%a %b %d %H:%M:%S %Y')

        return scan_dict


def parse_spec_file(filename):
    fn = os.path.abspath(filename)
    with open(fn, 'r') as f:
        scan_data = f.read().split('#S')
    header = scan_data.pop(0)
    scan_data = [section.split('\n') for section in scan_data]
    # for scan in scan_data:
    #     print scan
    #     AAA
    scan_data = [item for sublist in scan_data for item in sublist][:-1]
    scans = [parse_spec_scan(scan) for scan in scan_data]
    return scans
