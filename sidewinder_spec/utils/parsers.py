try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser
import os
from datetime import datetime
from sidewinder_spec import time_from_epoch


def parse_spec_file(filename, exclude='test'):
    """
    Parse a spec file for loading into the DB
    Parameters
    ----------
    filename: Str
        The spec file to parse
    exclude: str or list of str
        Strings which denote lines to be excluded even though the have #U

    Returns
    -------
    list of lists of dicts:
        The data parsed by section then line with each line's data parsed into
        a dictionary
    """
    fn = os.path.abspath(filename)
    with open(fn, 'r') as f:
        scan_data = f.read().split('#S')
    header = scan_data.pop(0)
    scan_data = [section.split('\n') for section in scan_data]

    total_parsed_data = []
    for section in scan_data:
        section_parsed_data = []
        for line in section:
            if line.startswith('#U') and exclude not in line:
                section_parsed_data.append(parse_spec_scan(line))
        if len(section_parsed_data) != 0:
            total_parsed_data.append(section_parsed_data)
    return total_parsed_data
    # scan_data = [item for sublist in scan_data for item in sublist][:-1]
    # scans = [parse_spec_scan(scan) for scan in scan_data if scan.startswith('#U')]
    # return scans


def parse_spec_scan(line):
    """
    Parse a line of the spec file
    Parameters
    ----------
    line: str
        The line which contains metadata for a saved image
    Returns
    -------
    dict:
        A dictionary which contains the metadata for an image
    """
    scan_dict = {}
    if "T = " in line:
        a, T, i00 = line.split(',')
        T = float(T.strip('T=C '))
        scan_dict['T'] = T
    else:
        a, i00 = line.split(',')
    i00 = float(i00.split('= ')[-1])
    scan_dict['I00'] = i00
    b, c = a.split(' at ')
    _, _, stem, shots = b.split()
    scan_dict['stem'] = stem
    scan_dict['shot_number'] = int(shots)
    c = c.strip()
    spec_time_pattern = '%a %b %d %H:%M:%S %Y'
    scan_dict['time_from_date'] = time_from_epoch(
        datetime.strptime(c, spec_time_pattern))

    return scan_dict


def parse_tif_metadata(filename):
    """
    Parse the data from the QXRD tif.metadata file

    Parameters
    ----------
    filename: str
        The file location

    Returns
    -------
    dict:
        The metadata, parsed
    """
    config = ConfigParser.ConfigParser()
    config.read(filename)
    output_dict = {}
    for section in config.sections():
        for option in config.options(section):
            try:
                output_dict[option] = float(config.get(section=section,
                                                       option=option))
            except ValueError:
                output_dict[option] = config.get(section=section,
                                                 option=option)
    if 'datestring' in output_dict.keys():
        pattern = '%Y.%m.%d : %H:%M:%S.%f'
        output_dict['time_from_date'] = time_from_epoch(
            datetime.strptime(output_dict['datestring'],
                              pattern))
    if 'timestamp' in output_dict.keys():
        output_dict['time'] = time_from_epoch(
            datetime.fromtimestamp(output_dict['timestamp']))
    return output_dict


def parse_run_config(file):
    config = ConfigParser.ConfigParser()
    try:
        config.read(file)
        output_dict = {}
        for section in config.sections():
            output_dict2 = {}
            for option in config.options(section):
                try:
                    output_dict2[option] = float(config.get(section=section,
                                                            option=option))
                except ValueError:
                    output_dict2[option] = config.get(section=section,
                                                      option=option)
            output_dict[section] = output_dict2
        return output_dict
    except ConfigParser.ParsingError:
        print('Invalid Config File')
        return None


def parse_new_spec_file(filename, exclude=['test']):
    fn = os.path.abspath(filename)
    with open(fn, 'r') as f:
        scan_data = f.read().split('\n\n')
    runs = [section.split('\n') for section in scan_data]
    return runs


def parse_new_spec_run(runs):
    proc_runs = []
    for run in runs:
        d_run = {'events': []}
        for header_or_event in run:
            name, md_dict = parse_header_or_event(header_or_event)
            if name in ['start', 'stop']:
                d_run[name] = md_dict
            else:
                d_run['events'].append(md_dict)
        proc_runs.append(d_run)
    return proc_runs


def parse_header_or_event(header_or_event):
    name, data = header_or_event.split(': ', 1)
    md_dict = {}
    for md in data.split(', '):
        k, v = md.split(':')
        md_dict[k] = v
    return name, md_dict
