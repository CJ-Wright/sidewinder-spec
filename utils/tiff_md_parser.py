import ConfigParser
import time
from datetime import datetime

def parse_tif_metadata(file):
    config = ConfigParser.ConfigParser()
    config.read(file)
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
        output_dict['time_from_date'] = datetime.strptime(output_dict['datestring'],
                                                        pattern)
    if 'timestamp' in output_dict.keys():
        output_dict['time'] = datetime.fromtimestamp(output_dict['timestamp'])
    return output_dict

