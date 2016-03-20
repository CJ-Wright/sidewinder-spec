import ConfigParser
import time

def parse_tif_metadata(file):
    config = ConfigParser.ConfigParser()
    config.read(file)
    ouput_dict = {}
    for section in config.sections():
        for option in config.options(section):
            try:
                ouput_dict[option] = float(config.get(section=section,
                                                  option=option))
            except ValueError:
                ouput_dict[option] = config.get(section=section,
                                                  option=option)
    # if 'datestring' in ouput_dict.keys():
    #     pattern = '%Y.%m.%d : %H:%M:%S.%f'
    #     epoch = int(time.mktime(time.strptime(ouput_dict['datestring'], pattern)))
    #     ouput_dict['epoch_time'] = epoch
    return ouput_dict


if __name__ == '__main__':
    f = '/run/media/christopher/Seagate Backup Plus Drive/merge_backup/Documents/Research/Brown/Sun_Lab/PDF/PDFdata/July/X17A/1/NiPd_1/NiPd_1-00031.tif.metadata'

    print parse_tif_metadata(f)