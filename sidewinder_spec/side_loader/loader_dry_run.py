"""
The plan:
1. Load up all the SPEC metadata
2. Load each run_start configuration file, which we write currently in each dir
3. Create the run_start document which should include:
    [Distance 1]
    Distance
    File_Stem
    StartMask
    Configuration_img (fs_uuid?)
    Configuration_poni(fs_uuid?) (also write a pyFAI.geo handler)
    Background: file or folder
4. Make the data descriptors
5. Insert the data that goes with the image
5a. Match data with SPEC metadata
6. Insert runstop
7. Repeat for all data sets
"""
from __future__ import print_function

if __name__ == '__main__':
    import os
    from sidewinder_spec.utils.parsers import parse_spec_file, \
        parse_tif_metadata, \
        parse_tif_metadata, parse_run_config
    import numpy as np
    from uuid import uuid4
    from filestore.api import db_connect as fs_db_connect
    from metadatastore.api import db_connect as mds_db_connect
    from sidewinder_spec.side_loader.loaders import *
    from databroker import db, get_events

    fs_db_connect(**{'database': 'data-processing-dev', 'host': 'localhost',
                     'port': 27017})
    mds_db_connect(**{'database': 'data-processing-dev', 'host': 'localhost',
                      'port': 27017})

    root_folder = '/mnt/bulk-data/research_data/USC_beamtime/APS_March_2016/'

    # dry_run = True
    dry_run = False
    # 1. Load up all the SPEC metadata
    spec_file_loc = os.path.join(root_folder, 'EmirMar16')
    spec_data = parse_spec_file(spec_file_loc)
    print(len(spec_data))

    section_start_times = np.asarray(
        [section[0]['time_from_date'] for section in spec_data])
    for section in spec_data:
        print(section[0])
    print('Start loading-------------------------------------------')

    for run_folder, dirs, files in os.walk(root_folder):
        config_file = os.path.join(run_folder, 'config.txt')
        if os.path.exists(config_file):
            general_loader(run_folder, spec_data, section_start_times, dry_run)
        else:
            # print('{} did not exist, therefore {} is not a run folder'.format(config_file, run_folder))
            pass
