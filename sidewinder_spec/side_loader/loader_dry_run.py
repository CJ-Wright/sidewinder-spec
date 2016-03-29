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
import numpy as np
from uuid import uuid4

if __name__ == '__main__':
    import os
    from sidewinder_spec.utils.parsers import parse_spec_file, \
        parse_tif_metadata, \
        parse_tif_metadata, parse_run_config


    root_folder = '/mnt/bulk-data/research_data/USC_beamtime/APS_March_2016/'

    # 1. Load up all the SPEC metadata
    spec_file_loc = os.path.join(root_folder, 'EmirMar16')
    spec_data = parse_spec_file(spec_file_loc)
    print len(spec_data)

    section_start_times = np.asarray(
        [section[0]['time_from_date'] for section in spec_data])
    for section in spec_data:
        print section[0]


    from sidewinder_spec.side_loader.loaders import *
    run_folders = [os.path.join(root_folder, f) for f in [
        # 'Quartz_Background/temp_exp'
        # 'S1/temp_exp',
        # 'S6',
        'Calibration'
    ]]

    for run_folder in run_folders:
        # 2. Load each run_start configuration file, which we write currently in each dir

        config_file = os.path.join(run_folder, 'config.txt')
        run_config = parse_run_config(config_file)
        run_kwargs = run_config
        try:
            # figure out type of run to load
            loader = run_loaders[run_kwargs['run_config']['loader_name']]
            # Load it
            run_start_uuid = loader(run_folder, spec_data, section_start_times, run_kwargs)
        except KeyError:
            print('That kind of loader does not exist please try a'
                  ' different one')

