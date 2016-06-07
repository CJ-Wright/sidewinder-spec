"""
The plan:
1. Load up all the SPEC metadata
2. Load each run_start configuration file, which we write currently in each dir
3. Create the run_start document from config.txt file in run_folder
4. Make the data descriptors
5. Insert the data that goes with the image
5a. Match data with SPEC metadata
6. Insert runstop
7. Repeat for all data sets
"""
from __future__ import print_function

from sidewinder_spec.side_loader.loaders import *


def load_beamtime(root_folder, spec_file_name, dry_run=True, verbose=False):
    print('dry run:', dry_run)
    spec_file_loc = os.path.join(root_folder, spec_file_name)
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
            print(run_folder)
            try:
                general_loader(run_folder, spec_data, section_start_times,
                               dry_run)
            except:
                raise
                # pass
        elif verbose:
            print('{} did not exist, therefore {} is not a run folder'.format(
                config_file, run_folder))
            pass
