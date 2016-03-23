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
    Background_img (File or folder if T dependent) (fs_uuid?)
    Background_chi (File or folder if T dependent) (fs_uuid?)
4. Make the data descriptors
5. Insert the data that goes with the image
5a. Match data with SPEC metadata
6. Insert runstop
7. Repeat for all data sets

"""
# from filestore.api import insert_resource, insert_datum, register_handler
# from filestore.handlers_base import HandlerBase
import numpy as np
from uuid import uuid4

if __name__ == '__main__':
    import os
    from sidewinder_spec.utils.spec_parser_11_id_b import parse_spec_file
    from sidewinder_spec.utils.tiff_md_parser import parse_tif_metadata
    from xpd_workflow.vis import StackExplorer
    import matplotlib.pyplot as plt

    # 1. Load up all the SPEC metadata
    spec_file_loc = '/mnt/bulk-data/research_data/USC_beamtime/APS_March_2016/EmirMar16'
    spec_data = parse_spec_file(spec_file_loc)
    print len(spec_data)

    section_start_times = np.asarray([section[0]['time_from_date'] for section in spec_data])

    run_folders = ['/mnt/bulk-data/research_data/USC_beamtime/APS_March_2016/S1/temp_exp',
                   '/mnt/bulk-data/research_data/USC_beamtime/APS_March_2016/S6']
    for run_folder in run_folders:
        # 2. Load each run_start configuration file, which we write currently in each dir
        # config_file = os.path.join(run_folder, 'config.txt')
        # kwargs = 'GIANT BLOB FROM CONFIG FILE'

        # Load all the metadata files in the folder
        tiff_metadata_files = [os.path.join(run_folder, f) for f in os.listdir(run_folder)
                               if f.endswith('.tif.metadata')]
        tiff_metadata_data = [parse_tif_metadata(f) for f in tiff_metadata_files]

        # Sort the folder's data by time so we can have the start time
        timestamp_list = [f['timestamp'] for f in tiff_metadata_data]

        # read in all the remaining data namely image file names
        tiff_file_names = [f[:-9] for f in tiff_metadata_files]

        # sort remaining data by time
        sorted_tiff_metadata_data = [x for (y, x) in sorted(
            zip(timestamp_list, tiff_metadata_data))]

        sorted_tiff_file_names = [x for (y, x) in sorted(
                zip(timestamp_list, tiff_file_names))]

        # make subset of spec data for this run
        ti = sorted_tiff_metadata_data[0]['time_from_date']

        # make a sub spec list which contains the spec section related to our data
        spec_start_idx = np.argmin(np.abs(section_start_times - ti))
        sub_spec = spec_data[spec_start_idx]

        # 3. Create the run_start document.
        print 'Run Start goes here'

        data_keys1 = {'I0': dict(source='IO', dtype='number'),
                      'img': dict(source='det', dtype='array', shape=(2048, 2048),
                                  external='FILESTORE:')}
        data_keys2 = {'T': dict(source='T', dtype='number'),}

        print 'Event descriptors go here'
        print data_keys1
        print data_keys2

        # insert all the temperature data
        temperature_data = [scan['T'] for scan in sub_spec]
        time_data = [scan['time_from_date'] for scan in sub_spec]

        for idx, (temp, t) in enumerate(zip(temperature_data, time_data)):
            tmp_dict = {'descriptor':'descriptor2_uid', 'time':t, 'data':{'T': temp},
                         'uid':str(uuid4()),
                         'timestamps':{'T': t}, 'seq_num':idx}
            print tmp_dict

        # insert the images
        I0 = [scan['I00'] for scan in sub_spec]

        for idx, (img_name, I, timestamp) in enumerate(
                zip(sorted_tiff_file_names, I0, time_data)):
            fs_uid = uuid4()
            # resource = insert_resource('TIFF', img_name)
            # insert_datum(resource, fs_uid)
            data = {'img': fs_uid, 'I0': I}
            timestamps = {'img': timestamp, 'I0': timestamp}
            tmp_dict = {'descriptor':'descriptor1_uid', 'time':timestamp, 'data':data,
                         'uid':str(uuid4()), 'timestamps':timestamps, 'seq_num':idx}
            print tmp_dict
        print "Run Stop goes here"
        plt.plot(time_data, temperature_data)
        plt.show()
