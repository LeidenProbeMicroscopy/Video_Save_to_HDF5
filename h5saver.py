import numpy as np
import h5py
import time
import os


class ImageData(object):
    """
    Test ImageData class
    """

    def __init__(self, frame, prop: dict):
        self.properties = prop
        self.frame = frame


class H5Saver(object):
    """
    test hdf5 file saver
    """

    def __init__(self, file_name=None, folder_path='.', max_frames=100, variable_prop={}, chunks_size=None, **kwargs):
        self.file_name = file_name  # name of h5 file, if None, the name defaults to 'ImagingData_<Year><Month><Day>_<Hour><Minutes><Second>.h5'
        self.folder_path = folder_path  # path to folder to store the h5 file
        self.frames_dset = None  # This variable will store the h5 dataset handler for saving frames
        self.prop_dset = None  # h5 dataset handler for saving the dataset properties
        self.variable_prop = variable_prop  # dict with the {key: 'dtype'} of the variable values
        self.variable_prop_dset = {}  # {k:None for k in variable_prop.keys()}     # dict that will store the variable properties for a dataset: # dicts with properties that can change over time: the user must initiate this dict with keys and datatype {key: 'dtype'} from the ImageData property class that are variable
        self.frames_dest_prop = {'shape': None, 'dtype': None}  # dict that will store the image shape and dtype
        self.ndset = 0  # index number of datasets inside group
        self.nframes = 0  # index of frame inside dataset
        self.total_frames = 0  # accumulator for the total frames saved between initializing and stop
        self.max_frames = max_frames  # the max amount of frames that can be stored in a dataset before extend_dataset() is needed
        self.max_frames_set = max_frames  # default max frames value upon dataset creation
        self.dataset_titles = ('frames',)
        self.chunks_size = chunks_size  # (1, #, #), No bigger than 1 MiB
        self.setting = kwargs  # NOT BEING USED YET

    def save_image(self, img: ImageData):
        # 1) ADAPT h5 dataset to incoming data: if image shape or dtype changes, close the old frame dataset and create a new one
        if self.frames_dest_prop['shape'] != img.properties['shape'] or self.frames_dest_prop['dtype'] != \
                img.properties['dtype']:
            # 1a) Finish the saving process of last dataset
            if self.ndset != 0:
                self.dataset_done()
            # 1b) Initialise the parameters and the new dataset
            self.max_frames = self.max_frames_set  # reset the max_frames number to the default setting value
            self.frames_dest_prop['shape'] = img.properties['shape']
            self.frames_dest_prop['dtype'] = img.properties['dtype']
            self.frames_dset = self.create_frames_dataset()
            for prop, dt in self.variable_prop.items():  # Create new VARIABLE properties datasets self.expo_time_dset, self.other_prop_dset
                if prop in img.properties.keys():
                    self.variable_prop_dset[prop] = self.create_prop_dataset(prop, dt)
            self.ndset += 1  # update total frames dataset number
            self.nframes = 0  # initial the frame number in the new dataset
        # 2) extend the size of datasets if needed
        if self.nframes >= self.max_frames:
            self.extend_dataset()
        # 3) save frames
        self.frames_dset[self.nframes] = img.frame
        # 4) save properties
        for k, v in img.properties.items():
            if k not in self.variable_prop.keys():
                if self.nframes == 0:
                    self.frames_dset.attrs[k] = v
            else:
                self.variable_prop_dset[k][self.nframes] = v
        # 5) Finally: increase number of frames by 1
        self.nframes += 1

    def create_h5file(self):
        if not os.path.exists(self.folder_path):
            os.makedirs(self.folder_path)
        if self.file_name is None:
            self.file_name = 'ImagingData_' + time.strftime("%y%m%d_%H%M%S", time.localtime(time.time())) + '.h5'
        file_path = self.folder_path + os.path.sep + self.file_name
        return h5py.File(file_path, 'a')

    def create_frames_dataset(self):
        # dataset created in the group of 'Capture data'
        dset_name = 'Capture data/Frames dataset #{}'.format(self.ndset)
        shape = (self.max_frames,) + self.frames_dest_prop['shape']
        max_shape = (None,) + self.frames_dest_prop['shape']
        dt = np.dtype(self.frames_dest_prop['dtype'])
        chunks = self.cal_chunks(self.frames_dest_prop['shape'], dt)
        print(chunks)
        # print('Frames dataset #{} created!\n\r'.format(self.ndset))
        dset = self.h5file.create_dataset(name=dset_name, shape=shape, maxshape=max_shape, dtype=dt, chunks=True)
        dset.attrs['title'] = 'frames'
        dset.attrs['dataset number'] = self.ndset

        return dset

    def create_prop_dataset(self, prop: str, date_type: str):
        # dataset created in the group of 'Properties'
        dset_name = 'Capture data/Properties/{} dataset #{}'.format(prop, self.ndset)
        shape = (self.max_frames,)
        max_shape = (None,)
        dt = np.dtype(date_type)
        # print('Properties \'{}\' dataset #{} created!\n\r'.format(prop, self.ndset))
        dset = self.h5file.create_dataset(name=dset_name, shape=shape, maxshape=max_shape, dtype=dt, chunks=True)
        dset.attrs['title'] = prop
        dset.attrs['dataset number'] = self.ndset
        if prop not in self.dataset_titles:
            self.dataset_titles += (prop,)

        return dset

    def dataset_done(self):
        # Resize datasets size based on the actual saved frames number
        self.frames_dset.resize(self.nframes, axis=0)
        self.total_frames += self.nframes
        for prop, prop_dset in self.variable_prop_dset.items():
            if prop_dset:
                prop_dset.resize(self.nframes, axis=0)
                self.frames_dset.attrs[
                    prop] = prop_dset.ref  # link the properties datasets to the attributions of frames datasets
        self.h5file.flush()
        # print("data flushed!!!")
        # print('Saved HDF5 file size: {:.2f} MB'.format((os.path.getsize(self.folder_path + os.path.sep + self.file_name)/1024/1024)))

    def extend_dataset(self):
        # double the size of all datasets
        self.max_frames *= 2
        self.frames_dset.resize(self.max_frames, axis=0)
        for prop, prop_dset in self.variable_prop_dset.items():
            if prop_dset:
                prop_dset.resize(self.max_frames, axis=0)

    def set_h5file_attrs(self):  # TODO  file attribution
        attrs_dict = {'total frames': self.total_frames,
                      'total frame dataset': self.ndset,
                      'dataset titles': self.dataset_titles
                      }
        for k, v in attrs_dict.items():
            self.h5file.attrs[k] = v

    def start(self):
        print('Saving started')
        self.h5file = self.create_h5file()

    def stop(self):
        self.dataset_done()  # finish the last dataset
        self.set_h5file_attrs()
        self.h5file.close()
        msg = '{} frames and {} frames datasets have been saved!\r\n'.format(self.total_frames, self.ndset)
        print(msg)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *excinfo):
        self.stop()

    # UTILITY FUNCTIONS

    @staticmethod
    def cal_chunks(frame_shape, dtype):
        # No bigger than 1 MB (depent on cache)
        # (1, frame_size[0], frame_[1]), load data frame by frame
        # 1. Larger chunks for a given dataset size reduce the size of the chunk B-tree, making it faster to find and load chunks.
        # 2. Larger chunks also increase the chance that you’ll read data into memory you won’t use.
        # 3. The HDF5 chunk cache can only hold a finite number of chunks. Chunks bigger than 1 MiB don’t even participate in the cache.
        a, b = frame_shape
        dt_size = np.dtype(dtype).itemsize # bytes
        size = a * b * dt_size
        while size > 1024 ** 2:
            a //= 2
            size = a * b * dt_size
        chunks = (1, a, b)
        return chunks

        pass
