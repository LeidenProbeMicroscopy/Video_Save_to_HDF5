""""

HDF5 attributions:
hdf5 file: 'total frames', 'total frame dataset', 'dataset titles'
frame dataset: 'title', 'dataset number', properties...
properties dataset: 'title', 'dataset number'
"""

import numpy as np
import h5py
import bisect


class Frames(object):
    """
    for frame in frames
    frames[index]
    frames[start:end]
    """

    def __init__(self):
        self.dset = None

    def __iter__(self):
        pass

    def __next__(self):
        pass

    def __getitem__(self, index):
        pass

    def __len__(self):
        pass


class H5Reader(object):
    """

    """

    def __init__(self, file_name, mode: str = None):
        self.file_name = file_name
        self.mode = mode if mode in ('r', 'r+', 'w', 'w-', 'x', 'a') else 'r'
        self.h5file = h5py.File(self.file_name, self.mode)
        self.index_array = self._form_index_array()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        self.h5file.close()

    # Frame generator
    @property
    def frames(self):
        for key in self.h5file['Capture data']:
            if key != 'Properties':
                for frame in self.h5file['Capture data'][key]:
                    yield frame

    def frames_dset(self, dset_number):
        for frame in self.get_dataset(dset_number):
            yield frame

    # File information
    def show_info(self):
        self.h5file.visititems(print)

    # Get data from file
    def get_dataset(self, number, title='frames'):
        def find(_, obj):
            if obj.attrs.get('title') == title and obj.attrs.get('dataset number') == number:
                return obj

        if title in self.h5file.attrs['dataset titles'] and -1 < number <= self.h5file.attrs['total frame dataset']:
            ret = self.h5file.visititems(find)
            print(ret)
            return ret
        else:
            return False

    def get_frame(self, index=0, dset_number=0, dset_index=0):
        if index:
            dset_number, dset_index = self.convert_index(index)
        dset = self.get_dataset(dset_number)
        return dset[dset_index]

    def get_frame_properties(self, index=0, dset_number=0, dset_index=0):
        if index:
            dset_number, dset_index = self.convert_index(index)
        dset = self.get_dataset(dset_number)
        prop_dict = {}
        for name, val in dset.attrs.items():
            if isinstance(val, h5py.Reference):
                prop_dict[name] = self.h5file[dset.attrs[name]][dset_index]
            else:
                prop_dict[name] = val
        return prop_dict

    def convert_index(self, index):
        """
        convert the global index to dset_number and dset_index (index in a dataset        )
        :param index: index for all datasets
        :return: (dset_number, dset_index)
        """
        left, right = 0, len(self.index_array) - 1
        while left < right:
            mid = (left + right) // 2
            if self.index_array[mid] < index:
                left = mid + 1
            elif self.index_array[mid] > index:
                right = mid
            else:
                left = mid + 1
                break
        dset_number = left
        dset_index = index - self.index_array[dset_number - 1] if dset_number > 0 else index
        return dset_number, dset_index

    def convert_index_1(self, index):
        """
        convert the global index to dset_number and dset_index (index in a dataset        )
        :param index: index for all datasets
        :return: (dset_number, dset_index)
        """
        left, right = 0, len(self.index_array) - 1
        while left < right:
            mid = (left + right) // 2
            if self.index_array[mid] < index:
                left = mid + 1
            elif self.index_array[mid] > index:
                right = mid
            else:
                left = mid + 1
                break
        dset_number = left
        dset_index = index - self.index_array[dset_number - 1] if dset_number > 0 else index
        return dset_number, dset_index

    def convert_dset_loc(self, dset_number, dset_index):
        """
        convert dset_number, dset_index to a global index
        """
        return dset_index if dset_number == 0 else self.index_array[dset_number - 1] + dset_index

    def _form_index_array(self):
        index_temp = 0
        index_array = np.empty(self.h5file.attrs['total frame dataset'], dtype=int)
        for key in self.h5file['Capture data']:
            if key != 'Properties':
                dset = self.h5file['Capture data'][key]
                index_temp += dset.shape[0]
                index_array[dset.attrs['dataset number']] = index_temp
        return index_array

    # TODO
    # Generate video
    def video(self, *args):
        if len(args) == 0:
            start, end = 0, self.h5file.attrs['total frames']
        elif len(args) == 1:
            assert args < self.h5file.attrs['total frame dataset'], 'out of index'
            dset_number = args
        elif len(args) == 2:
            assert args[1] < self.h5file.attrs['total frames'], 'out of index'
            start, end = args
        else:
            raise ValueError
