import timeit
import os
from h5saver import *
from h5reader import *


def test_writer():
    # 1) init
    now = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime(time.time()))
    h5path = 'test'
    h5name = 'test file' + ' ' + now + '.h5'
    fpath = h5path + os.path.sep + h5name
    variable_prop_dict = {'exposure time': 'uint16', 'time': 'float64'}

    # 2) create images
    img_set = []
    img_info_1 = (((1500, 2048), 'uint16'),
                  ((600, 2048), 'uint8'),
                  ((1500, 2048), 'uint16'))
    img_info_2 = (((1500, 2048), 'uint16'),
                  ((1500, 2048), 'uint8'),
                  ((1500, 2048), 'uint16'))
    for (img_size, data_type) in img_info_1:
        for i in range(5):
            prop = {'shape': img_size, 'dtype': data_type, 'exposure time': 100.3, 'time': time.time()}
            frame = np.random.randint(255, size=img_size, dtype=np.dtype(data_type))
            img_set.append(ImageData(frame, prop))
    for (img_size, data_type) in img_info_2:
        for i in range(5):
            prop = {'shape': img_size, 'dtype': data_type, 'exposure time': 100.3, 'time': time.time()}
            frame = np.random.randint(255, size=img_size, dtype=np.dtype(data_type))
            img_set.append(ImageData(frame, prop))

    # 3) time saving images
    start = timeit.default_timer()
    with H5Saver(file_name=h5name, folder_path=h5path, variable_prop=variable_prop_dict) as h5saver:
        for img in img_set:
            h5saver.save_image(img)
    stop = timeit.default_timer()
    run_time = stop - start

    # 4) open the file again to check the information of the saved file
    print("Saved file information:")
    with h5py.File(fpath, 'r') as hf:
        hf.visititems(print)
        print('Chunks size of frame dataset:')
        for k in hf['Capture data'].keys():
            if k != 'Properties':
                print('{} : {}'.format(k, hf['Capture data'][k].chunks))
        print(hf.attrs.keys())

    # 5) Finally, print our conclusions
    print()
    print('Saved HDF5 file size: {:.2f} MB'.format((os.path.getsize(fpath) / 1024 / 1024)))
    print('Running time: {:.4f} seconds'.format(run_time))
    print('Saving rate: {:.4f} frame/second'.format(h5saver.total_frames / run_time))


def test_reader():
    file_path = './test/'
    file_list = os.listdir(file_path)
    file_list.sort(key=lambda f: os.path.getmtime(file_path + f))
    print(file_list)
    file_name = os.path.join(file_path, file_list[-1])
    print(file_name)
    # hf = h5py.File(file_name, 'r')
    # for k, v in hf.attrs.items():
    #     print(k, v)
    # hf.close()
    with H5Reader(file_name) as h5reader:
        # print((hf["Capture data/Frames dataset #0"][1] == ret[1]).all())
        # print(h5reader.get_frame_properties(dset_number=0, dset_index=0))
        # print(h5reader.get_frame_properties(0))
        # print(h5reader.get_frame(0))
        # print(h5reader.get_frame(dset_number=0, dset_index=0))

        h5reader.show_info()
        # print(h5reader.get_dataset(1))
        for i in range(30):
            print(h5reader.convert_index(i))
        # frames = h5reader.frames
        # frames_dset = h5reader.frames_dset(0)
        # for frame in frames_dset:
        #     print(frame)


def clean_test_folder():
    file_path = './test/'
    file_list = os.listdir(file_path)
    for f in file_list:
        os.remove('./test/' + f)


if __name__ == '__main__':
    clean_test_folder()
    test_writer()
    test_reader()
