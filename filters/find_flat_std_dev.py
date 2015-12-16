import sys
import os
import h5py
import numpy
import time

# file_path = '/mnt/data/GSSTF_NCEP.3.concat.1x72x144.gzip9.h5'
file_path = '/mnt/data/GSSTF_NCEP.3.concat.25x20x20.gzip9.h5'
# file_path = '/mnt/data/GSSTF_NCEP.3.concat.7850x1x1.gzip9.h5'
h5path = '/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m'
return_values = []
print("start processing")
start_time = time.time()       
curr = prev = 0.0
with h5py.File(file_path, 'r') as f:
    dset_all = f[h5path]
    for i in range(7850):
        dset = dset_all[i][:][:]
        # mask fill value
        if '_FillValue' in dset_all.attrs:
            arr = dset[...]
            fill = dset_all.attrs['_FillValue'][0]
            v = arr[arr != fill]
        else:
            v = dset[...]
            # file name GSSTF_NCEP.3.YYYY.MM.DD.he5

        if i == 0:
            curr = prev = numpy.std(v)
        else:
            curr = numpy.std(v)
            delta = curr - prev
            if delta == 0.0:
                print("No change in std dev at time index =", i)
            prev = curr
end_time = time.time()
print(">>>>> runtime: {0:6.3f}s".format(end_time - start_time))
print(return_values)
