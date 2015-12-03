import sys
import os
import h5py
import numpy
import time

# file_path = '/mnt/data/GSSTF_NCEP.3.concat.1x72x144.gzip9.h5'
# file_path = '/mnt/data/GSSTF_NCEP.3.concat.25x20x20.gzip9.h5'
file_path = '/mnt/data/GSSTF_NCEP.3.concat.7850x1x1.gzip9.h5'
h5path = '/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m'
return_values = []
print("start processing")
start_time = time.time()       
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

        return_values.append( (file_path, len(v), numpy.min(v), numpy.max(v), numpy.mean(v),numpy.median(v), numpy.std(v) ) )
end_time = time.time()
print(">>>>> runtime: {0:6.3f}s".format(end_time - start_time))
print(return_values)
