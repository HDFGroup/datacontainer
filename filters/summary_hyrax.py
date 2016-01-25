import time
import pydap
import numpy
from pydap.client import open_url
# file_path = 'http://localhost:8080/opendap/data/hdf5/GSSTF_NCEP.3_concat.h5'
file_path = 'http://172.17.192.40:8080/opendap/data/hdf5/GSSTF_NCEP.3_concat.h5'
return_values = []
dataset = open_url(file_path)
# Make sure that server returns datasets.
print(dataset.keys())
dset_all = dataset['_HDFEOS_GRIDS_NCEP_Data_Fields_Tair_2m']
print("start processing")
start_time = time.time()       
# For quick testing use a small number.
# for i in range(3):
for i in range(7850):
    dset = dset_all[i][:][:]
    # Mask fill value
    if '_FillValue' in dset_all.attributes:
        arr = dset[...]
        fill = dset_all._FillValue
        v = arr[arr != fill]
    else:
        v = dset[...]
    return_values.append( (file_path, len(v), numpy.min(v), numpy.max(v), numpy.mean(v),numpy.median(v), numpy.std(v) ) )
end_time = time.time()
print(">>>>> runtime: {0:6.3f}s".format(end_time - start_time))
print(return_values)

