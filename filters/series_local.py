import sys
import os
import h5py
import numpy
import time

return_values = []
# file_path = '/mnt/data/GSSTF_NCEP.3.concat.1x72x144.gzip9.h5'
# file_path = '/mnt/data/GSSTF_NCEP.3.concat.25x20x20.gzip9.h5'
file_path = '/mnt/data/GSSTF_NCEP.3.concat.7850x1x1.gzip9.h5'
file_name = os.path.basename(file_path)
h5path = '/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m'
return_values = []
start_time = time.time()       
lat = 444
lon = 555
with h5py.File(file_path, 'r') as f:
    
    dset = f[h5path]

    for i in range(7850):
        val = dset[i, lat, lon]
        return_values.append( (file_name, val) )
            
end_time = time.time()
print(">>>>> runtime: {0:6.3f}s".format(end_time - start_time))
print(return_values)
