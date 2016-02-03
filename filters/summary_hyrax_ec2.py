import time
import pydap
import numpy
from pydap.client import open_url

# Pick URL for either Hyrax, Hyrax-CF or THREDDS.

# Hyrax-CF: EnableCF=False
file_path = 'http://54.174.38.12:8888/opendap/data/HPD/hpd/GSSTF_NCEP.3.2008.01.01.he5'
h5path= '/HDFEOS/GRIDS/NCEP/Data%20Fields/Tair_2m'

# Hyrax: EanbleCF=True
# file_path = 'http://54.174.38.12/opendap/data/HPD/hpd/GSSTF_NCEP.3.2008.01.01.he5'
# h5path = 'Tair_2m'

# THREDDS
# file_path = 'http://54.174.38.12/thredds/dodsC/testAll/hpd/GSSTF_NCEP.3.2008.01.01.he5'
# h5path = '/HDFEOS/GRIDS/NCEP/Data_Fields/Tair_2m'




return_values = []
dataset = open_url(file_path)
# Make sure that server returns datasets.
print(dataset.keys())
dset = dataset[h5path]
print(dset.attributes)
print("start processing")
start_time = time.time()       

# Mask fill value
if '_FillValue' in dset.attributes:
    arr = dset[...]
    fill = dset._FillValue
    v = arr[arr != fill]
else:
    v = dset[...]
end_time = time.time()    
return_values.append( (file_path, len(v), numpy.min(v), numpy.max(v), numpy.mean(v),numpy.median(v), numpy.std(v) ) )

print(">>>>> runtime: {0:6.3f}s".format(end_time - start_time))
print(return_values)

