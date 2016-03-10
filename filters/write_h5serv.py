
import sys
import h5pyd
#import h5py as h5pyd
import numpy as np

endpoint="http://127.0.0.1:5000"

cube_side = 256

if len(sys.argv) > 1:
    cube_side = int(sys.argv[1])
    
filename = "cube_" + str(cube_side) + "_" + str(cube_side) + "_" + str(cube_side)
filename += ".client_test.hdfgroup.org"
#filename += ".h5"
 
f = h5pyd.File(filename, "w", endpoint=endpoint)

print("filename,", f.filename)
 
print("create dataset")
 
dset = f.create_dataset('dset', (cube_side, cube_side, cube_side), dtype='f8')

print("name:", dset.name)
print("shape:", dset.shape)
print("dset.type:", dset.dtype)
print("dset.maxshape:", dset.maxshape)

print("writing data...")

for i in range(cube_side):
    arr = np.random.rand(cube_side, cube_side)
    dset[i,:,:] = arr
print("done!")

f.close() 

 
