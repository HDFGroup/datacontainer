
import sys
import h5pyd
#import h5py as h5pyd
import numpy as np

endpoint="http://127.0.0.1:5000"
n = 256

if len(sys.argv) > 1:
    n = int(sys.argv[1])
    
filename = "cube_" + str(n) + "_" + str(n) + "_" + str(n)
filename += ".client_test.hdfgroup.org"
#filename += ".h5"
 
f = h5pyd.File(filename, "w", endpoint=endpoint)
 
dset = f.create_dataset('dset', (n, n, n), dtype='f8')

for i in range(n):
    print(i+1, "of", n)
    arr = np.random.rand(n, n)
    dset[i,:,:] = arr
print("done!")

f.close() 

 
