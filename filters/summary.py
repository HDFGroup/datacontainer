import sys
import os
import h5py
import numpy as np
import argparse

sys.path.append('../lib')
from  s3cache import getS3Object

    
def file_helper(filepath):
    if filepath.startswith("s3://"):
        filepath = getS3Object(filepath)
    if not os.path.exists(filepath):
        raise IOError("filepath " + filepath + " does not exist")
    if not h5py.is_hdf5(filepath):
        raise IOError("filepath " + filepath + " is not an HDF5 file")
    
    return filepath
         

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', "--file", help="name of file or s3 uri")
    parser.add_argument('-p', "--path", help="h5path")
    # example file:
    # public AWS -  
    # s3://hdfgroup/data/hdf5test/GSSTF_NCEP.3.2000.05.01.he5
    # OSDC Ceph -
    # s3://hdfdata/ncep3/GSSTF_NCEP.3.2000.05.01.he5
    
    # example path (for above file):
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Psea_level
    
    args = parser.parse_args()
    
    if not args.file:
        print("No filename specified!")
        sys.exit(1)
        
    if not args.path:
        print("No h5path specified!")
        sys.exit(1)
        
    file_path = file_helper(args.file)
    file_name = os.path.basename(file_path)
    result = {}
    with h5py.File(file_path, 'r') as f:
        dset = f[args.path]
        
        # mask fill value
        if '_FillValue' in dset.attrs:
            tair_2m = dset[...]
            fill = dset.attrs['_FillValue'][0]
            v = tair_2m[tair_2m != fill]
        else:
            v = dset[...]
        # file name GSSTF_NCEP.3.YYYY.MM.DD.he5
        key = file_name
        result = [(key, (len(v), np.mean(v), np.median(v), np.std(v)))]
       
        print "result: ", result
        

main()