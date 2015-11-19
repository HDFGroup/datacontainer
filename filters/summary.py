from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import sys
import os
import subprocess
import time
import h5py
import numpy
import argparse
from ipyparallel import Client


def summary(file_path, h5path):
    
    file_name = os.path.basename(file_path)

    # print("Summary ", file_path, h5path)

    if not os.path.exists(file_path):
        raise IOError(platform.node() + ': File does not exist: ' + file_path)
    elif not h5py.is_hdf5(file_path):
        raise IOError(platform.node() + ": Not an HDF5 file: " + file_path)
    with h5py.File(file_path, 'r') as f:
        dset = f[h5path]

        # mask fill value
        if '_FillValue' in dset.attrs:
            arr = dset[...]
            fill = dset.attrs['_FillValue'][0]
            v = arr[arr != fill]
        else:
            v = dset[...]
        # file name GSSTF_NCEP.3.YYYY.MM.DD.he5

        return(file_name, len(v), numpy.min(v), numpy.max(v), numpy.mean(v),
               numpy.median(v), numpy.std(v))
 
def processFiles(s3uri, h5path):
    print("processFiles()")
    return_values = []  
    
    from s3downloader import S3Download
    s3 = S3Download()
     
    downloads = s3.getFiles(s3uri_prefix=s3uri)
    if len(downloads) == 0:
        raise IOError("No downloads found")
        
    for download in downloads:
        if download['state'] != 'COMPLETE':
            raise IOError(s3uri + " in invalid state: " + download['state'])
        print(download)
        output = summary(download["local_filepath"], h5path)
        return_values.append(output)
    return return_values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', "--filename", help="s3 uri")
    parser.add_argument('-p', "--path", help="h5path")
     
    # example file:
    # public AWS -
    # s3://hdfgroup/data/hdf5test/GSSTF_NCEP.3.2000.05.01.he5
    # OSDC Ceph -
    # s3://hdfdata/ncep3/GSSTF_NCEP.3.2000.05.01.he5

    # example path (for above file):
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Psea_level
    # or 
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m
    
    file_names = []

    args = parser.parse_args()

    if not args.filename and not args.input:
        sys.exit("No filename specified!")
        
    if not args.filename.startswith("s3://"):
        sys.exit("Provide s3 uri path")

    if not args.path:
        sys.exit("No h5path specified!")
    
    h5path = args.path
    s3uri = args.filename
    
    rc = Client()
    if len(rc.ids) == 0:
        sys.exit("No engines found")
    print(len(rc.ids), "engines")    
    
    dview = rc[:] 
     
    print("start processing")
        
    # run process_files on engines
    start_time = time.time()
    
    with dview.sync_imports():
            import sys
            import os
            import h5py
            import numpy
            import platform
    
    # push the summary method to the engines
    dview.push(dict(summary=summary))
    
    output = dview.apply_sync(processFiles, s3uri, h5path)
    end_time = time.time()
    print(">>>>> runtime: {0:6.3f}s".format(end_time - start_time))
     
    # sort the output by first field (filename) 
    output_dict = {} 
    for elem in output:
        if type(elem) is list:
            # output from engines, break out each tuple
            for item in elem:
                k = item[0]
                if k not in output_dict:
                    output_dict[k] = item
        else:
            k = item[0]
            if k not in output_dict:
                output_dict[k] = item
                    
    keys = list(output_dict.keys())
    keys.sort()
    for k in keys:
        print(output_dict[k])
   

main()
