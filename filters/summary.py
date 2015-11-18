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
    s3.addFiles(s3uri)
    keys = list(s3.downloads)
    keys.sort()
    
    for s3uri in keys:
        download = s3.downloads[s3uri]
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
        
    from s3downloader import S3ParallelDownload
 
    s3p = S3ParallelDownload(rc) 
        
    s3p.loadFiles(s3uri)

    files = s3p.getFiles()
    
    print("files:")
    for filename in files:
        print(filename)
              
    print("start processing")
        
    # run process_files on engines
    start_time = time.time()
    dview = s3p.dview # rc[:]
    
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
    print('>>>>> runtime: ', end_time - start_time)
     
    for elem in output:
        print(elem)
        continue
        if type(elem) is list:
            # output from engines, break out each tuple
            for item in elem:
                print(item)
        else:
            print(elem)
   

main()
