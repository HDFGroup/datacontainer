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


def series(s3uri, h5path, lat, lon):
    # import within the function so the engines will pick them up
    import sys
    import os
    import h5py
    import numpy
    from s3downloader import S3Download
    
    s3 = S3Download()
    return_values = []
    for download in s3.files(s3uri_prefix=s3uri):
        file_path = download['local_filepath']   
        file_name = os.path.basename(file_path)

        with h5py.File(file_path, 'r') as f:
            dset = f[h5path]
            val = dset[lat, lon]
            return_values.append( (file_name, val) )
                
    return return_values

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', "--filename", help="s3 uri")
    parser.add_argument('-p', "--path", help="h5path")
    parser.add_argument("--lat", help="latitude")
    parser.add_argument("--lon", help="longitude")
     
    # example file:
    # public AWS -
    # s3://hdfgroup/data/hdf5test/GSSTF_NCEP.3.2000.05.01.he5
    # OSDC Ceph -
    # s3://hdfdata/ncep3/GSSTF_NCEP.3.2000.05.01.he5

    # example path (for above file):
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Psea_level
    # or 
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m
    #
    # For now lat/lon or just indexes into the datsaet
    # lat range [0, 720]
    # long range [0, 1440]
    
    file_names = []

    args = parser.parse_args()

    if not args.filename and not args.input:
        sys.exit("No filename specified!")
        
    if not args.filename.startswith("s3://"):
        sys.exit("Provide s3 uri path")

    if not args.path:
        sys.exit("No h5path specified!")
        
    if not args.lat:
        sys.exit("No latitude specified!")
        
    if not args.lon:
        sys.exit("No longitude specified!")
    
    
    h5path = args.path
    s3uri = args.filename
    lat = int(args.lat)
    lon = int(args.lon)
    
    rc = Client()
    if len(rc.ids) == 0:
        sys.exit("No engines found")
    print(len(rc.ids), "engines")    
    
    dview = rc[:] 
     
    print("start processing")
        
    # run process_files on engines
    start_time = time.time()       
   
    output = dview.apply_sync(series, s3uri, h5path, lat, lon)
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
        text = ""
        values = output_dict[k]
        for value in values:
            text += str(value) + "   "
        print(text)
   

main()
