from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import sys
import os
import subprocess
import time
import argparse
import h5pyd
import numpy

#globals
endpoint = None
h5path = None
h5serv_domain = None
 
def summary(day):
     
    return_value = None
          
    with h5pyd.File(h5serv_domain, endpoint=endpoint) as f:
            dset = f[h5path]
     
            # mask fill value
            if '_FillValue' in dset.attrs:
                arr = dset[day,:,:]
                fill = dset.attrs['_FillValue'][0]
                v = arr[arr != fill]
            else:
                v = dset[day,:,:]
                
            return_value = (day, len(v), numpy.min(v), numpy.max(v), numpy.mean(v),
                numpy.median(v), numpy.std(v) )   
                
    return return_value

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', "--filename", help="h5serv domain")
    parser.add_argument('-p', "--path", help="h5path")
    parser.add_argument('-e', "--endpoint", help="h5serv endpoint")
     
    # example file:
    # public AWS -
    # s3://hdfgroup/data/hdf5test/GSSTF_NCEP.3.2000.05.01.he5
    # OSDC Ceph -
    # s3://hdfdata/ncep3/GSSTF_NCEP.3.2000.05.01.he5

    # example path (for above file):
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Psea_level
    # or 
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m
    
    global endpoint, h5path, h5serv_domain
    
    args = parser.parse_args()

    if not args.filename and not args.input:
        sys.exit("No filename specified!")
        
    if not args.path:
        sys.exit("No h5path specified!")
        
    if not args.endpoint:
        endpoint = "http://127.0.0.1:5000"
    else:
        endpoint = args.endpoint
    
    h5path = args.path
    h5serv_domain = args.filename
    print("domain:", h5serv_domain)
    print("h5path:", h5path)
    print("endpoint:", endpoint)
    
        
    with h5pyd.File(h5serv_domain, endpoint=endpoint) as f:
            dset = f[h5path]
            num_days = dset.shape[0]
            
    print("start processing")
    num_days = 10    
    # run process_files on engines
    start_time = time.time()       
    
    output = []
    for day in range(num_days):
        print("day:", day, flush=True)
        result = summary(day)
        output.append(result)
        
    end_time = time.time()
    print(">>>>> runtime: {0:6.3f}s".format(end_time - start_time))
        
    for item in output:
        text = ""
        for value in item:
            text += str(value) + "   "
        print(text)
    

main()
