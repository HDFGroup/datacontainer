from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import sys
import os
import subprocess
import time
import argparse
import h5pyd
import numpy
from ipyparallel import Client

def series(s3uri, h5path, lat, lon):
    # import within the function so the engines will pick them up
    import sys
    import os
    import h5py
    import numpy
     
    return_values = []
    for download in s3.files(s3uri_prefix=s3uri):
        file_path = download['local_filepath']   
        file_name = os.path.basename(file_path)

        with h5py.File(file_path, 'r') as f:
            dset = f[h5path]
            val = dset[lat, lon]
            return_values.append( (file_name, val) )
                
    return return_values

def summary(day):
    # import within the function so the engines will pick them up
    import sys
    import os
    import h5pyd
    import numpy
     
     
    return_value = None
          
    with h5pyd.File(h5serv_domain, 'r', endpoint=endpoint) as f:
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
    parser.add_argument('-n', "--nodes", help="number of engine nodes")
     
    # example file:
    # public AWS -
    # s3://hdfgroup/data/hdf5test/GSSTF_NCEP.3.2000.05.01.he5
    # OSDC Ceph -
    # s3://hdfdata/ncep3/GSSTF_NCEP.3.2000.05.01.he5

    # example path (for above file):
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Psea_level
    # or 
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m
    
    args = parser.parse_args()

    if not args.filename and not args.input:
        sys.exit("No filename specified!")
        
    if not args.path:
        sys.exit("No h5path specified!")
        
    if not args.endpoint:
        endpoint = "http://127.0.0.1:5000"
    else:
        endpoint = args.endpoint
    if args.nodes:
        nodes = int(args.nodes)
    else:
        nodes = 1
    
    h5path = args.path
    h5serv_domain = args.filename
    print("domain:", h5serv_domain)
    print("h5path:", h5path)
    print("endpoint:", endpoint)
    print("nodes:", nodes)
    
        
    with h5pyd.File(h5serv_domain, endpoint=endpoint) as f:
            dset = f[h5path]
            num_days = dset.shape[0]
            
    rc = Client()
    if len(rc.ids) == 0:
        sys.exit("No engines found")
     
    if nodes > len(rc.ids):
        sys.exit("Not enough engines!")
     
    dview = rc[:nodes] 
    dview.push(dict(h5serv_domain=h5serv_domain, h5path=h5path, endpoint=endpoint))
     
    print("start processing")
        
    # run process_files on engines
    start_time = time.time()       
    
    #num_days = 10
    output = dview.map_sync(summary, range(num_days))
    end_time = time.time()
     
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
            k = elem[0]
            if k not in output_dict:
                output_dict[k] = elem
                    
    keys = list(output_dict.keys())
    keys.sort()
    for k in keys:
        text = ""
        values = output_dict[k]
        for value in values:
            text += str(value) + "   "
        print(text)
        
    print(">>>>> runtime: {0:6.3f}s".format(end_time - start_time))
    

main()
