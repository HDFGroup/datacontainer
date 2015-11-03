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

BATCH_SIZE = 5  # download files this many at a time

file_names = []
h5path = None

def summary(file_path, h5path):
   
    file_name = os.path.basename(file_path)
     
    #print("Summary ", file_path, h5path)

    if not h5py.is_hdf5(file_path):
        raise IOError("Not an HDF5 file: " + file_path)
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
              
def process_files():
    print("process files")
    s3_prefix = "s3://"
    downloads = []  # handles to sub-processes
    s3_cache_dir = os.environ["S3_CACHE_DIR"]
    downloaded_files = []
    for filename in file_names:
        if filename.startswith(s3_prefix):
            if s3_cache_dir is None:
                raise IOError("Environment variable S3_CACHE_DIR not set")
            s3_path = filename[len(s3_prefix):]
            s3_uri = filename
            local_filepath = os.path.join(s3_cache_dir, s3_path)
     
            if os.path.exists(local_filepath):
                # todo, check that the s3 object is the same as local copy
                pass
            else:
                p = subprocess.Popen(['s3cmd', 'get', s3_uri, local_filepath])
                downloads.append(p)
            downloaded_files.append(local_filepath)
        else:
            downloaded_files.append(filename)
            
    if len(downloads) > 0:
        done = False
        while not done:
            print('.')
            time.sleep(1)
            done = True
            for p in downloads:
                p.poll()
                if p.returncode is None:
                    done = False # still waiting on a download
                elif p.returncode < 0:
                    raise IOError("s3cmd failed for " + filename)
                else:
                    pass  # success!
                    
    print("downloads complete")
    print("h5path", h5path)
    return_values = []
    for filename in downloaded_files:
        output = summary(filename, h5path )   
        return_values.append(output)
    return return_values     
    
def dobatch(filelist, **kwargs):
    downloads = []  # handles to sub-processes
    s3_cache_dir = os.environ["S3_CACHE_DIR"]
    downloaded_files = []
    for filename in filelist:
        if filename.startswith(s3_prefix):
            if s3_cache_dir is None:
                raise IOError("Environment variable S3_CACHE_DIR not set")
            s3_path = filename[len(s3_prefix):]
            s3_uri = filename
            local_filepath = os.path.join(s3_cache_dir, s3_path)
     
            if os.path.exists(local_filepath):
                # todo, check that the s3 object is the same as local copy
                pass
            else:
                p = subprocess.Popen(['s3cmd', 'get', s3_uri, local_filepath])
                downloads.append(p)
            downloaded_files.append(local_filepath)
        else:
            downloaded_files.append(filename)
            
    if len(downloads) > 0:
        done = False
        while not done:
            print('.')
            time.sleep(1)
            done = True
            for p in downloads:
                p.poll()
                if p.returncode is None:
                    done = False # still waiting on a download
                elif p.returncode < 0:
                    raise IOError("s3cmd failed for " + filename)
                else:
                    pass  # success!
                    
    print("downloads complete")
    h5path = kwargs['h5path']
    if "dview" in kwargs and kwargs["dview"]:
        dview = kwargs["cluster"]
        results = []
        engine = 0
        for filename in downloaded_files:
            dview = rc[engine]
            result = dview.apply(summary, filename, h5path)
            results.append(result)   
            engine = (engine + 1) % num_engines  # move on to next engine
        ready = False
        while not ready:
            ready = True
            for result in results:
                if not result.ready():
                    ready = False
                    break
        print("ready!")
        for result in results:
            print(result.get())
    else:
        for filename in downloaded_files:
            output = summary(filename, h5path )   
            print(output)     
     
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', "--filename", help="name of file or s3 uri")
    parser.add_argument('-i', "--input", help="text file of files or s3 uri")
    parser.add_argument('-p', "--path", help="h5path")
    parser.add_argument('-c', "--cluster", help="cluster profile")
    # example file:
    # public AWS -
    # s3://hdfgroup/data/hdf5test/GSSTF_NCEP.3.2000.05.01.he5
    # OSDC Ceph -
    # s3://hdfdata/ncep3/GSSTF_NCEP.3.2000.05.01.he5

    # example path (for above file):
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Psea_level
    

    args = parser.parse_args()

    if not args.filename and not args.input:
        sys.exit("No filename specified!")

    if not args.path:
        sys.exit("No h5path specified!")
    global h5path
    h5path = args.path
    
    files = []
    if args.input:
        with open(args.input) as f:
            for line in f:
                line = line.strip()
                if not line or line[0] == '#':
                    continue
                file_names.append(line)              
    else:
        file_names.append(args.filename)
         
        
    rc = None # client interface for cluster mode
    dview = None
        
    if args.cluster:
        rc = Client()
        if len(rc.ids) == 0:
            sys.exit("No engines found")
        print(len(rc.ids), "engines")
        dview = rc[:]
        dview.block = True # use sync 
        # have engines import packages
        with dview.sync_imports():
            import sys
            import os
            import time
            import h5py
            import numpy
        # send the summary method to engines
        dview.push(dict(summary=summary))
        
        # push the path name
        dview.push(dict(h5path=h5path))
       
        # split file_names across engines
        dview.scatter('file_names', file_names)
       
        # run process_files on engines
        output = dview.apply(process_files)
    else:
        output = process_files() # just run locally
       
    for elem in output:
        if type(elem) is list:
            #output from engines, break out each tuple
            for item in elem:
                print(item)
        else:
            print(elem)
                 

    
    """
    batch = []    
    for filename in files:
        batch.append(filename)
        if len(batch) == BATCH_SIZE:
            dobatch(batch, h5path=args.path, dview=dview)
            batch = []
    """        
    #dobatch(batch, h5path=args.path, dview=dview)  # catch any trailers

main()
