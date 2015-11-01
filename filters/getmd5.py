from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import sys
import os
import subprocess
import time
import argparse
import hashlib

BATCH_SIZE = 5  # download files this many at a time
s3_prefix = "s3://"

def getmd5(file_path):
     
    file_name = os.path.basename(file_path)
    
    print("md5 ", file_path)
    
    hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
            
    print(file_name, hash.hexdigest())

    
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
    for filename in downloaded_files:
        getmd5(filename)        
     
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', "--filename", help="name of file or s3 uri")
    parser.add_argument('-i', "--input", help="text file of files or s3 uri")
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

    files = []
    if args.input:
        with open(args.input) as f:
            for line in f:
                line = line.strip()
                if not line or line[0] == '#':
                    continue
                files.append(line)              
    else:
        files.append(args.filename)
         
    
    batch = []    
    for filename in files:
        batch.append(filename)
        if len(batch) == BATCH_SIZE:
            dobatch(batch)
            batch = []
            
    dobatch(batch)  # catch any trailers


main()
