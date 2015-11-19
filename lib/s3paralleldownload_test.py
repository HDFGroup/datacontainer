import sys
import subprocess
import os
import time
import logging
from ipyparallel import Client

sys.path.append('s3downloader')

import s3downloader  
import s3paralleldownloader
 
   
#main

ncep_url = "S3://hdfdata/ncep3/"

rc = Client()
 
s3paralleldownloader.init(rc)
print("clearing..")
s3paralelldownloader.clear(rc)
"""


num_engines = len(rc.ids)
if num_engines == 0:
    raise IOError("No engines running")

dview = rc[:]
dview.block=True

with dview.sync_imports():
    import s3downloader
    
dview.apply(s3downloader.init)

# get list of uri's3

s3downloader.init()
downloads = s3downloader.s3cmdls(ncep_url)
for download in downloads:
    print(download['uri'])
    
if len(downloads) == 0:
    raise IOError("Nothing to download!")

    
# manually divy up the files to the set of engines
file_set_size = len(downloads)/num_engines   # PY 3 style float result

uri_map = {}
engine_num = 0
for download in downloads:
    if engine_num not in uri_map.keys():
        uri_map[engine_num] = []
    uri_list = uri_map[engine_num]
    uri_list.append(download['uri'])
    engine_num = (engine_num+1) % num_engines
     
      
# add each engines list
async_results = []
for engine_id in range(num_engines):
    engine_view = rc[engine_id]
    url_list = uri_map[engine_id]
    async_result = engine_view.apply_async(s3downloader.addFiles, url_list)
 
print("uri_map", uri_map)
    
   
# wait while the file list gets downloaded   
ready = False
while not ready:
    ready = True
    for async_result in async_results:
        if not async_result.ready():
            ready = False
            break
    if not ready:
        time.sleep(1)
        
engine_files = dview.apply(s3downloader.getFiles)  
                           
s3downloader.addFiles("s3://hdfdata/ncep3/")
output = s3downloader.dump()

s3_uris = s3downloader.getFiles(state="PENDING")
print(s3_uris)
 
counts = dview.apply(s3downloader.start)
while any(v > 0 for v in counts):
    time.sleep(1)
    counts = dview.apply(s3downloader.update)
    print("counts:", counts)
"""    
print("done!")




    
     
     
  
