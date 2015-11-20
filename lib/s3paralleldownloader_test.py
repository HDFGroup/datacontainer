import sys
import subprocess
import os
import time
import logging
from ipyparallel import Client

sys.path.append('s3downloader')

from s3paralleldownloader import S3ParallelDownload
 
#main

ncep_url = "s3://hdfdata/ncep3/"

rc = Client()

if len(rc.ids) == 0:
    raise IOError("No engines running")
 
s3p = S3ParallelDownload(rc) 
 
print("clearing..")
s3p.clear()

print("loading ncep3")
s3p.loadFiles(ncep_url)

files = s3p.getFiles()
print("files:")
for filepath in files:
    print(filepath)

print("done")




    
     
     
  
