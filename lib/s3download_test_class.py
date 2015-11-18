import sys
import subprocess
import os
import time
import logging

sys.path.append('s3downloader')

from s3downloader import S3Download 
 
    
#main

ncep3_files = ["s3://hdfdata/ncep3/GSSTF_NCEP.3.1987.07.01.he5",
"s3://hdfdata/ncep3/GSSTF_NCEP.3.1987.07.02.he5",
"s3://hdfdata/ncep3/GSSTF_NCEP.3.1987.07.03.he5",
"s3://hdfdata/ncep3/GSSTF_NCEP.3.1987.07.04.he5"]

s3download = S3Download()
nspace = s3download.freespace
print("free space:", nspace)
#usedspace = s3download.usedspace
#print("used space", usedspace)
"""
print("clearing..")
s3download.clear()
usedspace = s3download.usedspace
print("used space", usedspace)

#s3download.addFiles(ncep3_files)
s3download.addFiles("s3://hdfdata/ncep3/")
output = s3download.dump()

#output = s3download.s3cmdls("s3://hdfdata/ncep3/")
print(output)
"""
"""
s3download.addFiles("s3://hdfdata/ncep3/")
count = s3download.start()
while count > 0:
    count = s3download.update()
    #print(s3download.dump())
    time.sleep(1)
print("done!")
"""



    
     
     
  
