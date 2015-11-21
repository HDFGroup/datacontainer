from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
                        
import time                        
from ipyparallel import Client

def hrFormat(nbytes):
#
# Format num bytes as human readable string
#
    kb = 1024
    mb = kb*1024
    gb = mb*1024
    tb = gb*1024
    if nbytes > tb:
        return "{0:6.2f}TB".format(nbytes/tb)
    if nbytes > gb:
        return "{0:6.2f}GB".format(nbytes/gb)
    if nbytes > mb:
        return "{0:6.2f}MB".format(nbytes/mb)
    if nbytes > kb:
        return "{0:6.2f}KB".format(nbytes/kb)
    return str(nbytes) + " bytes"
    
 
def printResponse(responses):
#
# Pretty print download list
#
    engine_no = 0
    for response in responses:
        label = "engine #{0:4d}:".format(engine_no)
        if len(response) == 0:
            print(label, "<empty>")
        else:
            print(label)
        for download in response:
            print(">", download['s3_date'], download['s3_time'], download['size'], download['s3_uri'])
        engine_no += 1  
#
# Print usage and exit
#
def printUsage():
    print("usage: python s3pd.py [-h] cmd uri")
    print("  options -h: print this message")
    print("   ") 
    print("  cmd:   ")
    print("     ls - list downloaded files")
    print("     du - space used ")
    print("     get - get files")
    print("     rm - remove downloaded files")
    
    print("  ")
    print("  uri: uri path starting with s3://")
    print(" ------------------------------------------------------------------------------")
    print("  Example - download ncep3 files ")
    print("       python s3pd.py get s3://hdfdata/ncep3/  ")
    print("  Example - disk usage for files downloaded from ncep3")
    print("       python s3pd.py du s3://hdfdata/ncep3 ")
    print("  Example - list  files downloaded from ncep3")
    print("       python s3pd.py ls s3://hdfdata/ncep3 ")
    print("  Example - remove downloaded files  ")
    print("       python s3pd.py rm s3://hdfdata/ncep3 ")
    print(" ")
     
    
 
def main():
    
    import sys
     
    if len(sys.argv) <= 1:
        printUsage()
        sys.exit(-1)
        
    if sys.argv[1] in ("-h", "-help", "--help", "help"):
        printUsage()
        sys.exit(-1)
      
    argn = 1    
    cmd = sys.argv[argn]
    if cmd not in ("ls", "du", "get", "rm"):
        printUsage()
        sys.exit(-1)
    argn += 1    
    s3_uri = "s3://"
    if len(sys.argv) > argn:
        s3_uri = sys.argv[argn]
        if not s3_uri.startswith("s3://"):
            printUsage()
            sys.exit(-1)
            
    if cmd == "get" and s3_uri == "s3://":
        print("Provide bucket name in s3 uri")
        sys.exit(-1)
    
    
    rc = Client()
    if len(rc.ids) == 0:
        sys.exit("No engines found")
    
    print(len(rc.ids), "engines")     
        
    from s3downloader import S3ParallelDownload
 
    s3p = S3ParallelDownload(rc) 
        
    start_time = time.time()
    bytes_downloaded = 0
    # proces the different commands
    
    if cmd == "get":
        used_old = s3p.usedspace()
        s3p.loadFiles(s3_uri)  
        responses = s3p.getFiles(s3uri=s3_uri)
        printResponse(responses)
        used_new = s3p.usedspace()
        for n in range(len(used_old)):
            bytes_downloaded += (used_new[n] - used_old[n])
        
            
    elif cmd == "ls":
        print("ls cmd")
        responses = s3p.getFiles(state='COMPLETE', s3uri=s3_uri)    
        printResponse(responses)
        
    elif cmd == "du":
        print("du cmd")
        used_responses = s3p.usedspace()
        free_responses = s3p.freespace()
        if len(used_responses) != len(free_responses):
            raise IOError("unexpected response")
        engine_no = 0
        for n in range(len(used_responses)):
            label = "engine #{0:4d}:".format(engine_no)
            used = hrFormat(used_responses[n])
            free = hrFormat(free_responses[n])
            percent_used = used_responses[n] / (used_responses[n] + free_responses[n])
            percent_used = int(percent_used * 100.0) 
            percent_used = str(percent_used) + '%'
            print(label, used, "used", free, "free", percent_used, "percent_used")
            engine_no += 1
            
    elif cmd == "rm":
        print("rm cmd")
        used_old  = s3p.usedspace()
        s3p.clear(s3uri=s3_uri)
        used_new = s3p.usedspace()
        bytes_freed = 0
        for n in range(len(used_old)):
            bytes_freed += (used_old[n] - used_new[n])
        print(hrFormat(bytes_freed), "deleted")
        
              
    end_time = time.time()
    print(">>>>> runtime: {0:6.3f}s".format(end_time - start_time))
    if bytes_downloaded > 0:
        print(">>>>> ", hrFormat(bytes_downloaded), "downloaded")
        rate = bytes_downloaded / (end_time - start_time)
        print(">>>>>", hrFormat(rate), "/s")
    
main()
