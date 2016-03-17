
import requests
import json
import sys

# test the number of calls we can make to h5serv 
# (that don't really do anything)

endpoint="http://127.0.0.1:5000"
n = 1000

if len(sys.argv) > 1:
    n = int(sys.argv[1])
 
for i in range(n):   
    req = endpoint + "/info"
    rsp = requests.get(req)
    if rsp.status_code != 200:
        sys.exit("bad status code: " + str(rsp.status_code))
    
print("done!")    
