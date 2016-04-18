#!/bin/sh
docker run -v /mnt/ssd/data:/data -v ${PWD}:/usr/local/src/query -it hdfgroup/pytables:3.2.2 python /usr/local/src/query/ghcn_pytables.py 
