#!/bin/sh
docker run -v /mnt/ssd/data:/data -v ${PWD}:/usr/local/src/query -it hdfgroup/hdf5lib:1.8.16 python /usr/local/src/query/ghcn_yearmax_h5py.py