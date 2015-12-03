#!/bin/sh
h5repack -L -l CHUNK=1x72x144 -f GZIP=9 GSSTF_NCEP.3.concat.h5 GSSTF_NCEP.3.concat.1x72x144.gzip9.h5 
h5repack -L -l CHUNK=25x20x20 -f GZIP=9 GSSTF_NCEP.3.concat.h5 GSSTF_NCEP.3.concat.25x20x20.gzip9.h5 
h5repack -L -l CHUNK=7850x1x1 -f GZIP=9 GSSTF_NCEP.3.concat.h5 GSSTF_NCEP.3.concat.7850x1x1.gzip9.h5 
