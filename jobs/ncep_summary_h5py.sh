#!/bin/sh
# construct list of s3 uri's to run
s3cmd ls s3://hdfdata/ncep3_chunk_45_180_gzip_9/ | grep -v xml | grep he5 | rev | cut -d: -f1 | rev | sed -e 's/^/s3:/' > ncep_files_h5py.txt
cd ../filters/
python summary.py --input ../jobs/ncep_files_h5py.txt --path /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m -c 1
# rm ../jobs/ncep_files_h5py.txt
