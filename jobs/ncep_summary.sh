#!/bin/sh
# construct list of s3 uri's to run
s3cmd ls s3://hdfdata/ncep3/ | grep -v xml | grep he5 | rev | cut -d: -f1 | rev | sed -e 's/^/s3:/' > ncep_files.txt
cd ../filters/
python summary.py --input ../jobs/ncep_files.txt --path /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m
