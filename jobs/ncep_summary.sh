#!/bin/sh
#
cd ../util
python s3pd.py get s3://hdfdata/ncep3/   
cd ../filters/
python summary.py --file s3://hdfdata/ncep3 --path /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m
