#!/bin/sh
#
cd ../util
python s3pd.py get s3://hdfdata/ncep3/\*.he5   
cd ../filters/
python series.py --file s3://hdfdata/ncep3 --path /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m --lat 444 --lon 555
