#!/bin/sh
#
cd ../filters/
python summary_h5serv.py --file NCEP3_concat.hdfgroup.org --endpoint http://127.0.0.1:5000 --path /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m
