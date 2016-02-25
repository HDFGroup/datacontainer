#!/bin/bash
#
# run ipcluster before launching this script
MAXNODES=4
ENDPOINT="http://127.0.0.1:5000"
FILE="ncep3_25_20_20_gzip9"
DOMAIN=$FILE".hdfgroup.org"
cd ../filters/
for (( i=1; i <= $MAXNODES; i++ ))
do
    #echo "nodes:" $i
    #echo "domain: ["$DOMAIN"]"
    OUTFILE=$FILE"_summary_node_"$i".txt"
    echo $OUTFILE
    python summary_h5serv_par.py --file $DOMAIN --nodes $i --endpoint $ENDPOINT \
        --path /HDFEOS/GRIDS/NCEP/Data\ Fields/Tair_2m  | tee $OUTFILE
done
 