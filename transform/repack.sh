#!/bin/bash
#
# 1. Create repacked/ directory ($mkdir repacked/).
# 2. Run like ./repack.sh ncep_files.txt
# 
# ncep_files.txt can be found in [1]
#
# [1] http://github.com/HDFGroup/datacontainer/jobs/ncep_files.txt
#
while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Repacking file: $line"
    name=${line##*/}
    echo $name
    /home/ubuntu/bin/s3cmd get $line
    
    # h5py suggested size:
    # h5repack -L -l CHUNK=45x180 -f GZIP=9 $name repacked/$name
    # /home/ubuntu/bin/s3cmd put repacked/$name s3://hdfdata/ncep3_chunk_45_180_gzip_9/$name

    # Unidata suggested size:
    # h5repack -L -l CHUNK=22x46 -f GZIP=9 $name repacked/$name
    # /home/ubuntu/bin/s3cmd put repacked/$name s3://hdfdata/ncep3_chunk_22_46_gzip_9/$name

    # SZIP. It works only on joe_issue10 snapshot.
    # /home/ubuntu/anaconda/bin/h5repack -L -l CHUNK=45x180 -f SZIP=8,NN $name repacked/$name
    # /home/ubuntu/bin/s3cmd put repacked/$name s3://hdfdata/ncep3_chunk_45_180_szip_8_NN/$name

    # Mafisc. It works only on joe_issue10 snapshot.
    # /home/ubuntu/anaconda/bin/h5repack -L -l CHUNK=45x180 --filter=UD=32002,1,0  $name repacked/$name
    # /home/ubuntu/bin/s3cmd put repacked/$name s3://hdfdata/ncep3_chunk_45_180_mafisc_32002_1_0/$name

    # Blosc. It works only on joe_issue10 snapshot.
    /home/ubuntu/anaconda/bin/h5repack -L -l CHUNK=45x180 -f UD=32001,2,2,4  $name repacked/$name
    /home/ubuntu/bin/s3cmd put repacked/$name s3://hdfdata/ncep3_chunk_45_180_blosc_2_2_4/$name
    
    rm -f repacked/$name
    rm -f $name
done < "$1"
