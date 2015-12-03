#!/bin/bash
#
# Cortad files require big disk space. Run an OSDC instance with as 
# large disk space as possible.
#
# 1. Create /mnt/data/repacked/ directory ($mkdir /mnt/data/repacked/).
# 2. Run like ./repack_cortad.sh cortad.txt
# 

# Change to partition that has big space.
cd /mnt/data
while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Repacking file: $line"
    name=${line##*/}
    echo $name
    /home/ubuntu/bin/s3cmd get $line
    
    # Remove compression.
    h5repack -f NONE -L -l CHUNK=101x540x540 $name /mnt/data/repacked/$name
    /home/ubuntu/bin/s3cmd put /mnt/data/repacked/$name s3://hdfdata/cortad_no_filter/$name

    # h5py suggested size:
    # h5repack -L -l CHUNK=45x180 -f GZIP=9 $name /mnt/data/repacked/$name
    # /home/ubuntu/bin/s3cmd put /mnt/data/repacked/$name s3://hdfdata/ncep3_chunk_45_180_gzip_9/$name

    # Unidata suggested size:
    # h5repack -L -l CHUNK=22x46 -f GZIP=9 $name /mnt/data/repacked/$name
    # /home/ubuntu/bin/s3cmd put /mnt/data/repacked/$name s3://hdfdata/ncep3_chunk_22_46_gzip_9/$name

    # SZIP. It works only on issue25 snapshot.
    # /home/ubuntu/anaconda/bin/h5repack -L -l CHUNK=45x180 -f SZIP=8,NN $name /mnt/data/repacked/$name
    # /home/ubuntu/bin/s3cmd put /mnt/data/repacked/$name s3://hdfdata/ncep3_chunk_45_180_szip_8_NN/$name

    # Mafisc. It works only on issue25 snapshot.
    # /home/ubuntu/anaconda/bin/h5repack -L -l CHUNK=45x180 --filter=UD=32002,1,0  $name /mnt/data/repacked/$name
    # /home/ubuntu/bin/s3cmd put /mnt/data/repacked/$name s3://hdfdata/ncep3_chunk_45_180_mafisc_32002_1_0/$name

    # Blosc. It works only on issue25 snapshot.
    # /home/ubuntu/anaconda/bin/h5repack -L -l CHUNK=45x180 -f UD=32001,2,2,4  $name /mnt/data/repacked/$name
    # /home/ubuntu/bin/s3cmd put /mnt/data/repacked/$name s3://hdfdata/ncep3_chunk_45_180_blosc_2_2_4/$name
    
    rm -f /mnt/data/repacked/$name
    # rm -f $name
done < "$1"
