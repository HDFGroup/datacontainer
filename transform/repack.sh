#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Repacking file: $line"
    name=${line##*/}
    echo $name
    /home/ubuntu/bin/s3cmd get $line
    h5repack -L -l CHUNK=45x180 -f GZIP=9 $name repacked/$name
    /home/ubuntu/bin/s3cmd put repacked/$name s3://hdfdata/ncep3_chunk_45_180_gzip_9/$name
    rm -f repacked/$name
    rm -f $name
done < "$1"
