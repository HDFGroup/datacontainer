#!/bin/sh
while [ "$1" != "" ]
do  
  case $1 in
    -c | --count ) shift
                   count=$1
                   ;;
    -h | --help ) echo "Usage: run_engine.sh [ -c <num_vms> ]"
                  exit
                  ;;
  esac
  shift
done
echo "count:" $count
if [ "$count" != "" ]
then
   nova boot --flavor m1.small --image ipengine --key-name osdc_keypair --num-instances $count ipengine
else
   nova boot --flavor m1.small --image ipengine --key-name osdc_keypair ipengine
fi
