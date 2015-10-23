#!/bin/sh
profile="default"

while [ "$1" != "" ]
do
  case $1 in
  	-p | --profile )     shift
                             profile=$1
                             ;;
    -h | --help )       echo "Usage: ipcontroller.sh [-p <profile>]"
                        exit
                        ;; 
  esac
  shift
done

PROFILE_DIR=$HOME/.ipython/profile_$profile/security
ENGINE_JSON=ipcontroller-engine.json

echo "checking:" $PROFILE_DIR/$ENGINE_JSON

if [ ! -e $PROFILE_DIR/$ENGINE_JSON ]
then
   echo "engine file not found  (did you run 'ipcontroller --ip=*'?)"
   exit 1
fi
 
echo "copy engine json to s3"
s3cmd put --force $PROFILE_DIR/$ENGINE_JSON s3://hdfgroup/.ipython/profile_$profile/security/$ENGINE_JSON
echo "done!"
