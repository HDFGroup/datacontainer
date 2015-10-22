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

echo "copy engine json from s3"
s3cmd get --force  s3://hdfgroup/.ipython/profile_$profile/security/$ENGINE_JSON $PROFILE_DIR/$ENGINE_JSON
if [ $? != 0 ]
then
  echo "no engine file exists (controller not running?)"
  exit 1
fi

echo "starting ipengine"
ipengine 
