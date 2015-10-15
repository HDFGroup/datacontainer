import sys
import os

import boto
import boto.s3.connection

s3_prefix = "s3://"

"""
Given a s3 URI, return a filepath to a previously
cached version of the object (if previously downloaded).
If not, download the object, save to local cache and return
path to cached file. 
"""
def getS3Object(s3_uri):
    if not s3_uri.startswith(s3_prefix):
        raise IOError("not a s3 uri")
    
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]
    s3_gateway = os.environ["S3_GATEWAY"]
    s3_cache_dir = os.environ["S3_CACHE_DIR"]
    # for AWS S3 set to: s3.amazonaws.com
                      
    s3_path = s3_uri[len(s3_prefix):]
    index = s3_path.index('/')
    if index < 1:
        raise IOError("invalid s3_path: " + s3_uri)
    bucket_name = s3_path[:index] 
    obj_key = s3_path[(index+1):]
    
    local_filepath = os.path.join(s3_cache_dir, s3_path)
    
    if os.path.exists(local_filepath):
        # todo, check that the s3 object is the same as local copy
        return local_filepath
        
    # create a S3 connection
    conn = boto.connect_s3(
        aws_access_key_id = s3_access_key,
        aws_secret_access_key = s3_secret_key,
        host = s3_gateway,
        is_secure=False,
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )
    dirname = os.path.dirname(local_filepath)
    try:
        # create directories if needed to store
        # cached file
        os.makedirs(dirname)
    except OSError:
        pass  # ignore
        
    bucket = conn.get_bucket(bucket_name)
    key = bucket.get_key(obj_key)
    key.get_contents_to_filename(local_filepath)
    return local_filepath
     
  
