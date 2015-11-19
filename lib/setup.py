#!/usr/bin/env python

"""
    This is the main setup script for s3downloader
    
     
"""
from distutils.core import setup

VERSION='0.1'
short_desc="a package for downloading files from S3"
long_desc="A package that provides methods for downloading files from s3 and more"
 
setup(
  name = 's3downlaoder',
  version = VERSION,
  py_modules=[],
  description = short_desc,
  long_description = long_desc,
  author = 'John Readey',
  author_email = 'jreadey at hdfgroup dot org',
  maintainer = 'John Readey',
  maintainer_email = 'jreadey at hdfgroup dot org',
  url = 'https://github.com/HDFGroup/datacontainer',
  download_url = 'https://github.com/HDFGroup/datacontainer',
  packages = ['s3downloader',],
  requires = [ ] 
)

