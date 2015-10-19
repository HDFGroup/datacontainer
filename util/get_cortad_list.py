#!/usr/bin/env python
"""
Generate a bash script commands for downloading CoRTAD data to OSDC Griffin
cluster. Run this outside of OSDC and import resulting file into OSDC.
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from six.moves.urllib.request import urlopen

print('#!/usr/bin/env bash\n')
print('export no_proxy="griffin-objstore.opensciencedatacloud.org"\n'
      'function with_proxy() {\n'
      '    PROXY="http://cloud-proxy:3128"\n'
      '    http_proxy="${PROXY}" https_proxy="${PROXY}" ftp_proxy="${PROXY}" $@\n'
      '}\n')

url = 'ftp://ftp.nodc.noaa.gov/pub/data.nodc/cortad/Version5/'
rsp = urlopen(url)
page = rsp.read().decode('ascii')
lines = page.split('\r\n')
for line in lines:
    line = line.strip()
    fields = line.split(' ')
    num_fields = len(fields)
    filename = fields[-1]
    if not filename.endswith('.nc'):
        continue

    print("with_proxy wget", url+filename)
    print("s3cmd -v put ", filename, "s3://hdfdata/cortad/")
    print("rm -v", filename)
