"""
Generate script of NCEP3 files to download.
Run this outside of OSDC and import resulting file into OSDC
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from six.moves.urllib.request import urlopen

print('export no_proxy="griffin-objstore.opensciencedatacloud.org"\n'
      'function with_proxy() {\n'
      '    PROXY="http://cloud-proxy:3128"\n'
      '    http_proxy="${PROXY}" https_proxy="${PROXY}" ftp_proxy="${PROXY}" $@\n'
      '}\n')

url = 'ftp://measures.gsfc.nasa.gov/ftp/data/s4pa/GSSTF/GSSTF_NCEP.3/'
for year in range(1987, 2000):
    rsp = urlopen(url+str(year))
    page = rsp.read().decode('ascii')
    lines = page.split('\r\n')
    for line in lines:
        line = line.strip()
        fields = line.split(' ')
        num_fields = len(fields)
        filename = fields[-1]
        if not filename.endswith(('.he5', '.xml')):
            continue

        print("with_proxy wget", url+str(year)+'/'+filename)
        print("s3cmd put ", filename, "s3://hdfdata/ncep3/")
        print("rm", filename)
