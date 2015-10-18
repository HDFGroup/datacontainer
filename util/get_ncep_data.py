from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

url = 'ftp://measures.gsfc.nasa.gov/ftp/data/s4pa/GSSTF/GSSTF_NCEP.3/'
for year in range(2000, 2009):
    print('year:', year)
    # req = urllib2.Request(url+str(year))
    # rsp = urlopen(req)
    rsp = urlopen(url+str(year))
    page = rsp.read().decode('ascii')
    lines = page.split('\r\n')
    for line in lines:
        line = line.strip()
        fields = line.split(' ')
        num_fields = len(fields)
        filename = fields[-1]
        print(filename)
