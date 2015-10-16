"""
Generate script of NCEP3 files to download.
Run this outside of OSDC and import resulting file into OSDC
"""

import urllib2

print 'export no_proxy="griffin-objstore.opensciencedatacloud.org"'
print 'function with_proxy() {'
print '    PROXY="http://cloud-proxy:3128"'
print '    http_proxy="${PROXY}" https_proxy="${PROXY}" ftp_proxy="${PROXY}" $@'
print '}'

url = 'ftp://measures.gsfc.nasa.gov/ftp/data/s4pa/GSSTF/GSSTF_NCEP.3/'
#for year in range(2000, 2009):
for year in range(1987, 2000):
    req = urllib2.Request(url+str(year))
    rsp = urllib2.urlopen(req)
    page = rsp.read()
    lines = page.split('\n')
    for line in lines:
        line = line.strip()
        fields = line.split(' ')
        num_fields = len(fields)
        filename = fields[-1]
        if not filename.endswith(".he5") and not filename.endswith(".xml"):
            continue
            
        print "with_proxy wget", url+str(year)+'/'+filename
        print "s3cmd put ", filename, "s3://hdfdata/ncep3/"
        print "rm", filename
        
