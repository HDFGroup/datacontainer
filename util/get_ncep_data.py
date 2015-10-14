import urllib2
url = 'ftp://measures.gsfc.nasa.gov/ftp/data/s4pa/GSSTF/GSSTF_NCEP.3/'
for year in range(2000, 2009):
    print 'year:', year
    req = urllib2.Request(url+str(year))
    rsp = urllib2.urlopen(req)
    page = rsp.read()
    lines = page.split('\n')
    for line in lines:
        line = line.strip()
        fields = line.split(' ')
        num_fields = len(fields)
        filename = fields[-1]
        print filename
