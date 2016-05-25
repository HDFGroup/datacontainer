import h5py
import numpy as np
import sys

"""
Get Global max temperature by year
"""

def main():
    #filepath = "/data/ghcn.h5"  # 82 GB filepath
    #filepath = "/Volumes/work/data/NOAA/ghcn/ghcn_h5/ghcn.h5"
    filepath = "/Volumes/work/data/NOAA/ghcn/ghcn_h5/1763.h5"
     
    block_size = 256 * 100
    # ghcn datatype:
    # "station": str(11) - station code
    # "date": str(8) - date as YYYYMMDD
    # "element": str(4) - obs type
    # "value": I32 - obs value
    # "qflag" str(1) - q flag
    # "qflag" str(1) - q flag
    # "qflag" str(1) - q flag
    # "obstime" str(4) - 24 hr time
     
    # following is from row 10000:
    # (b'EZE00100082', b'17751105', b'TMAX', 142, b'', b'', b'E', b'')
     
    f = h5py.File(filepath, 'r')
    dset = f["/dset"]
    start = 0
    count = 0
    vals = {}
    while start < dset.shape[0]:
        end = start + block_size
        if end > dset.shape[0]:
            end = dset.shape[0]
        rows = dset[start:end]
        numlines = end - start
        for i in range(numlines):
            row = rows[i]
            
            if row['element'] != b'TMAX':
                continue
            #print(row)
            year = int(row['date'][:4])
               
            obsval = row['value']
            if obsval == -9999:
                continue
            if year not in vals:
                vals[year] = obsval
            elif obsval > vals[year]:
                vals[year] = obsval
        start = end  # go to next block
    f.close()
    
    print("done!")
    years = list(vals.keys())
    if len(years) == 0:
        print("no data found!")
    else:
        years.sort()
        yearstart = years[0]
        yearend = years[-1]
        for year in range(yearstart, yearend+1):
            if year in vals:
                print("{0:4d}: {1:4d}".format(year, vals[year]))
            else:
                print("{0:4d}: MISSING".format(year))
        
main()
