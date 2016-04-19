import h5py
import numpy as np
import sys


def main():
    filepath = "/data/ghcn.h5"  # 82 GB filepath
    block_size = 256 * 100
    # ground stations identifiers in Washington State start with this
    prefix = "US1WA"      
    seattle_sta = "US1WAKG0020"
    first_sta = "ITE00100554"
    # following is from row 10000:
    # (b'EZE00100082', b'17751105', b'TMAX', 142, b'', b'', b'E', b'')
    row10k_sta = "EZE00100082"  # 84126 rows
    #query = "(station == b'" + row10k_sta + "') & (obstype == b'TMAX')"  
    #query = "station == b'" + row10k_sta + "'"
    station = row10k_sta.encode("ascii")
    f = h5py.File(filepath, 'r')
    dset = f["/dset"]
    start = 0
    count = 0
    vals = []
    while start < dset.shape[0]:
        end = start  + block_size
        if end > dset.shape[0]:
            end = dset.shape[0]
        rows = dset[start:end]
        where_result = np.where(eval("(rows['station'] == station) & (rows['obstype'] == b'TMAX')"))
        index = where_result[0]
        if len(index) == 0:
            start = end
            continue
        for i in index:
            row = rows[i]
            vals.append(row['obsval'])
            count += 1
            if count < 32:
                print((start+i), row['date'].decode("ascii"), row['obsval'])
            elif count == 32:
                print("...")
                   
        start = end  # go to next block
         
    
    if len(vals) > 0:
    
        arr = np.array(vals, dtype="i4")
        print(len(vals), "rows, min:", arr.min(), "max: ", arr.max(), "stddev: ", arr.std())
    else:
        print("no matches!")
    f.close()
main()
