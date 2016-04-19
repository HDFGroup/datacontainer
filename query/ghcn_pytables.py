import h5py
import tables
import numpy as np
import numexpr as ne
import sys


def main():
    filepath = "/data/ghcn.h5"  # 82 GB filepath
    # ground stations identifiers in Washington State start with this
    prefix = "US1WA"      
    seattle_sta = "US1WAKG0020"
    first_sta = "ITE00100554"
    # following is from row 10000:
    # (b'EZE00100082', b'17751105', b'TMAX', 142, b'', b'', b'E', b'')
    row10k_sta = "EZE00100082"  # 84126 rows
    query = "(station == b'" + row10k_sta + "') & (obstype == b'TMAX')"  
    f = tables.open_file(filepath, 'r')
    dset = f.root._f_get_child("/dset")
    count = 0
    vals = []
    for row in dset.where(query):
        if count < 32:
            print(row.nrow, row['date'].decode("ascii"), row['obsval'])
        elif count == 32:
            print("...")
        vals.append(row["obsval"])
        count += 1
    
    if len(vals) > 0:
    
        arr = np.array(vals, dtype="i4")
         
        print(len(vals), "rows, min:", arr.min(), "max: ", arr.max(), "stddev: ", arr.std())
    else:
        print("no matches!")
    f.close()
main()
