import sys
import h5py
import numpy as np


 
            
def add_lines(dset, lines):
    num_lines = len(lines)
    if num_lines == 0:
       return
    numrows = dset.shape[0]
    dset.resize(((numrows+num_lines),))
    rows = dset[numrows:(numrows+num_lines)]
    
    for i in range(num_lines): 
        line = lines[i]
        row = rows[i]
        fields = line.split(',')
    
        if len(fields) != 8:
              raise IOError("invalid line:" + line)
        for j in range(8):
            if j == 3:
                  # convert to int!
                  row[3] = int(fields[j])
            else:
                  row[j] = fields[j] 
    dset[numrows:(numrows+num_lines)] = rows
    

def main():
    if len(sys.argv) <= 1:
       print("convert_ghcn <filename>")
       sys.exit(-1)
    f = h5py.File("ghcn.h5", 'a')
    if "dset" not in f:
        dt = np.dtype([('station', 'S11'), ('date', 'S8'), ('obstype', 'S4'), ('obsval', 'i4'), ('code1', 'S1'), ('code2', 'S1'), ('code3', 'S1'), ('obstime', 'S4')])   
        dset = f.create_dataset("dset", (0,), dtype=dt, maxshape=(None,))
    else:
        dset = f['dset']
    
    batch_size = 100
    lines = []
    filename = sys.argv[1]
    with open(filename, "r") as g:
        for line in g:
            line = line.strip()
            lines.append(line)
            print(line)
            if len(lines) == batch_size:
                add_lines(dset, lines)
                lines = []
                
    add_lines(dset, lines)
                    
    f.close()                   
    
    
main()
