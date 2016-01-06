import sys
import os
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
    if not os.path.isfile(sys.argv[1]):
       print("file not found")
       sys.exit(-1)
    outfile_name = sys.argv[1]
    if not outfile_name.endswith(".csv"):
        print("expected filename to have .csv suffix")
        sys.exit(-1)
    outfile_name = outfile_name[:-4] + ".h5"
     
    f = h5py.File(outfile_name, 'a')
    if "dset" not in f:
        dt = np.dtype([('station', 'S11'), ('date', 'S8'), ('element', 'S4'), ('value', 'i4'), ('mflag', 'S1'), ('qflag', 'S1'), ('sflag', 'S1'), ('obstime', 'S4')])   
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
    print("saved to: ", outfile_name)                
    
    
main()
