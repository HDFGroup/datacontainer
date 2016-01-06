import sys
import os
import h5py
import numpy as np
            
def add_lines(dset, line):
     
    if not line:
        return # skip
            
    numrows = dset.shape[0]
    dset.resize(((numrows+1),))
     
    row = dset[numrows]
    text =  line.strip()
    print(numrows, ':', text)
    
    # padd out any shorter than 85 char lines
    if len(text) < 85:
        padd = 85 - len(text)
        for i in range(padd):
            text += ' '
    
    row[0] = text[:11]          # station_id
    row[1] = float(text[13:20]) # lattitude
    row[2] = float(text[21:30]) # longitude
    row[3] = float(text[31:37]) # elevation
    row[4] = text[38:40]        # state
    row[5] = text[41:71]        # name
    row[6] = text[72:75]        # gsn_flag
    row[7] = text[76:79]        # hcn_flag
    row[8] = text[80:85]        # wmo_id
        
    dset[numrows] = row
    

def main():
    if len(sys.argv) <= 2:
       print("add_stations <ghcn_h5> <ghcnd-stations>")
       sys.exit(-1)
    if not os.path.isfile(sys.argv[1]):
       print("file not found")
       sys.exit(-1)
    outfile_name = sys.argv[1] 
    f = h5py.File(outfile_name, 'a')
    if "stations" not in f:
        dt = np.dtype([('id', 'S11'), ('latitude', 'f4'), ('longitude', 'f4'), ('elevation', 'f4'), ('state', 'S2'), ('name', 'S30'), ('gsn_flag', 'S3'), ('hcn_flag', 'S3'), ('wmo_id', 'S5')])   
        dset = f.create_dataset("stations", (0,), dtype=dt, maxshape=(None,))
    else:
        dset = f['stations']
    
    lines = []
    filename = sys.argv[2]
    with open(filename, "r") as g:
        for line in g:
            line = line.strip() 
            add_lines(dset, line)
                    
    f.close()   
    print("saved to: ", outfile_name)                
    
    
main()
