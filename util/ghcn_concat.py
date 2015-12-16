#!/usr/bin/env python

import os
import sys
import glob
import h5py

# Where the files to concatenate are...
path = '/mnt/ghcn/'

# Filename signature...
fname_sig = '*.h5'

# Output file with concatenated data...
concat_file = 'ghcn.h5'

# Let's find some files...
files = glob.glob(os.path.join(path, fname_sig))
if len(files) == 0:
    print('No %s files found in %s' % (fname_sig, path))
    sys.exit()

# Sort file names (This is a hacky approach to ensure the data is concatenated
# correctly. Perhaps it will need to be checked and changed for each new case.)
files = sorted(files)

# Which HDF5 datasets's data to concatenate...
h5path = [
    '/dset',
]

# Create the HDF5 file that will hold concatenated data...
big_f = h5py.File(concat_file, 'w')

# Concatenate...
first_pass = True
for fcount, fname in enumerate(files):
    print('Processing', fname)
    f = h5py.File(fname, 'r')

    # If first_pass is True, create new datasets that will hold concatenated
    # data...
    if first_pass:
        first_pass = False
        concat_dsets = dict()
        for p in h5path:
            # Get some information from the dataset in the first file...
            orig_dset = f[p]

            # Create the dataset for concatenated data...
            print('Creating dataset', p, 'for concatenated data')
            concat_dsets[p] = big_f.create_dataset(
                p, (0,), fillvalue=orig_dset.fillvalue,
                dtype=orig_dset.dtype, maxshape=(None,))

            # Copy all attributes from the original dataset...
            for a in orig_dset.attrs:
                concat_dsets[p].attrs[a] = orig_dset.attrs[a]

    for (p, dset) in concat_dsets.items():
        src_dset = f[p]
        if len(src_dset.shape) != 1:
            raise IOError("can concat multi-dimensional dataset!")
        num_elements = src_dset.shape[0]
        # resize the dataset
        numrows = dset.shape[0]
        dset.resize(((numrows+num_elements),))
        #rows = dset[numrows:(numrows+num_lines)]
        print("num_elements:", num_elements)
        print("numrows:", numrows)
        
        dset[numrows:(numrows+num_elements)] = src_dset[...]

    # Done with the input data file...
    f.close()

big_f.close()
print('Done! Check out:', os.path.abspath(concat_file))
