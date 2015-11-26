#!/usr/bin/env python

import os
import sys
import glob
import h5py

# Where the files to concatenate are...
path = '/mnt/ncep-orig'

# Filename signature...
fname_sig = 'GSSTF_NCEP.3.*.he5'

# Output file with concatenated data...
concat_file = 'GSSTF_NCEP.3.concat.h5'

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
    '/HDFEOS/GRIDS/NCEP/Data Fields/Psea_level',
    '/HDFEOS/GRIDS/NCEP/Data Fields/Qsat',
    '/HDFEOS/GRIDS/NCEP/Data Fields/SST',
    '/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m',
]

#  A hacky assumption here is that the same dataset in all the files is of the
# same shape so the concatenation dimension's size can be set here...
concat_dimsize = len(files)

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
            new_shape = (concat_dimsize,) + orig_dset.shape
            print('Creating dataset', p, 'for concatenated data')
            concat_dsets[p] = big_f.create_dataset(
                p, new_shape, fillvalue=orig_dset.fillvalue,
                dtype=orig_dset.dtype)

            # Copy all attributes from the original dataset...
            for a in orig_dset.attrs:
                concat_dsets[p].attrs[a] = orig_dset.attrs[a]

    for (p, dset) in concat_dsets.items():
        dset[fcount, :, :] = f[p][...]

    # Done with the input data file...
    f.close()

big_f.close()
print('Done! Check out:', os.path.abspath(concat_file))
