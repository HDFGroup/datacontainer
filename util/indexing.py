#!/usr/bin/env python

"""
fdfddfdfd
"""

import os
import sys
import glob
import itertools
import tables
import numpy as np
import h5py


class Index2d(tables.IsDescription):
    """Table schema for index information for 2D datasets."""
    fname = tables.StringCol(30)
    grid_lat_start = tables.UInt32Col()
    grid_lat_end = tables.UInt32Col()
    grid_lon_start = tables.UInt32Col()
    grid_lon_end = tables.UInt32Col()
    min_val = tables.Float64Col()
    max_val = tables.Float64Col()


class Index3d(tables.IsDescription):
    """Table schema for index information for 3D datasets."""
    fname = tables.StringCol(30)
    grid_t_start = tables.UInt32Col()
    grid_t_end = tables.UInt32Col()
    grid_lat_start = tables.UInt32Col()
    grid_lat_end = tables.UInt32Col()
    grid_lon_start = tables.UInt32Col()
    grid_lon_end = tables.UInt32Col()
    min_val = tables.Float64Col()
    max_val = tables.Float64Col()

# Which indexing class (schema) to use...
Index = Index2d

# Where the files to index are...
path = '/Users/ajelenak/Downloads'

# Filename signature of data to index...
fname_sig = 'GSSTF_NCEP.3.*.he5'

# Let's find some files...
files = glob.glob(os.path.join(path, fname_sig))
if len(files) == 0:
    print('No %s files found in %s' % (fname_sig, path))
    sys.exit()

# Which HDF5 datasets' data to index...
h5path = [
    '/HDFEOS/GRIDS/NCEP/Data Fields/Psea_level',
    '/HDFEOS/GRIDS/NCEP/Data Fields/Qsat',
    '/HDFEOS/GRIDS/NCEP/Data Fields/SST',
    '/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m',
]

# File that holds the index...
index_file = 'GSSTF_NCEP.3.index.h5'

# Create the index file...
idx_f = tables.open_file(index_file, mode='w',
                         title='Index file for %s' % fname_sig)

# Expected number of table rows (important for optimal performance)...
num_rows = len(files)

# Create tables for all indices...
tabl = dict()
for d in h5path:
    tabl['d'] = idx_f.create_table(os.path.dirname(d),
                                   os.path.basename(d),
                                   Index,
                                   title='Index table for ' + d,
                                   expectedrows=num_rows, createparents=True)

for fname in files:
    # Open the input file...
    print('Processing', fname)
    in_f = h5py.File(fname, 'r')
    just_fname = os.path.basename(fname)

    for p in h5path:
        # Get the dataset and its chunk size...
        dset = in_f[p]
        print('Processing dataset', dset.name)
        chunk = dset.chunks
        if chunk is None:
            sys.exit(dset.name + ' Dataset not chunked')

        # Pull data for each chunk, collect descriptive stats, and store in the
        # table...
        shape = dset.shape
        args = tuple()
        for n in range(len(shape)):
            args += list(range(0, shape[n], chunk[n]))
        for s in itertools.product(*args):
            e = tuple([i + j for i, j in zip(s, chunk)])
            chunk_data = dset[s[0]:e[0], s[1]:e[1]]

            # Store indexing info into the table...
            row = tabl[p].row
            row['fname'] = just_fname
            row['grid_lat_start'] = s[0]
            row['grid_lat_end'] = e[0]
            row['grid_lon_start'] = s[1]
            row['grid_lon_end'] = e[1]
            row['min_val'] = np.min(chunk_data)
            row['max_val'] = np.max(chunk_data)

        # Flush the table...
        tabl[p].flush()

idx_f.close()
print('Done! Check out:', os.path.abspath(index_file))
