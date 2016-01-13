import os
import sys
import time
import itertools
import h5py
import ipyparallel
import tables


def check_file_exists(fname):
    """Check if a file ``fname`` exists and raise an error if it does not."""
    if os.path.isfile(fname):
        return True
    else:
        raise IOError('File %s does not exist' % fname)


def n_at_time(shape, chunk, n=1):
    """Return an iterator that produces a list of``n`` tuples at a time holding
    dataset chunks' first element indices.

    :arg tuple shape: HDF5 dataset's shape.
    :arg tuple chunk: HDF5 dataset's chunk size.
    :arg int n: How many tuples to produce at a time.
    """
    arg = tuple()
    for i in range(len(shape)):
        arg += tuple([range(0, shape[i], chunk[i])])
    args = [itertools.product(*arg)] * n
    return itertools.zip_longest(*args)


def where_func(fname, dpath, val_range):
    """The engine function that runs the "query".

    :arg str fname: HDF5 file name where the data is.
    :arg str dname: Dataset full path.
    :arg dict val_range: Dataset value range expressed via a min and max value.
    """
    import h5py
    import numpy as np

    f = h5py.File(fname, 'r')
    dset = f[dpath]
    chunk_size = dset.chunks

    result = []

    # Go over the chunks and find all their elements that satisfy the query
    # condition...
    for c in chunks:
        chunk_data = dset[c[0]:c[0]+chunk_size[0],
                          c[1]:c[1]+chunk_size[1],
                          c[2]:c[2]+chunk_size[2]]
        idx = np.where((chunk_data <= val_range['max']) &
                       (val_range['min'] <= chunk_data))

        if idx[0].size:
            # Prepare the result set: Convert idx indices back into the
            # dataset's dataspace and include their dataset value...
            for t in map(lambda x, y, z: (x, y, z), idx[0], idx[1], idx[2]):
                result.append(
                    tuple([tuple(map(lambda a, b: a+b, t, c)), chunk_data[t]]))

    return result


class Timing:
    """Store timing information as a series of named checkpoints."""

    def __init__(self):
        self._ckpts = list()

    def checkpoint(self, name):
        """Append a new named checkpoint."""
        self._ckpts.append((name, time.time()))

    @property
    def total(self):
        return len(self._ckpts)

    def as_text(self, delim='\n'):
        if self.total == 0:
            return ''
        out = list()
        ckpts = self._ckpts
        for n, (what, t) in enumerate(ckpts[1:], 1):
            out.append('{:s}={:f}'.format(what, t-ckpts[n-1][1]))
        return delim.join(out)

    def as_csv(self):
        if self.total == 0:
            return ''
        heading = list()
        values = list()
        for n, (what, t) in enumerate(self._ckpts[1:], 1):
            heading.append(what)
            values.append(str(t-self._ckpts[n-1][1]))
        return ','.join(heading) + '\n' + ','.join(values)

    def total_elapsed(self):
        tot = 0
        if self.total > 0:
            for n, (what, t) in enumerate(self._ckpts[1:], 1):
                tot += t-self._ckpts[n-1][1]
        return tot


#
# Main program
#

# Data file...
data_fname = '/mnt/GSSTF_NCEP.3.concat.25x20x20.gzip9.h5'
check_file_exists(data_fname)

# Name of file holding index data of the data file...
index_fname = '/mnt/GSSTF_NCEP.3.concat.25x20x20.index.h5'
check_file_exists(index_fname)

# Data value range...
val_range = {'min': 21, 'max': 23}

rc = ipyparallel.Client()
num_eng = len(rc.ids)
if num_eng == 0:
    sys.exit('No cluster engines found')
else:
    print('Using', num_eng, 'cluster engines')

# Obtain DirectView to all engines...
dv = rc[:]

# Variable for storing timing info...
timing = Timing()

timing.checkpoint('start')

f = h5py.File(data_fname, 'r')
dset_name = '/HDFEOS/GRIDS/NCEP/Data Fields/Tair_2m'
dset = f[dset_name]
shape = dset.shape
chunk = dset.chunks
f.close()

# Calculate the indices of first elements for all chunks (this can be a very
# large list)...
arg = tuple()
for i in range(len(shape)):
    arg += tuple([range(0, shape[i], chunk[i])])
chunks = list(itertools.product(*arg))

timing.checkpoint('prep time')

#
# Scenario #1:
#
#  Find all elements (grid cells) of a HDF5 dataset in the data file with
#  values that satisfy a certain condition.
#

# Brute force method: Use all the chunks

# Partition the chunks list over all engines...
dv.scatter('chunks', chunks)

timing.checkpoint('preload data')
res = dv.apply_sync(where_func, data_fname, dset_name, val_range)
timing.checkpoint('processing')

# Combine the results from all the engines which are in a list of lists into a
# single record...
res = list(itertools.chain.from_iterable(res))
print('\nBrute force method')
print('Found %d grid cells that satisfy the query criteria' % len(res))

timing.checkpoint('report')

print('\nTiming information:')
print(timing.as_text())
print('Total runtime:', timing.total_elapsed())

# Indexed method: Use only the chunks with the data that satisfy the query
# condition...
timing = Timing()
timing.checkpoint('start')
f = tables.open_file(index_fname, 'r')
table = f.get_node(dset_name)

# Query condition (the "where" statement for database folks)...
query = '(min_val <= {max:g}) & ({min:g} <= max_val)'.format(**val_range)

# Query the index data and retrieve grid_t_start, grid_lat_start, and
# grid_lon_start values in a tuple, then produce a list of such tuples...
chunks = [r[1:6:2] for r in table.where(query)]
f.close()
timing.checkpoint('prep time')
dv = rc[:]
dv.scatter('chunks', chunks)
timing.checkpoint('preload data')
res = dv.apply_sync(where_func, data_fname, dset_name, val_range)
timing.checkpoint('processing')
res = list(itertools.chain.from_iterable(res))
print('\nBlock range index method')
print('Found %d grid cells that satisfy the query criteria' % len(res))
timing.checkpoint('report')
print('\nTiming information:')
print(timing.as_text())
print('Total runtime:', timing.total_elapsed())
