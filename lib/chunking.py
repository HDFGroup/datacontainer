"""
A collection of different algorithms that claim to calculate "optimal" chunk
size.
"""

import math
import numpy as np

# Hard upper limit for chunk size (1MiB)
CHUNK_MAX = 1024*1024


"""
Unidata Algorithm

Claims to provide optimized chunking of 3d variables for balanced 1d and 2d
access.

The code fetched from:
    http://www.unidata.ucar.edu/staff/russ/public/chunk_shape_3D.py
"""
def unidata_chunk(varShape, valSize=4, chunkSize=CHUNK_MAX):
    """
    Return a 'good shape' for a 3D variable, assuming balanced 1D, 2D access

    varShape  -- length 3 list of variable dimension sizes
    chunkSize -- maximum chunksize desired, in bytes (default 1MiB)
    valSize   -- size of each data value, in bytes (default 4)

    Returns integer chunk lengths of a chunk shape that provides
    balanced access of 1D subsets and 2D subsets of a netCDF or HDF5
    variable var with shape (T, X, Y), where the 1D subsets are of the
    form var[:,x,y] and the 2D slices are of the form var[t,:,:],
    typically 1D time series and 2D spatial slices.  'Good shape' for
    chunks means that the number of chunks accessed to read either
    kind of 1D or 2D subset is approximately equal, and the size of
    each chunk (uncompressed) is no more than chunkSize, which is
    often a disk block size.
    """
    def binlist(n, width=0):
        """Return list of bits that represent a non-negative integer.

        n      -- non-negative integer
        width  -- number of bits in returned zero-filled list (default 0)
        """
        return map(int, list(bin(n)[2:].zfill(width)))

    def numVals(shape):
        """Return number of values in chunk of specified shape, given by a list
        of dimension lengths.

        shape -- list of variable dimension sizes"""
        if(len(shape) == 0):
            return 1
        return np.prod(shape)

    def perturbShape(shape, onbits):
        """Return shape perturbed by adding 1 to elements corresponding to 1
        bits in onbits

        shape  -- list of variable dimension sizes
        onbits -- non-negative integer less than 2**len(shape)
        """
        return list(map(sum, zip(shape, binlist(onbits, len(shape)))))

    rank = 3  # this is a special case of n-dimensional function chunk_shape
    if len(varShape) != rank:
        raise ValueError('Variable shape must be 3-dimensional: %s' % varShape)
    chunkVals = chunkSize / float(valSize) # ideal number of values in a chunk
    numChunks  = varShape[0]*varShape[1]*varShape[2] / chunkVals  # ideal number of chunks
    axisChunks = numChunks ** 0.25 # ideal number of chunks along each 2D axis
    cFloor = [] # will be first estimate of good chunk shape
    # cFloor  = [varShape[0] // axisChunks**2, varShape[1] // axisChunks, varShape[2] // axisChunks]
    # except that each chunk shape dimension must be at least 1
    # chunkDim = max(1.0, varShape[0] // axisChunks**2)
    if varShape[0] / axisChunks**2 < 1.0:
        chunkDim = 1.0
        axisChunks = axisChunks / math.sqrt(varShape[0]/axisChunks**2)
    else:
        chunkDim = varShape[0] // axisChunks**2
    cFloor.append(chunkDim)
    prod = 1.0  # factor to increase other dims if some must be increased to 1.0
    for i in range(1, rank):
        if varShape[i] / axisChunks < 1.0:
            prod *= axisChunks / varShape[i]
    for i in range(1, rank):
        if varShape[i] / axisChunks < 1.0:
            chunkDim = 1.0
        else:
            chunkDim = (prod*varShape[i]) // axisChunks
        cFloor.append(chunkDim)

    # cFloor is typically too small, (numVals(cFloor) < chunkSize)
    # Adding 1 to each shape dim results in chunks that are too large,
    # (numVals(cCeil) > chunkSize).  Want to just add 1 to some of the
    # axes to get as close as possible to chunkSize without exceeding
    # it.  Here we use brute force, compute numVals(cCand) for all
    # 2**rank candidates and return the one closest to chunkSize
    # without exceeding it.
    bestChunkSize = 0
    cBest = cFloor
    for i in range(8):
        # cCand = map(sum,zip(cFloor, binlist(i, rank)))
        cCand = perturbShape(cFloor, i)
        thisChunkSize = valSize * numVals(cCand)
        if bestChunkSize < thisChunkSize <= chunkSize:
            bestChunkSize = thisChunkSize
            cBest = list(cCand)  # make a copy of best candidate so far
    return tuple(map(int, cBest))


"""
h5py Algorithm

From: https://github.com/h5py/h5py/blob/master/h5py/_hl/filters.py#L252

Used when dataset to be created requires chunking but the user has not provided
chunk shape.
"""
def h5py_chunk(shape, typesize=4, chunk_size=CHUNK_MAX):
    """Guess an appropriate chunk layout for a dataset given its shape, the
    size of each element in bytes (default 4) and chunk size in bytes (default
    1MiB). Chunks are generally close to some power-of-2 fraction of each axis,
    slightly favoring bigger values for the last index. Undocumented and
    subject to change without warning.
    """
    CHUNK_BASE = 16*1024    # Multiplier by which chunks are adjusted
    CHUNK_MIN = 8*1024      # Soft lower limit (8KiB)

    # For unlimited dimensions we have to guess 1024
    shape = tuple((x if x != 0 else 1024) for i, x in enumerate(shape))

    ndims = len(shape)
    if ndims == 0:
        raise ValueError("Chunks not allowed for scalar datasets.")

    chunks = np.array(shape, dtype='=f8')
    if not np.all(np.isfinite(chunks)):
        raise ValueError("Illegal value in chunk tuple")

    # Determine the optimal chunk size in bytes using a PyTables expression.
    # This is kept as a float.
    dset_size = np.product(chunks)*int(typesize)
    target_size = CHUNK_BASE * (2**np.log10(dset_size/(1024.*1024)))

    if target_size > chunk_size:
        target_size = chunk_size
    elif target_size < CHUNK_MIN:
        target_size = CHUNK_MIN

    idx = 0
    while True:
        # Repeatedly loop over the axes, dividing them by 2.  Stop when:
        # 1a. We're smaller than the target chunk size, OR
        # 1b. We're within 50% of the target chunk size, AND
        #  2. The chunk is smaller than the maximum chunk size

        chunk_bytes = np.product(chunks)*int(typesize)

        if ((chunk_bytes < target_size
                or abs(chunk_bytes-target_size)/target_size < 0.5)
                and chunk_bytes < chunk_size):
            break

        if np.product(chunks) == 1:
            break  # Element size larger than chunk_size

        chunks[idx % ndims] = np.ceil(chunks[idx % ndims] / 2.0)
        idx += 1

    return tuple(int(x) for x in chunks)
