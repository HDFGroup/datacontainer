# How to make Blosc filter work with HDF5
 

Instructions for installing blosc 

* Update CMakeFile.txt to include blosc_plugin.h and blosc_plugin.c. 
* Use hdf5-1.8.15-patch1. I tried with hdf5-1.8.16 but it did not wok on Mac but worked on Ubuntu.
* Install blosc library from source. Mac's port version of blosc did not seem to work. blosc-hdf5 downloads and installs blosc library through CMake on Ubuntu. 
* Make sure that HDF5_PLUGIN_PATH is correct. 
* Use gdb to make sure that the correct HDF5 library is being loaded. That's the only way to debug. A slight version mismatch in HDF5 library will not lead to anything - no debugging message, etc. This is very difficult to notice if you use Anaconda's environment that loads h5py's HDF5 library. 
* Enable BLOSC_DEBUG in blosc-hdf5 if you're in doubt. Yet, it doesn't give enough error message at blosc library level if something fails inside blosc library.  You can add debugging info by add_definitions( -DBLOSC_DEBUG) in the CMakeLists.txt.
