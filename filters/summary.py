from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import sys
import os
import subprocess
import time
import h5py
import numpy
import argparse
from ipyparallel import Client

file_names = []
downloads = {}
h5path = None
s3cmd_batch_size=2
s3_prefix = "s3://"

def summary(file_path, h5path):

    file_name = os.path.basename(file_path)

    #print("Summary ", file_path, h5path)

    if not h5py.is_hdf5(file_path):
        raise IOError("Not an HDF5 file: " + file_path)
    with h5py.File(file_path, 'r') as f:
        dset = f[h5path]

        # mask fill value
        if '_FillValue' in dset.attrs:
            arr = dset[...]
            fill = dset.attrs['_FillValue'][0]
            v = arr[arr != fill]
        else:
            v = dset[...]
        # file name GSSTF_NCEP.3.YYYY.MM.DD.he5

        return(file_name, len(v), numpy.min(v), numpy.max(v), numpy.mean(v),
              numpy.median(v), numpy.std(v))

def startFileDownload():
    print("start file download")

    s3_cache_dir = os.environ["S3_CACHE_DIR"]
    downloads.clear()

    subprocesses = 0

    for filename in file_names:
        download = {}
        if filename.startswith(s3_prefix):
            if s3_cache_dir is None:
                raise IOError("Environment variable S3_CACHE_DIR not set")
            s3_path = filename[len(s3_prefix):]
            s3_uri = filename
            download["s3_uri"] = s3_uri
            local_filepath = os.path.join(s3_cache_dir, s3_path)
            download["local_filepath"] = local_filepath

            if os.path.exists(local_filepath):
                # todo, check that the s3 object is the same as local copy
                download["state"] = "COMPLETE"
            else:
                if subprocesses < s3cmd_batch_size:
                    # start a new download process
                    p = subprocess.Popen(['s3cmd', 'get', s3_uri, local_filepath])
                    download["subprocess"] = p
                    subprocesses += 1
                    download["state"] = "INPROGRESS"
                else:
                    download["state"] = "PENDING"
        else:
            if os.path.exists(filename):
                download["state"] = "COMPLETE"
                download["local_filepath"] = filename
            else:
                download["state"] = "FAILED"
        downloads[filename] = download

def checkDownloadComplete():
    print("checkDownloadComplete()")
    in_process_count = 0
    queued_items = []
    done = True
    for filename in downloads.keys():
        download = downloads[filename]
        if download["state"] == 'INPROGRESS':
            p = download['subprocess']
            p.poll()
            if p.returncode is None:
                done = False # still waiting on a download
                in_process_count += 1
            elif p.returncode < 0:
                raise IOError("s3cmd failed for " + filename)
            else:
                download["state"] = "COMPLETE"
        elif download["state"] == "PENDING":
             queued_items.append(download)
             done = False

    if len(queued_items) > 0:
        for download in queued_items:
            if in_process_count >= s3cmd_batch_size:
                break # don't start any more subprocesses just yet
            p = subprocess.Popen(['s3cmd', 'get', download["s3_uri"], download["local_filepath"]])
            download["subprocess"] = p
            download["state"] = "INPROGRESS"
            in_process_count += 1

    if done:
        print("download complete!")
    return done

def processFiles():
    print("processFiles()")
    return_values = []

    filenames = list(downloads.keys())
    filenames.sort()
    for filename in filenames:
        download = downloads[filename]
        print(download)
        output = summary(download["local_filepath"], h5path )
        return_values.append(output)
    return return_values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', "--filename", help="name of file or s3 uri")
    parser.add_argument('-i', "--input", help="text file of files or s3 uri")
    parser.add_argument('-p', "--path", help="h5path")
    parser.add_argument('-c', "--cluster", help="cluster profile")
    # example file:
    # public AWS -
    # s3://hdfgroup/data/hdf5test/GSSTF_NCEP.3.2000.05.01.he5
    # OSDC Ceph -
    # s3://hdfdata/ncep3/GSSTF_NCEP.3.2000.05.01.he5

    # example path (for above file):
    # /HDFEOS/GRIDS/NCEP/Data\ Fields/Psea_level


    args = parser.parse_args()

    if not args.filename and not args.input:
        sys.exit("No filename specified!")

    if not args.path:
        sys.exit("No h5path specified!")
    global h5path
    h5path = args.path

    files = []
    if args.input:
        with open(args.input) as f:
            for line in f:
                line = line.strip()
                if not line or line[0] == '#':
                    continue
                file_names.append(line)
    else:
        file_names.append(args.filename)

    rc = None # client interface for cluster mode
    dview = None

    if args.cluster:
        rc = Client()
        if len(rc.ids) == 0:
            sys.exit("No engines found")
        print(len(rc.ids), "engines")
        dview = rc[:]
        dview.block = True # use sync
        # have engines import packages
        with dview.sync_imports():
            import sys
            import os
            import h5py
            import numpy
            import subprocess
        # send the summary method to engines
        dview.push(dict(summary=summary))
        # push the path name
        dview.push(dict(h5path=h5path))
        # push downloaded_files
        dview.push(dict(downloads={}))
        # push batch size
        dview.push(dict(s3cmd_batch_size=5))
        # push s3 s3_prefix
        dview.push(dict(s3_prefix=s3_prefix))

        # split file_names across engines
        dview.scatter('file_names', file_names)

        # start download
        dview.apply(startFileDownload)

        # wait for downloads
        while True:
            # check for download check
            download_complete = dview.apply(checkDownloadComplete)
            if all(download_complete):
                print("downloads complete")
                break
            print(".")
            time.sleep(1)

        print("start processing")
        # run process_files on engines
        output = dview.apply(processFiles)
    else:
        startFileDownload()
        while not checkDownloadComplete():
            time.sleep(1)  # wait for downloads

        output = processFiles() # just run locally

    for elem in output:
        if type(elem) is list:
            #output from engines, break out each tuple
            for item in elem:
                print(item)
        else:
            print(elem)

main()
