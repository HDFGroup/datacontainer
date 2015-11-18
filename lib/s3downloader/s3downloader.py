import sys
import subprocess
import os
import time
import logging
import shutil

def init():
    s3 = S3Download()
    globals()['s3'] = s3
    
def freespace():
    # global s3
    return s3.freespace
    
def usedspace():
    return s3.usedspace
    
def clear(remove_all=False):
    s3.clear()
    
def s3cmdls(s3uri):
    return s3.cmdls(s3uri)
    
def addFiles(s3uris):
    s3.addFiles(s3uris)
    
def getFiles(state=None):
    return s3.getFiles(state=state)

def update():
    return s3.update()
    
def start():
    return s3.start()
    
def dump():
    return s3.dump()
    
"""
Class definition
"""

class S3Download:

    def __init__(self):
        self.home_dir = os.environ["HOME"]
        self.log = logging.getLogger("s3download")
        self.log.setLevel(logging.INFO)
        handler = logging.FileHandler(os.path.join(self.home_dir,
                                      "s3download.log"))
        self.log.addHandler(handler)
        self.downloads = {}

        self.s3_prefix = "s3://"
        self.s3cmd_batch_size = 2

        # Hardcode the destination for downloaded files
        self.s3_dir = os.environ["S3_CACHE_DIR"]
        if not os.path.isdir(self.s3_dir):
            raise IOError(self.s3_dir + ": directory does not exist")

        # Make sure the s3_dir is owned by this process' uid...
        if os.stat(self.s3_dir).st_uid != os.getuid():
            subprocess.check_call(
                ['sudo', 'chown', '-R', 'ubuntu:ubuntu', self.s3_dir])

        self.log.info("s3_dir: " + self.s3_dir)

    @property
    def freespace(self):
        """Get the amount of free space available.
        """
        self.log.info("freespace")
        freebytes = shutil.disk_usage(self.s3_dir).free
        self.log.info("returning:" + str(freebytes))
        return freebytes

    @property
    def usedspace(self):
        """Get the number of bytes used in the s3 download directory.
        """
        return shutil.disk_usage(self.s3_dir).used

    def rmrf(self, pdir):
        """ Remove all files in the given directory.
           caution recursive delete - use with care!

        :arg str pdir: directory to remove files from
        """
        self.log.info("rmrf({})".format(pdir))
        if pdir is None:
            raise IOError("rmrf() called with non-valid path")
        if not os.path.isdir(pdir):
            raise IOError("rmrf() called with non-directory path")
        # sanity check, so we don't got erasing files outside the s3_dir...
        if not pdir.startswith(self.s3_dir):
            raise IOError("Invalid path: " + pdir)
            
        contents = os.listdir(pdir)
        for name in contents:
            path = os.path.join(pdir, name)
            if os.path.isdir(path):
                # remove directory tree
                shutil.rmtree(path)
            else:
                # remove a file
                os.remove(path)
    

    def clear(self, remove_all=False):
        """ Remove files from s3 download directory, clears download queue
        """
        self.log.info("clear")
        
        for download in self.downloads:
            filepath = download['local_filepath']
            if os.path.isfile(filepath):
                os.remove(filepath)
        self.downloads.clear()
        if remove_all:
            # delete directories
             self.rmrf(self.s3_dir) 
            

    def cmdls(self, s3uri):
        """ List s3 objects

        :arg str s3uri: s3 uri
        """
        self.log.info("cmdls: " + s3uri)
        s3cmd = subprocess.Popen(["s3cmd", "ls", s3uri],
                                 stdout=subprocess.PIPE)
        output = s3cmd.communicate()[0]
        text = output.decode("utf-8")  # convert to string
        lines = text.split('\n')
        s3ls = []
        if len(lines) == 0:
            return s3ls  # empty list
        for line in lines:
            line = line.strip()
            print("line:", line)
            if line:
                fields = line.split()
                # expecting something like:
                # 2015-10-23 07:44  16631632 s3://hdfdata/ncep3/GSSTF_NCEP.3.1987.07.01.he5
                if len(fields) == 2 and fields[0] == "DIR":
                    # ignore dirs
                    continue
                if len(fields) != 4:
                    raise IOError("Unexpected output from s3cmd: " + line)
                s3ls_output = {'date': fields[0], 'time': fields[1], 'size': int(fields[2]), 'uri': fields[3]}
                s3ls.append(s3ls_output)
        self.log.info("cmdls returning")
        return s3ls
        
    def getFiles(self, state=None, s3uri_prefix=None):
        """Get list of files in the download queue.
           if state is provided, returns list of files in given state:
               PENDING|INPROGRESS|COMPLETE|FAILED
        """
            
        s3_uris = []
        keys = list(self.downloads.keys())
        keys.sort()
        for key in keys:
            download = self.downloads[key]
            if not state or (state and download['state'] == state):
                print(download)
                s3_uris.append(download['s3_uri'])
                
        return s3_uris
        
     

    def addFiles(self, s3uris):
        """add objects to download list.

        :arg list s3uris: can be folder path, or list of object uri's
        """
        if type(s3uris) is str:
            # convert to one element list
            s3uris = [s3uris]

        for s3_uri in s3uris:
            self.log.info("addFiles: " + s3_uri)
            if not s3_uri.startswith(self.s3_prefix):
                raise IOError("Invalid s3 uri: %s" % s3_uri)
            s3ls_out = self.cmdls(s3_uri)
            if len(s3ls_out) == 0:
                raise IOError("no s3 objects found for " + s3_uri)
            for output in s3ls_out:
                s3_item = output['uri']
                self.log.info("got item: " + s3_item)
                if s3_item in self.downloads:
                    self.log.info(s3_item + " already added")
                    continue
                 
                 
                s3_path = s3_item[len(self.s3_prefix):]
                self.log.info("s3_path:" + s3_path)
                local_filepath = os.path.join(self.s3_dir, s3_path)

                download = {}
                download['s3_uri'] = s3_item
                download['s3_date'] = output['date']
                download['s3_time'] = output['time']
                download['size'] = output['size']
                download["local_filepath"] = local_filepath

                if os.path.exists(local_filepath):
                    # todo, check that the s3 object is the same as local copy
                    download["state"] = 'COMPLETE'
                else:
                    download['state'] = 'PENDING'

                self.downloads[s3_item] = download

    def update(self):
        """check status of downloads and start more subprocesses as needed.
        :return int number of pending/inprogress downloads
        """
        self.log.info("update")
    

        keys = list(self.downloads.keys())
        keys.sort()
        self.log.info("num items " + str(len(keys)))
        # count number of pending/inprocess items
        pending_count = 0
        inprocess_count = 0

        for s3_uri in keys:
            print(s3_uri)
            download = self.downloads[s3_uri]

            print(download)
            state = download["state"]
            s3_uri = download["s3_uri"]
            local_filepath = download["local_filepath"]

            if state == 'INPROGRESS':
                p = download['subprocess']
                p.poll()
                if p.returncode is None:
                    inprocess_count += 1  # still waiting on a download
                elif p.returncode < 0:
                    self.log.error("s3cmd failed for " + s3_uri)
                    download['subprocess'] = None
                    download["state"] = "FAILED"
                    download["rc"] = p.returncode
                else:
                    self.log.info("s3cmd complete for " + s3_uri)
                    download['subprocess'] = None
                    download["state"] = 'COMPLETE'
                    download["rc"] = 0
            elif state == 'PENDING':
                self.log.info("pending")
                if inprocess_count < self.s3cmd_batch_size:
                    # start a new download process
                    p = subprocess.Popen(
                        ['s3cmd', 'get', s3_uri, local_filepath])
                    download["subprocess"] = p
                    inprocess_count += 1
                    download["state"] = "INPROGRESS"
                else:
                    pending_count += 1
        # return count of pending and inprogress items
        # 0 -> download COMPLETE
        return pending_count + inprocess_count

    def downloadsize(self):
        """Get the number of bytes to be downloaded.
        """
        keys = list(self.downloads.keys())
        keys.sort()

        # count number of pending/inprocess items
        download_size = 0

        for s3_uri in keys:
            download = self.downloads[s3_uri]
            state = download["state"]
            if state in ("PENDING", "INPROGRESS"):
                download_size += download["size"]
        return download_size

    def start(self):
        """Start download.  Return exception if the amount of freespace is
        not sufficient to store objects in download queue.
        """
        # verify that we have enough free space for the download
        freespace = self.freespace
        download_size = self.downloadsize()
        if download_size > freespace:
            msg = "not enough free space to download: " + str(download_size)
            msg += ", " + str(freespace) + " available"
            self.log.info(msg)
            raise IOError(msg)

        count = self.update()
        return count

    def dump(self):
        """ Return state of download queue.
        """
        output = []
        s3_uris = list(self.downloads.keys())
        s3_uris.sort()
        for s3_uri in s3_uris:
            download = self.downloads[s3_uri]
            output.append((s3_uri, download['state']))
        return output
