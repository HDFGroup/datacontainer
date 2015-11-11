import sys
import subprocess
import os
import time
import logging
 

class S3Download:
     
    def __init__(self):
        self.home_dir = os.environ["HOME"]
        self.log = logging.getLogger("s3download")
        self.log.setLevel(logging.INFO)
        handler = logging.FileHandler(os.path.join(self.home_dir, "s3download.log"))
        self.log.addHandler(handler)
        self.downloads = {}
        
        self.s3_prefix = "s3://"
        self.s3_dir = os.environ["S3_CACHE_DIR"]
        self.s3cmd_batch_size = 2
        
        if self.s3_dir is None:
            raise IOError("S3_CACHE_DIR environment variable not set")
         
        self.log.info("s3_dir: " + self.s3_dir)
        if not os.path.isdir(self.s3_dir):
            raise IOError(self.s3_dir, ": directory does not exist")
        
            
    def freespace(self):
        """Get the amount of freespace available.
        """
        
        df = subprocess.Popen(["df", self.s3_dir], stdout=subprocess.PIPE)
        output = df.communicate()[0]
        text = output.decode("utf-8")  # convert to string
        index = text.index('\n')
        if index < 0:
            raise IOError("unexpected output of df")
        text = text[(index+1):]
        fields = text.split()
        if len(fields) != 6:
            raise IOError("unexpected output of df")
        return int(fields[3]) 
        
    def usedspace(self):
        """Get the number of bytes used in the s3 download directory.
        """
        du = subprocess.Popen(["du", self.s3_dir], stdout=subprocess.PIPE)
        output = du.communicate()[0]
        text = output.decode("utf-8")  # convert to string
        lines = text.split('\n')
        if len(lines) == 0:
            raise IOError("unexpected output of du")
        last_line = None
        for line in lines:
            line = line.strip()
            if line:
                last_line = line    
        
        fields = last_line.split()
         
        if len(fields) != 2:
            raise IOError("unexpected output of du")
        return int(fields[0])
        
    def rmrf(self, pdir):
        """ Remove all files in the given directory.
           caution recursive delete - use with care!
        :arg str pdir: directory to remove files from
        """
        self.log.info("rmrf()")
        files = []
        if pdir is None:
            raise IOError("clear called with non-valid path")
        if not os.path.isdir(pdir):
            raise IOError("clear called with non-directory path")
        # sanity check, so we don't got erasing files outside the s3_dir...
        if not pdir.startswith(self.s3_dir):
            raise IOError("Invalid path", pdir)
            
        for f in os.listdir(pdir):
            path = os.path.join(pdir, f)
            if os.path.isdir(path):
                self.rmrf(pdir=path)
                self.log.info("rmdir:" + path)
                os.rmdir(path)
            else:
                self.log.info("rm: " + path)
                os.remove(path)
                
    def clear(self):
        """ Remove files from s3 download directory, clears download queue
        """
        self.log.info("clear")
        self.rmrf(self.s3_dir)
        self.downloads.clear()
        
    def s3cmdls(self, s3uri):
        """ List s3 objects
        :arg str s3uri: s3 uri
        """
        s3cmd = subprocess.Popen(["s3cmd", "ls", s3uri], stdout=subprocess.PIPE)
        output = s3cmd.communicate()[0]
        text = output.decode("utf-8")  # convert to string
        lines = text.split('\n')
        s3ls = []
        if len(lines) == 0:
            return s3ls  # empty list
        for line in lines:
            line = line.strip()
            if line:
                fields = line.split()
                # expecting something like:
                # 2015-10-23 07:44  16631632   s3://hdfdata/ncep3/GSSTF_NCEP.3.1987.07.01.he5
                if len(fields) == 2 and fields[0] == "DIR":
                    # ignore dirs
                    continue
                if len(fields) != 4:
                    raise IOError("Unexpected output from s3cmd: " + line)  
                s3ls_output = (fields[0], fields[1], int(fields[2]), fields[3])
                s3ls.append(s3ls_output) 
        
        
        return s3ls
        
        
    def addFiles(self, s3uris):
        """add objects to download list.
        :arg list s3uris can be folder path, or list of object uri's
        """
        if type(s3uris) is str:
            # convert to one element list
            s3_list = (s3uris,)
            s3uris = s3_list
        
        for s3_uri in s3uris:
            self.log.info("addFiles: " + s3_uri)
            if not s3_uri.startswith(self.s3_prefix):
                raise IOError("Invalid s3 uri")
            s3ls_out = self.s3cmdls(s3_uri)
            if len(s3ls_out) == 0:
                raise IOError("no s3 objects found for " + s3_uri)
            for output in s3ls_out:
                s3_item = output[3]
                print("got item: ", s3_item)
                if s3_item in self.downloads:
                    self.log.info(s3_item + " already added")
                    continue
                s3_path = s3_item[len(self.s3_prefix):]
                print("s3_path", s3_path)
                local_filepath = os.path.join(self.s3_dir, s3_path)
            
                download = {}
                download['s3_uri'] = s3_item
                download['s3_date'] = output[0]
                download['s3_time'] = output[1]
                download['size'] = output[2]
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
        print("update!")
        
        keys = list(self.downloads.keys())
        keys.sort()
        print("num items " + str(len(keys)))
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
                    inprocess_count += 1 # still waiting on a download
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
                print("pending")      
                if inprocess_count < self.s3cmd_batch_size:
                    # start a new download process
                    p = subprocess.Popen(['s3cmd', 'get', s3_uri, local_filepath])
                    download["subprocess"] = p
                    inprocess_count += 1
                    download["state"] = "INPROGRESS"
                else:
                    pending_count += 1
        # return count of pending and inprogress items
        # 0 -> download COMPLETE
        return pending_count + inprocess_count
    
    def getDownloadSize(self):
        """Get the number of bytes to be downloaded.
        """
        keys = list(self.downloads.keys())
        keys.sort()
        
        # count number of pending/inprocess items
        download_size = 0
        
        for s3_uri in keys:
            download = self.downloads[s3_uri]
            state = download["state"]
            if state in ("PENDING" or "INPROGRESS"): 
                download_size += download["size"]
        return download_size
                    
    def start(self):
        """Start download.  Return exception if the amount of freespace is 
        not sufficient to store objects in download queue.
        """
        # verify that we have enough free space for the download
        freespace = self.freespace()
        download_size = self.getDownloadSize()
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
            output.append((s3_uri, download['state'] ))
        return output
         
     

    
     
     
  
