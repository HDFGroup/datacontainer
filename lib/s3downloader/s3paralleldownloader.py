import sys
import os
import time
import logging
from fnmatch import fnmatch

# from s3downloader import S3Download
import s3downloader


class S3ParallelDownload(object):

    def __init__(self, rc):
        import s3downloader
        self.rc = rc
        self.dview = rc[:]
        self.s3 = s3downloader.S3Download()
        self.log = logging.getLogger("s3download")
        self.log.setLevel(logging.INFO)
        handler = logging.FileHandler(os.path.join(os.environ["HOME"],
                                      "s3download.log"))
        self.log.addHandler(handler)
        with self.dview.sync_imports():
            import s3downloader
        self.dview.apply_sync(s3downloader.init)

    def freespace(self):
        if len(self.rc) == 0:
            raise IOError("No egnines running")
        return self.dview.apply_sync(s3downloader.freespace)

    def usedspace(self):
        if len(self.rc) == 0:
            raise IOError("No egnines running")
        return self.dview.apply_sync(s3downloader.usedspace)

    def clear(self, s3uri=None, remove_all=False):
        if len(self.rc) == 0:
            raise IOError("No egnines running")
        return self.dview.apply_sync(s3downloader.clear,
                                     s3uri_prefix=s3uri,
                                     remove_all=remove_all)

    def getFiles(self, state=None, s3uri=None):
        if len(self.rc) == 0:
            raise IOError("No egnines running")
        return self.dview.apply_sync(s3downloader.getFiles,
                                     state=state, s3uri_prefix=s3uri)

    def loadFiles(self, s3uris):
        print("loadFiles")
        if len(self.rc) == 0:
            raise IOError("No egnines running")
        if type(s3uris) is not str or not s3uris.startswith("s3://"):
            raise IOError("Invalid argument to laodFiles")
            
        pattern = None
        
        if s3uris.endswith('/') or s3uris.rfind('*') == -1:
            # no wild card - just get all objects with this prefix
            s3uri_prefix = s3uris
        else:
            index = s3uris.rfind('/')
            if index == 4:
                raise IOError("Must provide a bucket name")
            s3uri_prefix = s3uris[:(index+1)]
            pattern = s3uris[(index+1):]
            print("Using pattern:", pattern)
            
        
        print("getting list of uri's to download")    
        # first get the s3 list serially
        ls_out = self.s3.cmdls(s3uri_prefix)

        if len(ls_out) == 0:
            raise IOError("Nothing to download!")
         
        print("checking storage spaace across machines") 
        # do a rough check to see if there is enough space on the 
        # machines to do the downoad.    
        total_bytes = 0    
        downloads = []
        
        for item in ls_out:
            
            if pattern is not None:
                # filter out any uri's that don't match the pattern
                uri = item['uri']
                index = uri.rfind('/')
                name = uri[(index+1):]
                print("fnmatch", name, pattern)
                if fnmatch(name, pattern) is False:
                    print("no match")
                    continue  # doesn't match
                print("match")
             
            total_bytes += item['size']
            downloads.append(item)
                
        if len(downloads) == 0:
            raise IOError("Nothing matching filter, no download")
            
        num_engines = len(self.rc.ids)
        
        # manually divy up the files to the set of engines
        file_set_size = total_bytes/num_engines   # PY 3 style float result
        
        output = self.freespace()
        for item in output:
            if item < file_set_size:
                raise IOError("Not enough space to download")

        print("storage space should be sufficient")
        
        uri_map = {}
        engine_num = 0
        for download in downloads:
            if engine_num not in uri_map.keys():
                uri_map[engine_num] = []
            uri_list = uri_map[engine_num]
            uri_list.append(download['uri'])
            engine_num = (engine_num+1) % num_engines

        # add each engines list
        async_results = []
        for engine_id in range(num_engines):
            engine_view = self.rc[engine_id]
            if engine_id not in uri_map:
                continue  # nothing for this guy to do
            url_list = uri_map[engine_id]
            print("dispatching download list to engine:", engine_id)
            print("url_list", url_list)
            async_result = engine_view.apply_async(s3downloader.addFiles,
                                                   url_list)

        # wait while the file list gets downloaded
        ready = False
        while not ready:
            ready = True
            for async_result in async_results:
                if not async_result.ready():
                    ready = False
                    break
            if not ready:
                time.sleep(1)
                
        print("starting download")

        counts = self.dview.apply_sync(s3downloader.start)
        loop_number = 0
        while any(v > 0 for v in counts):
            time.sleep(1)
            counts = self.dview.apply_sync(s3downloader.update)
            loop_number += 1
            if (loop_number % 5) == 0:
                print("counts:", counts)

        print("done!")
