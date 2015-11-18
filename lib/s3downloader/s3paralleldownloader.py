import sys
import os
import time
import logging

#from s3downloader import S3Download
import s3downloader


class S3ParallelDownload:

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
        return self.dview.apply_sync(s3downloader.freespace)  
    
    def usedspace(self):
        return self.dview.apply_sync(s3downloader.usedspace)
    
    def clear(self, remove_all=False):
        return self.dview.apply_sync(s3downloader.clear, remove_all)
         
    
    def getFiles(self):
        return self.dview.apply_sync(s3downloader.getFiles)
    
    def loadFiles(self, s3uris):
        # first get the s3 list serially
        downloads = self.s3.cmdls(s3uris)
    
        if len(downloads) == 0:
            raise IOError("Nothing to download!")

        num_engines = len(self.rc.ids)
        # manually divy up the files to the set of engines
        file_set_size = len(downloads)/num_engines   # PY 3 style float result

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
            url_list = uri_map[engine_id]
            async_result = engine_view.apply_async(s3downloader.addFiles, url_list)
  
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
        
 
        counts = self.dview.apply_sync(s3downloader.start)
        while any(v > 0 for v in counts):
            time.sleep(1)
            counts = self.dview.apply_sync(s3downloader.update)
            print("counts:", counts)
    
        print("done!")




    
     
     
  
