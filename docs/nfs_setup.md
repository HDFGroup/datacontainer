### How to setup NFS on OSDC
 

Instructions for installing NFS

## On the server 

# Install NFS:

   $ with_proxy sudo -E apt-get install nfs-kernel-server
   
# Make the directory to be exported public read/write:

   $ sudo chmod 777 /mnt/hdfdata/
   
# Add entry to /etc/exports:

   /mnt/hdfdata     172.17.192.0/24(rw,fsid=0,insecure,no_subtree_check,async)

# Start service:

   $ sudo service nfs-kernel-server restart
   
# configure to run on startup:
 
   $ sudo update-rc.d nfs-kernel-server defaults
   
## On the client:

# Install software:

   $ with_proxy sudo -E install nfs-common
   
# Mount remote filesystem:

   $ sudo mount -v -t nfs -o proto=tcp,port=2049 172.17.192.26:/ /mnt
   
   where 172.17.192.26 should be adjusted to the actual IP of the NFS Server