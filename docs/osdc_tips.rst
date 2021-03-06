Intro
-----

Griffin is a OpenStack cluster managed by OSDC

Access the console here: https://www.opensciencedatacloud.org/project/#.

Information about Griffin can be found here: https://www.opensciencedatacloud.org/support/griffin.html.

Griffin will be used as our testbed for the Data Container project.

Griffin has no shared filesystem (like NFS) and VM's have no persistant storage.  We'll use a Ceph 
object store to store persistent data.

We are not charged for usage on Griffin, but be a good citizen and don't waste resources you are not using.

Setup
-----

Get in touch with jreadey to have an account setup.  He will also provide you with the 
osdc_keypair to login to a Griffin node.  You'll need a google account to access the console.


Launching a machine manually
----------------------------

From this page: https://www.opensciencedatacloud.org/project/instances/, select 
"Launch Instance".  Use the latest user snapshot snapshot<n> with the largest value of n.  Choose the desired "Flavor" (type of machine) and select "osdc_keypair" in the Access and Security Tab.

Choose "Terminate" when you are done with the machine.  

Note: all data (that wasn't captured in the snapshot) will be lost once you shut down the machine!

SSH-ing to the machine
----------------------

You will need to first ssh into a bastion host (griffin.opensciencedatacloud.org) before you 
can ssh into a openstack VM (the VM's are not directly connected to the internet).

For convenience you can write a little shell script with the ssh options:

ssh.sh --

ssh  -i osdc_keypair.pem -A <yourid>@griffin.opensciencedatacloud.org


From griffin you can ssh into any of the instances from the console page.
First copy the osdc_keypair.pem to your home page on Griffin and run: 

$ chmod 4000 osdc_keypair.pem


The ssh command will look like this:

ssh -i osdc_keypair.pem -A ubuntu@<IP>

Where <IP> is the IP address of the instance displayed in the console.

VM Hosts
--------
Once you have ssh'd into the VM you will be user 'ubuntu' with sudo privaleges.

The snapshot we've setup has:
 * Anaconda Python (use conda install xxx to install python packages)
 * Python packages; h5py, ipython, numpy, boto, etc.
 * HDF5 1.8.15 and tools (like h5dump)
 * s3cmd (for object store)
 * git clones of this project and h5serv
 
 To access anything outside the Griffin cluster, use the "with_proxy" command.  E.g.
 $ with_proxy ping www.google.com
 
 
S3Cmd
-----
S3cmd is a CLI tool for reading and writing to the object store.  The configuration is already setup, 
so you'll be able to use it directly.

Usage page is here, http://s3tools.org/usage, but some common examples are:

$ s3cmd ls  # list all buckets
$ s3cmd ls s3://hdfdata/  # list items in hdfdata bucket
$ s3cmd get s3://hdfdata/ncep3/GSSTF_NCEP.3.2008.12.31.he5.xml # download a file
$ s3cmd put foo s3://hdftest  # copy file 'foo' to hdftest bucket

Nova Client
-----------
From Griffin you can access the nova cli to monitor, create, and remove VMs:

$ nova list  # list all running VMs
$ nova image-list  # list image and snapshots
$ nova flavor-list # list avilable machine types
$ nova boot --flavor m1.small --snapshot <snapshot-id> --key-name osdc_keypair <name> # launch a VM with given snapshot and name.
$ nova delete mytest  # delete VM 'mytest'

Full user guide is here: http://docs.openstack.org/cli-reference/content/


