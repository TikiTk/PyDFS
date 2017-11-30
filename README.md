# PyDFS
Simple distributed file system like HDFS (and of-course GFS). It consists of one Nameserver (NameNode) and multiple Storageservers (DataNode). And a client for interaction. It will dump metadata/namespace when given SIGINT and reload it when fired up next time. Replicate data  the way HDFS does. It will send data to one storageserver and that storageserver will send it to next one and so on. Reading done in similar manner. Will contact first storageserver for block, if fails then second and so on.  Uses RPyC for RPC.

#### [Blog: Simple Distributed File System in Python : PyDFS](https://superuser.blog/distributed-file-system-python/) 

### Requirements:
  - rpyc (Really! That's it.)
  
### How to run.
  1. Start nameserver.py followed by storage.py and client.py
  2. storage.py will need details about nameserver which will be hinted by the prompt
  3. Start `client.py`, type 'help' to see command list.

##### Stop it using Ctrl + C so that it will dump the namespace.

## TODO:
  1. Use better algorithm for storage server selection to put a block (currently random)
  2. Add entry in namespace only after write succeeds.

