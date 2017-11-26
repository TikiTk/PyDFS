# PyDFS
Simple distributed file system like HDFS (and of-course GFS). It consists of one Nameserver (NameNode) and multiple Storageservers (DataNode). And a client for interaction. It will dump metadata/namespace when given SIGINT and reload it when fired up next time. Replicate data  the way HDFS does. It will send data to one storageserver and that storageserver will send it to next one and so on. Reading done in similar manner. Will contact fitst storageserver for block, if fails then second and so on.  Uses RPyC for RPC.

#### [Blog: Simple Distributed File System in Python : PyDFS](https://superuser.blog/distributed-file-system-python/) 

### Requirements:
  - rpyc (Really! That's it.)
  
### How to run.
  1. Edit `dfs.conf` for setting block size, replication factor and list storageservers (`id:host:port`)
  2. Fireup `nameserver.py` and `storage.py`.
  3. Start `client.py`, type 'help' to see command list.

##### Stop it using Ctll + C so that it will dump the namespace.

## TODO:
  1. Implement Delete
  2. Use better algo for minion selection to put a block (currently random)
  3. Dump namespace periodically (maybe)
  4. Minion heartbeats / Block reports
  5. Add entry in namespace only after write succeeds.
  6. Use proper datastructure(tree-like eg. treedict) to sotre
     namespace(currently simple dict)
  7. Logging
  8. Expand this TODO
