import os
import sys

import rpyc
import nameserver


def send_to_storage(block_uuid, data, minions):
    print "sending: " + str(block_uuid) + str(minions)
    minion = minions[0]
    minions = minions[1:]
    host, port = minion

    con = rpyc.connect(host, port=port)
    minion = con.root.Storage()
    minion.put(block_uuid, data, minions)


def read_from_storage(block_uuid, minion):
    host, port = minion
    con = rpyc.connect(host, port=port)
    minion = con.root.Storage()
    return minion.get(block_uuid)


def put(master, source):
    size = os.path.getsize(source)
    dest = source
    blocks = master.write(dest, size)
    with open(source) as f:
        for b in blocks:
            data = f.read(master.get_block_size())
            block_uuid = b[0]
            minions = [master.get_storageservers()[_] for _ in b[1]]
            send_to_storage(block_uuid, data, minions)


def read_file(file_table, master):
    for block in file_table:
        for m in [master.get_storageservers()[_] for _ in block[1]]:
            data = read_from_storage(block[0], m)
            if data:
                sys.stdout.write(data)
                break
        else:
            print "No blocks found. Possibly a corrupt file"

def main(args):
    con = rpyc.connect("localhost", port=2131)
    master = con.root.Nameserver()
    if args[0] == "get":
        nameserver.get(master, args[1])
    elif args[0] == "put":
        put(master, args[1])
    else:
        print "try 'put srcFile destFile OR get file'"


if __name__ == "__main__":
    main(sys.argv[1:])


