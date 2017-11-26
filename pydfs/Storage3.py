import rpyc
import uuid
import os
from pathlib2 import Path

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/storage/"


def check_storageserver_existence(new_storage, dictionary_of_existing_storage_server):
    for keys in dictionary_of_existing_storage_server:
        temp_conversion = new_storage[0],str(new_storage[1])
        if dictionary_of_existing_storage_server[keys] == temp_conversion:
            return True
    return False


def setup(host, ip, master):
    print "Initialising setup for minion: " + str(host) + " : " + str(ip)
    current_minions_dictionary = master.get_list_of_minions()
    new_minion = (host, ip)

    if check_storageserver_existence(new_minion, current_minions_dictionary):
        print "A host with a similar ip and port is already connected"

    else:
        current_size_of_dictionary = len(current_minions_dictionary)
        current_minions_dictionary[current_size_of_dictionary + 1] = new_minion
        master.set_new_minions(current_minions_dictionary)
        replication_factor = master.get_replication_factor()
        master.set_replication_factor(replication_factor + 1)
        print "New storage server added " + str(host) + " : " + str(ip)


class StorageService(rpyc.Service):
    @property
    def get_current_directory(self):
        return DATA_DIR

    class exposed_Storage():
        blocks = {}

        def exposed_put(self, block_uuid, data, minions):
            with open(DATA_DIR + str(block_uuid), 'w') as f:
                f.write(data)
            if len(minions) > 0:
                self.forward(block_uuid, data, minions)

        def exposed_get(self, block_uuid):
            block_addr = DATA_DIR + str(block_uuid)
            if not os.path.isfile(block_addr):
                return None

            # print Path(block_addr)

            with open(block_addr) as f:
                return f.read()

        def forward(self, block_uuid, data, minions):
            print "8888: forwaring to:"
            print block_uuid, minions
            minion = minions[0]
            minions = minions[1:]
            host, port = minion
            local_connection = rpyc.connect(host, port=port)
            minion = local_connection.root.Storage()
            minion.put(block_uuid, data, minions)

        def delete_block(self, uuid):
            pass


if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)

    try:
        con = rpyc.connect("localhost", port=2131)
        master = con.root
        setup('127.0.0.1', 1111, master)
    except Exception as message:
        print message
    t = ThreadedServer(StorageService, port=1111)
    t.start()
