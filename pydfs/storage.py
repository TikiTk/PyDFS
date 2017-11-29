import uuid
import os
import sys
import pickle
import signal

import rpyc
from pathlib2 import Path

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/storage/"


def check_storageserver_existence(new_storage, dictionary_of_existing_storage_server):
    for keys in dictionary_of_existing_storage_server:
        temp_conversion = new_storage[0], str(new_storage[1])
        if dictionary_of_existing_storage_server[keys] == temp_conversion:
            return True
    return False


def setup(host, ip, master):
    print "Initialising setup for minion: " + str(host) + " : " + str(ip)
    current_minions_dictionary = master.get_list_of_minions()
    new_minion = (host, ip)

    if check_storageserver_existence(new_minion, current_minions_dictionary):
        print "A host with a similar ip and port is already connected"
        return

    else:
        current_size_of_dictionary = len(current_minions_dictionary)
        current_minions_dictionary[current_size_of_dictionary + 1] = new_minion
        master.set_new_minions(current_minions_dictionary)
        replication_factor = master.get_replication_factor()
        master.set_replication_factor(replication_factor+1)
        print "New storage server added " + str(host) + " : " + str(ip)
        return
def get_ip_port_config(default_ip, default_port):
    sys.stdout.write('Type IP-address (default is ' + default_ip + '): '); sys.stdout.flush()
    user_input = sys.stdin.readline().strip()
    addr = user_input if user_input else default_ip

    sys.stdout.write('Type a port (default is ' + str(default_port) + '): '); sys.stdout.flush()
    user_input = sys.stdin.readline().strip()
    port = int(user_input) if user_input else default_port

    return addr, port

def int_handler(signal, frame):
    pickle.dump((n_addr, n_port, s_addr, s_port),
                open('last_storage.conf', 'wb'))
    sys.exit(0)

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

        def exposed_delete(self, block_uuid):
            block_addr = DATA_DIR + str(block_uuid)
            if os.path.isfile(block_addr):
                os.remove(block_addr)

        def forward(self, block_uuid, data, minions):
            print "Forwaring to:"
            print block_uuid, minions
            minion = minions[0]
            minions = minions[1:]
            host, port = minion
            local_connection = rpyc.connect(host, port=port)
            minion = local_connection.root.Storage()
            minion.put(block_uuid, data, minions)


if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)


    if os.path.isfile('last_storage.conf'):
        n_addr, n_port, s_addr, s_port = pickle.load(
            open('last_storage.conf', 'rb'))
    else:
        print "Nameserver configuration:"
        n_addr, n_port = get_ip_port_config('localhost', 2131)
        print ""

        print "Storage server configuration: "
        s_addr, s_port = get_ip_port_config('127.0.0.1', 8888)
        print ""

    signal.signal(signal.SIGINT, int_handler)

    try:
        con = rpyc.connect(n_addr, n_port)
        master = con.root
        setup(s_addr, s_port, master)

    except Exception as message:
        print message
    t = ThreadedServer(StorageService, port=s_port)
    t.start()
