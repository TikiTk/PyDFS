import ConfigParser
import math
import os
import pickle
import random
import signal
import socket
import sys
import uuid
import rpyc
from rpyc.utils.server import ThreadedServer
from copy import deepcopy
from storage import StorageService


def int_handler(signal, frame):
    pickle.dump((Nameserver.file_table, Nameserver.block_mapping),
                open('fs.img', 'wb'))
    sys.exit(0)


def set_conf():
    conf = ConfigParser.ConfigParser()
    conf.readfp(open('dfs.conf'))

    Nameserver.block_size = int(conf.get('nameserver', 'block_size'))
    Nameserver.replication_factor = int(conf.get('nameserver', 'replication_factor'))
    storage = conf.get('nameserver', 'storage').split(',')
    for m in storage:
        id, host, port = m.split(":")
        Nameserver.minions[id] = (host, port)

    if os.path.isfile('fs.img'):
        Nameserver.file_table, Nameserver.block_mapping = pickle.load(
            open('fs.img', 'rb'))


class Nameserver(rpyc.Service):
    file_table = {}
    block_mapping = {}
    minions = {}
    working_directory = StorageService.get_current_directory
    block_size = 0
    replication_factor = 0
    file_sizes = {}

    def exposed_get_replication_factor(self):
        return self.replication_factor

    def exposed_set_replication_factor(self,replication):
        self.replication_factor = replication

    def exposed_get_list_of_minions(self):
        return self.__class__.minions

    def exposed_set_new_minions(self, new_minion_dic={}):
        self.__class__.minions = new_minion_dic

    def exposed_list_files(self):
        files = self.__class__.file_table.keys()
        return files

    def exposed_read(self, fname):
        mapping = self.__class__.file_table[fname]
        return mapping

    def exposed_write(self, dest, size):
        if self.exists(dest):
            # print "File already exists"
            # return
            pass
        self.check_connection_to_storageservers(self.minions)
        self.__class__.file_table[dest] = []
        self.__class__.file_sizes[dest] = size

        num_blocks = self.calc_num_blocks(size)
        blocks = self.alloc_blocks(dest, num_blocks)
        return blocks

    def exposed_get_file_table_entry(self, fname):
        if fname in self.__class__.file_table:
            return self.__class__.file_table[fname]
        else:
            return None

    def exposed_get_file_size(self, fname):
        if fname in self.__class__.file_sizes:
            return self.__class__.file_sizes[fname]
        else:
            return None

    def exposed_del_file(self, fname):
        if fname in self.__class__.file_sizes:
            del self.__class__.file_sizes[fname]

        if fname in self.__class__.file_table:
            del self.__class__.file_table[fname]



    def exposed_get_block_size(self):
        return self.__class__.block_size

    def exposed_get_storageservers(self):
        return self.__class__.minions

    def calc_num_blocks(self, size):
        return int(math.ceil(float(size) / self.__class__.block_size))

    def exists(self, file):
        return file in self.__class__.file_table

    def alloc_blocks(self, dest, num):
        blocks = []
        for i in range(0, num):
            block_uuid = uuid.uuid1()
            nodes_ids = random.sample(self.__class__.minions.keys(), self.__class__.replication_factor)
            blocks.append((block_uuid, nodes_ids))

            self.__class__.file_table[dest].append((block_uuid, nodes_ids))

        return blocks

    def storageserverworksfine(self, host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = s.connect_ex((host, port))
        s.close()
        if result:
            return True  # server has a problem
        else:
            return False  # server works fine

    def check_connection_to_storageservers(self, dictionary_of_minions):

        temp_dic = deepcopy(dictionary_of_minions)

        for keys in temp_dic:
            host, port = temp_dic[keys][0], temp_dic[keys][1]
            if self.storageserverworksfine(str(host), int(port)):
                print "Storage server " + str(temp_dic[keys][0][1]) + " has problems"
                del dictionary_of_minions[keys]
                self.__class__.replication_factor -= 1
        self.minions = dictionary_of_minions
        return self.minions


if __name__ == "__main__":
    set_conf()
    signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(Nameserver, port=2131)
    t.start()
