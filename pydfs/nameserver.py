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
from functools import reduce
import operator


def int_handler(signal, frame):
    pickle.dump((Nameserver.file_table, Nameserver.block_mapping, Nameserver.file_sizes, Nameserver.directory_tree),
                open('fs.img', 'wb'))
    sys.exit(0)


def set_conf():
    conf = ConfigParser.ConfigParser()
    conf.readfp(open('dfs.conf'))

    Nameserver.block_size = int(conf.get('nameserver', 'block_size'))
    Nameserver.replication_factor = int(conf.get('nameserver', 'replication_factor'))
    # storage = conf.get('nameserver', 'storage').split(',')
    # for m in storage:
    #     id, host, port = m.split(":")
    #     Nameserver.minions[id] = (host, port)

    if os.path.isfile('fs.img'):
        Nameserver.file_table, Nameserver.block_mapping, Nameserver.file_sizes, Nameserver.directory_tree = pickle.load(
            open('fs.img', 'rb'))


class Nameserver(rpyc.Service):
    file_table = {}
    block_mapping = {}
    minions = {}
    working_directory = StorageService.get_current_directory
    block_size = 0
    replication_factor = 0
    file_sizes = {}
    directory_tree = {}

    def exposed_get_replication_factor(self):
        return self.replication_factor

    def exposed_set_replication_factor(self, replication):
        self.replication_factor = replication

    def exposed_get_list_of_minions(self):
        return self.__class__.minions

    def exposed_set_new_minions(self, new_minion_dic={}):
        self.__class__.minions = new_minion_dic

    def exposed_list_files(self):
        files = self.__class__.file_table.keys()
        return files

    def exposed_list(self, path):
        dirs = path.split('/')
        dirs = filter(lambda dir: dir != '', dirs)

        dir_list = {}
        if path == '/':
            for obj, value in self.__class__.directory_tree.iteritems():
                if value != 'file':
                    dir_list[obj] = 'dir'
                else:
                    dir_list[obj] = 'file'
        else:
            dir_content = reduce(operator.getitem, dirs, self.__class__.directory_tree)
            for obj, value in dir_content.iteritems():
                if value != 'file':
                    dir_list[obj] = 'dir'
                else:
                    dir_list[obj] = 'file'
        return dir_list

    def exposed_read(self, fname):
        self.check_connection_to_storageservers(self.minions)
        mapping = self.__class__.file_table[fname]
        return mapping

    def exposed_write(self, filename, size):

        self.check_connection_to_storageservers(self.minions)
        self.__class__.file_table[filename] = []
        self.__class__.file_sizes[filename] = size
        num_blocks = self.calc_num_blocks(size)
        return self.alloc_blocks(filename, num_blocks)

    def exposed_check_if_directory_exists(self, current_directory, new_directory):
        dirs = current_directory.split('/')
        dirs = filter(lambda directory: directory != '', dirs)
        child_directory = reduce(operator.getitem, dirs, self.__class__.directory_tree)

        if current_directory == '/':
            return new_directory in self.__class__.directory_tree and self.directory_tree[new_directory] == 'dir'
        return new_directory in child_directory

    def exposed_add_obj(self, path, obj_name, obj_type='dir'):
        dirs = path.split('/')
        dirs = filter(lambda directory: directory != '', dirs)

        if obj_type == 'file':
            obj_to_add = {obj_name: 'file'}
        elif obj_type == 'dir':
            obj_to_add = {obj_name: {'.': 'self', '..': 'parent'}}

        if path == '/':
            self.__class__.directory_tree.update(obj_to_add)
        else:
            reduce(operator.getitem, dirs, self.__class__.directory_tree).update(obj_to_add)

    def exposed_get_file_table_entry(self, path, fname):
        self.check_connection_to_storageservers(self.minions)
        flist = Nameserver.exposed_list(self, path)
        if (fname in self.__class__.file_table) and (fname in flist):
            return self.__class__.file_table[fname]
        else:
            return None

    def exposed_get_file_size(self, fname):
        if fname in self.__class__.file_sizes:
            return self.__class__.file_sizes[fname]
        else:
            return None

    def exposed_del_file(self, fname):
        temp_files_size = deepcopy(self.__class__.file_sizes)
        temp_file_table = deepcopy(self.__class__.file_table)
        temp_dictionary = deepcopy(self.__class__.directory_tree)
        if fname in temp_files_size:
            del temp_files_size[fname]
            self.__class__.file_sizes = temp_files_size

        if fname in temp_file_table:
            del temp_file_table[fname]
            self.__class__.file_table = temp_file_table

        if fname in temp_dictionary:
            del temp_dictionary[fname]
            self.__class__.directory_tree = temp_dictionary

    def exposed_get_block_size(self):
        return self.__class__.block_size

    def exposed_get_storageservers(self):
        return self.__class__.minions

    def calc_num_blocks(self, size):
        return int(math.ceil(float(size) / self.__class__.block_size))

    def exposed_exists(self, filename, dirs):
        dirs = dirs.split('/')
        dirs = filter(lambda directory: directory != '', dirs)
        if dirs == '/':
            return filename in self.__class__.directory_tree and self.__class__.directory_tree[filename] == 'file'
        child_directory = reduce(operator.getitem, dirs, self.__class__.directory_tree)
        return filename in child_directory

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
                print "Storage server " + str(temp_dic[keys]) + " has problems"
                del dictionary_of_minions[keys]
                self.__class__.replication_factor -= 1
        self.minions = dictionary_of_minions
        return self.minions


if __name__ == "__main__":
    set_conf()
    signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(Nameserver, port=2131)
    t.start()
