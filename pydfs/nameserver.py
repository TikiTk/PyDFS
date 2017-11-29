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

    TOTAL_DISK_SPACE = 2000000000

    def exposed_get_total_space(self):
        return self.__class__.TOTAL_DISK_SPACE

    def exposed_get_space_available(self):
        occupied = 0
        for value in self.__class__.file_sizes.values():
            occupied = occupied + int(value)
        available = self.__class__.TOTAL_DISK_SPACE - occupied
        return available

    def exposed_get_replication_factor(self):
        return self.replication_factor

    def exposed_set_replication_factor(self, replication):
        self.replication_factor = replication

    def exposed_get_list_of_minions(self):
        return self.__class__.minions

    def exposed_set_new_minions(self, new_minion_dic={}):
        self.__class__.minions = new_minion_dic

    def exposed_list(self, path):
        dirs = self.get_dirs_in_path(path)
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

    def exposed_write(self, dest, size):
        if self.exists(dest):
            return None
        else:
            self.check_connection_to_storageservers(self.minions)
            self.__class__.file_table[dest] = []
            self.__class__.file_sizes[dest] = size

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            return blocks

    def exposed_add_obj(self, path, obj_name, obj_type='dir'):
        dirs = self.get_dirs_in_path(path)
        
        if obj_type == 'file':
            obj_to_add = {obj_name: 'file'}
        elif obj_type == 'dir' and not self.dir_exists(path, obj_name):
            obj_to_add = {obj_name: {'.':'self','..':'parent'}}
        else:
            return False

        if path == '/':
            self.__class__.directory_tree.update(obj_to_add)
        else:
            reduce(operator.getitem, dirs, self.__class__.directory_tree).update(obj_to_add)
        return True

    def exposed_get_file_table_entry(self, path, fname):
        self.check_connection_to_storageservers(self.minions)
        full_name = path + fname
        if full_name in self.__class__.file_table:
            return self.__class__.file_table[full_name]
        else:
            return None

    def exposed_get_file_size(self, fname):
        if fname in self.__class__.file_sizes:
            return self.__class__.file_sizes[fname]
        else:
            return None

    def exposed_del_file(self, path, fname):
        full_name = path + fname
        if full_name in self.__class__.file_sizes:
            del self.__class__.file_sizes[full_name]

        if full_name in self.__class__.file_table:
            del self.__class__.file_table[full_name]

        dirs = self.get_dirs_in_path(path)
        if path == '/':
            if fname in self.__class__.directory_tree:
                del self.__class__.directory_tree[fname]
        else:
            fdir = reduce(operator.getitem, dirs, self.__class__.directory_tree)
            if fname in fdir:
                del fdir[fname]

    def exposed_del_dir(self, dirname):
        if dirname in self.__class__.directory_tree:
            del self.__class__.directory_tree[dirname]

    def exposed_get_files_in_dir(self, path):
        files = {}
        for file in self.__class__.file_table:
            if file.startswith(path):
                files.update({file:self.__class__.file_table[file]})
        return files

    def exposed_get_block_size(self):
        return self.__class__.block_size

    def exposed_get_storageservers(self):
        return self.__class__.minions



    def get_dirs_in_path(self, path):
        dirs = path.split('/')
        while '' in dirs:
            dirs.remove('')
        return dirs

    def calc_num_blocks(self, size):
        return int(math.ceil(float(size) / self.__class__.block_size))

    def exists(self, file):
        return file in self.__class__.file_table

    def dir_exists(self, path, dirname):
        dirs = self.get_dirs_in_path(path)
        if path == '/':
            return dirname in self.__class__.directory_tree
        else:
            return dirname in reduce(operator.getitem, dirs, self.__class__.directory_tree)

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
