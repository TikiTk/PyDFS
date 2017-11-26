import os
import sys

from termcolor import colored
from colorama import Fore, Back, init, Style

import rpyc

init(autoreset=True)
class bcolors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

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

def delete_from_storage(block_uuid, minion):
    host, port = minion
    con = rpyc.connect(host, port=port)
    minion = con.root.Storage()
    return minion.delete(block_uuid)

def put(master, source, dest):
    size = os.path.getsize(source)    
    blocks = master.write(dest, size)
    with open(source) as f:
        for b in blocks:
            data = f.read(master.get_block_size())
            block_uuid = b[0]
            minions = [master.get_list_of_minions()[_] for _ in b[1]]
            send_to_storage(block_uuid, data, minions)


def get(master,fname, mode):
    file_table = master.get_file_table_entry(fname)
    if not file_table:
        print "404: file not found"
        return
    print file_table
    flag = 0
    download_dir = os.getcwd() + '/files'
    for block in file_table:
        for i in range(len(block[1])):
            if block[1][i] in master.get_list_of_minions():
                for m in master.get_list_of_minions():
                    data = read_from_storage(block[0], m)
                    if data:
                        if mode == 'download':
                            if not os.path.isdir(download_dir): os.mkdir(download_dir)
                            if flag:

                                with open(download_dir + '/' + fname, 'a') as f:
                                    f.write(data)
                            else:
                                with open(download_dir + '/' + fname, 'w') as f:
                                    f.write(data)
                                    flag = 1
                        elif mode == 'open':
                            sys.stdout.write(data)
                        break
                    
                else:
                    print "No blocks found. Possibly a corrupt file"
            print "Done"

def delete(master,fname):
    file_table = master.get_file_table_entry(fname)
    if not file_table:
        print "404: file not found"
        return
    for block in file_table:
        for m in [master.get_list_of_minions()[_] for _ in block[1]]:
            delete_from_storage(block[0], m)
    master.del_file(fname)

def main():
    con = rpyc.connect("localhost", port=2131)
    master = con.root

def get_keyboard_input(cur_dir):
    sys.stdout.write(bcolors.BOLD + bcolors.GREEN + cur_dir); sys.stdout.flush()

    cmd = sys.stdin.readline()
    parts = cmd.split(' ')
    args = []
    for part in parts:
        args.append(part.strip())
    return args

def main():
    con = rpyc.connect("localhost", port=2131)
    master = con.root

    cur_dir = "~/"
    print "Client started. Use 'help' to list all available commands."
    args = get_keyboard_input(cur_dir)
    while args[0] != 'exit':
        if args[0] == 'get':
            if len(args) > 1:
                get(master, args[1], 'download')
            else:
                print "Filename is not specified. Usage: get <filename>"
        elif args[0] == 'cat':
            if len(args) > 1:
                get(master, args[1], 'open')
            else:
                print "Filename is not specified. Usage: cat <filename>"
        elif args[0] == 'put':
            if len(args) > 1:
                if len(args) == 3:
                    put(master, args[1], args[2])
                elif len(args) == 2:
                    fname = os.path.basename(args[1])
                    put(master, args[1], fname)
                else:
                    print "Too many arguments"
            else:
                print "File is not specified. Usage: put <file> [new filename]"    
        elif args[0] == 'ls':
            for f in master.list_files():
                s = master.get_file_size(f)
                print Fore.YELLOW + f + ' ' + str(s)
        elif args[0] == 'mkdir':
            if len(args) > 1:
                directoryname = args[1]
                os.mkdir(directoryname)
            else:
                print "Directory name is not specified. Usage: mkdir <dirname>"
        elif args[0] == 'cd':
            if len(args) > 1:
                directoryname = args[1]
                os.chdir(directoryname)
            else:
                print "Directory name is not specified. Usage: cd <dirname>"
        elif args[0] == 'del':
            if len(args) > 1:
#                obj_name = args[1]
#                os.rmdir(obj_name)
                delete(master, args[1])
            else:
                print "Directory or file name is not specified. Usage: del <dirname>/<filename>"
        elif args[0] == 'help':
            print "Commands:"
            print "  ls - see the list of files and directories;"
            print "  mkdir - create a new directory. Usage: mkdir <dirname>"
            print "  cd - open a directory. Usage: mkdir <dirname>"
            print "  del - delete a file or directory. Usage: del <dirname>/<filename>"
            print "  put - write a file in the current directory. Usage: put <filename> [new filename]"
            print "  get - download a file. Usage: get <filename>"
            print "  cat - open a file. Usage: cat <filename>"
            print "  exit - stop the client"
        else:
            print "Wrong input. Try again (use 'help' to get list of all available commands)"
        args = get_keyboard_input(cur_dir)


if __name__ == "__main__":
    main()


