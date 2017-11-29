import os
import sys
import logging
import time
from termcolor import colored
from colorama import Fore, Back, init, Style

import collections

import rpyc

FILENAME_LENGTH = 20
DIRNAME_LENGTH = 20

def check_name_length(name,length):
    if len(name) > length:
        print "Name can not be more than " + str(length) + " characters."
        return False
    else:
        return True

init(autoreset=True)
class bcolors:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def send_to_storage(block_uuid, data, minions):
    try:
        print "sending: " + str(block_uuid) + str(minions)
        minion = minions[0]
        minions = minions[1:]
        host, port = minion

        con = rpyc.connect(host, port=port)
        minion = con.root.Storage()
        minion.put(block_uuid, data, minions)
        logging.info("Blocks written to storage " + str(time.strftime("%d/%m/%Y") + time.strftime("%H:%M:%S")))
    except (RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while writing to storage " + str(time.strftime("%d/%m/%Y") + time.strftime("%H:%M:%S")))

def read_from_storage(block_uuid, minion):
    try:
        host, port = minion
        con = rpyc.connect(host, port=port)
        minion = con.root.Storage()
        return minion.get(block_uuid)
    except(RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while reading from storage " + str(time.strftime("%d/%m/%Y") + time.strftime("%H:%M:%S")))

def delete_from_storage(block_uuid, minion):
    try:
        host, port = minion
        con = rpyc.connect(host, port=port)
        minion = con.root.Storage()
        logging.info("deleted object from storage " + str(block_uuid))
        return minion.delete(block_uuid)
    except(RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while deleting from storage " + str(time.strftime("%d/%m/%Y") + time.strftime("%H:%M:%S")))


def put(master, path, source, dest):
    try:
        size = os.path.getsize(source)
        full_dest = path + dest
        blocks = master.write(full_dest, size)
        if blocks:
            with open(source) as f:
                for b in blocks:
                    data = f.read(master.get_block_size())
                    block_uuid = b[0]
                    minions = [master.get_list_of_minions()[_] for _ in b[1]]
                    send_to_storage(block_uuid, data, minions)
        else:
            print "File already exists"
        logging.info(dest + " successfully put in storage " + str(time.strftime("%d/%m/%Y") + time.strftime("%H:%M:%S")))
    except (RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while putting to storage " + str(time.strftime("%d/%m/%Y") + time.strftime("%H:%M:%S")))

def get(master, path, fname, mode):
    try:
        file_table = master.get_file_table_entry(path, fname)
        if not file_table:
            print "No such file"
            return
        flag = 0
        download_dir = os.getcwd() + '/files'
        for block in file_table:
            for i in range(len(block[1])):
                if block[1][i] in master.get_list_of_minions():
                    for m in master.get_list_of_minions():
                        data = read_from_storage(block[0], master.get_list_of_minions()[m])
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
        print "\nDone."
        if mode == 'download':
            print "File has been downloaded in ./files directory"
        logging.info("successfully get from storage " + path + " " + fname)
    except (RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while getting from storage " + str(time.strftime("%d/%m/%Y") + time.strftime("%H:%M:%S")))

def delete_file(master, path, fname):
    try:
        file_table = master.get_file_table_entry(path, fname)
        if file_table:
            for block in file_table:
                for i in range(len(block[1])):
                    if block[1][i] in master.get_list_of_minions():
                        active_minions = master.get_list_of_minions()
                        for m in active_minions:
                            delete_from_storage(block[0], active_minions[m])
        else:
            print "No such file"
        logging.info("deleted storage " + path)
        master.del_file(path, fname)
    except (RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while deelting storage " + str(time.strftime("%d/%m/%Y") + time.strftime("%H:%M:%S")))


def delete(master, path, obj_name):
    try:
        obj_list = master.list(path)
        if obj_name in obj_list:
            if obj_list[obj_name] == 'file':
                delete_file(master, path, obj_name)
            elif obj_list[obj_name] == 'dir' and obj_name != '.' and obj_name != '..':
                files = master.get_files_in_dir(path + obj_name)
                if files:
                    for file in files:
                        fpath, fname = file.rsplit('/',1)
                        fpath = fpath + '/'
                        delete_file(master, fpath, fname)
                master.del_dir(path, obj_name)
        logging.info("deleted object from storage " + path + " " + obj_name)
    except(RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while deleting from storage " + str(time.strftime("%d/%m/%Y") + time.strftime("%H:%M:%S")))



def get_keyboard_input(cur_dir):
    sys.stdout.write(bcolors.BOLD + bcolors.GREEN + '~' + cur_dir);
    sys.stdout.flush()

    cmd = sys.stdin.readline()
    parts = cmd.split(' ')
    args = []
    for part in parts:
        args.append(part.strip())
    return args
    
def check_free_diskspace(master, source):
    if os.path.isfile(source):
        return os.path.getsize(source) <= master.get_space_available()

def print_free_diskspace(master, mode='-b'):
    if mode == '-mb':
        available_space = str(round(master.get_space_available() / 1000000.0, 2)) + ' MB'
        total_space = str(round(master.get_total_space() / 1000000.0, 2)) + ' MB'
    elif mode == '-b':
        available_space = str(master.get_space_available()) + ' bytes'
        total_space = str(master.get_total_space()) + ' bytes'
    elif mode == '-gb':
        available_space = str(round(master.get_space_available() / 1000000000.0, 2)) + ' GB'
        total_space = str(round(master.get_total_space() / 1000000000.0, 2)) + ' GB'
    else:
        print "Wrong argumetns. Usage: space [-b/-mb/-gb]"
        return
    print "You have " + available_space + " free space out of " + total_space + " total disk space."

def check_dir(cur_dir, dirname):
    d_path = ''
    d_name = ''
    if dirname.startswith('/'):        
        d_path, d_name = dirname.rsplit('/', 1)
        d_path = d_path + '/'       
    elif dirname.startswith('./'):
        dirname = cur_dir + dirname[2:]
        d_path, d_name = dirname.rsplit('/', 1)
        d_path = d_path + '/'       
    elif dirname.startswith('../'):
        a, b, c = cur_dir.rsplit('/', 2)        
        dirname = a + dirname[2:]
        d_path, d_name = dirname.rsplit('/', 1)
        d_path = d_path + '/'
    else:
        if '/' in dirname:
            dirname = cur_dir + dirname
            d_path, d_name = dirname.rsplit('/', 1)
            d_path = d_path + '/'        
        else:
            d_path = cur_dir
            d_name = dirname
    return d_path, d_name

def main():
    con = rpyc.connect("localhost", port=2131)
    master = con.root

    cur_dir = "/"  
 
    print "Client started. Use 'help' to list all available commands."
    print_free_diskspace(master)
    args = get_keyboard_input(cur_dir)
    while args[0] != 'exit':
        if args[0] == 'get':
            if len(args) > 1:
                get(master, cur_dir, args[1], 'download')
            else:
                print "Filename is not specified. Usage: get <filename>"
        elif args[0] == 'cat':
            if len(args) > 1:
                get(master, cur_dir, args[1], 'open')
            else:
                print "Filename is not specified. Usage: cat <filename>"
        elif args[0] == 'put':
            if len(args) > 1:
                if len(args) == 3:                    
                    if check_name_length(args[2], FILENAME_LENGTH):
                        if not '/' in args[2]:
                            if os.path.isfile(args[1]):
                                if check_free_diskspace(master, args[1]):
                                    put(master, cur_dir, args[1], args[2])
                                    master.add_obj(cur_dir, args[2], 'file')
                                else:
                                    print "There is no enough space"
                            else:
                                print "There is no such file"
                        else:
                            print "Wrong input. Filename can not contain '/'."                    
                elif len(args) == 2:
                    if os.path.isfile(args[1]):
                        if check_free_diskspace(master, args[1]):
                            fname = os.path.basename(args[1])
                            put(master, cur_dir, args[1], fname)
                            master.add_obj(cur_dir, fname, 'file')
                        else:
                            print "There is no enough space"
                    else:
                        print "There is no such file"
                else:
                    print "Too many arguments"
            else:
                print "File is not specified. Usage: put <file> [new filename]"    
        elif args[0] == 'ls':
            obj_list = master.list(cur_dir)            
            for obj in obj_list:                    
                if obj_list[obj] == 'file':
                    s = master.get_file_size(cur_dir + obj)
                    print Fore.YELLOW + obj + '\t\t' + str(s) + ' bytes'
                else:
                    print Fore.CYAN + bcolors.BOLD + obj
        elif args[0] == 'mkdir':
            if len(args) > 1:
                if check_name_length(args[1], DIRNAME_LENGTH):
                    if not '/' in args[1]:
                        dirname = args[1]                
                        if not master.add_obj(cur_dir, dirname):
                            print "Directory already exists"
                    else:
                        print "Wrong input. Directory name can not contain '/'."
            else:
                print "Directory name is not specified. Usage: mkdir <dirname>"
        elif args[0] == 'cd':
            if len(args) > 1:
                dirname = args[1]
                if dirname == '..':                        
                    a,b,c = cur_dir.rsplit('/',2)                        
                    cur_dir = cur_dir[:-(len(b)+ 1)]
                else:
                    d_path, d_name = check_dir(cur_dir, dirname)
                    obj_list = master.list(d_path)                    
                    if obj_list and d_name in obj_list:
                        if obj_list[d_name] == 'dir' and d_name != '.':
                            cur_dir = d_path + d_name + '/'                            
                    else:
                        print "No such directory"              
            else:
                cur_dir = '/'
        elif args[0] == 'del':
            if len(args) > 1:
                delete(master, cur_dir, args[1])
            else:
                print "Directory or file name is not specified. Usage: del <dirname>/<filename>"
        elif args[0] == 'space':
            if len(args) > 1:
                print_free_diskspace(master, args[1])
            else:
                print_free_diskspace(master)
            
        elif args[0] == 'help':
            print "Commands:"
            print "  space - show available disk space. Arguments: -b in bytes, -mb in megabytes, -gb in gygabytes"
            print "  ls - see the list of files and directories"
            print "  mkdir - create a new directory. Usage: mkdir <dirname>"
            print "  cd - open a directory. Usage: cd <dirname>"
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