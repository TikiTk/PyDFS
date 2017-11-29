import os
import sys
from colorama import Fore, init
import logging
import time
import rpyc

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
        logging.info("Blocks written to storage "+str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))
    except (RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while writing to storage "+str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))


def read_from_storage(block_uuid, minion):

    try:
        host, port = minion
        con = rpyc.connect(host, port=port)
        minion = con.root.Storage()
        return minion.get(block_uuid)
    except(RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while reading from storage " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))


def delete_from_storage(block_uuid, minion):

    try:
        host, port = minion
        con = rpyc.connect(host, port=port)
        minion = con.root.Storage()
        logging.info("deleted object from storage "+str(block_uuid))
        return minion.delete(block_uuid)

    except(RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while writing to storage " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))

def put(master, source, filename,dir):

    try:
        size = os.path.getsize(source)
        if master.exists(filename,dir):
            print 'File exists'
            return
        else:
            blocks = master.write(filename, size)
            with open(source) as f:
                for b in blocks:
                    data = f.read(master.get_block_size())
                    block_uuid = b[0]
                    minions = [master.get_list_of_minions()[_] for _ in b[1]]
                    send_to_storage(block_uuid, data, minions)
        logging.info(filename + "successfully put in storage "+str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))
    except (RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while putting to storage " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))


def get(master, path, fname, mode):
    try:
        file_table = master.get_file_table_entry(path, fname)
        if not file_table:
            print "No such file"
            return
        print file_table
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
                    print "Done"
        logging.info("successfully get from storage " + path +" "+ fname)
    except (RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while getting from storage " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))


def delete(master, path, obj_name):
    try:
        obj_list = master.list(path)
        if obj_name in obj_list:
            if obj_list[obj_name] == 'file':
                file_table = master.get_file_table_entry(path, obj_name)
                if not file_table:
                    print "No such file"
                    return
                for block in file_table:
                    for i in range(len(block[1])):
                        if block[1][i] in master.get_list_of_minions():
                            active_minions = master.get_list_of_minions()
                            for m in active_minions:
                                delete_from_storage(block[0], active_minions[m])
                master.del_file(obj_name)
            elif obj_list[obj_name] == 'dir' and obj_name != '.' and obj_name != '..':
                pass
        logging.info("deleted object from storage " + path + " " + obj_name)
    except(RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " while deleting from storage " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))


def get_keyboard_input(cur_dir):
    sys.stdout.write(bcolors.BOLD + bcolors.GREEN + '~' + cur_dir);
    sys.stdout.flush()

    cmd = sys.stdin.readline()
    parts = cmd.split(' ')
    args = []
    for part in parts:
        args.append(part.strip())
    return args

def main():
    logging.basicConfig(filename='clientlog.log', level=logging.INFO)
    logging.info("logging started " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))
    try:
        con = rpyc.connect("localhost", port=2131)
        master = con.root
        logging.info("connection established " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))
    except (RuntimeError, TypeError, NameError):
        message = RuntimeError.message or TypeError.message or NameError.message
        logging.error(message + " connection failed " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))

    cur_dir = "/"
    print "Client started. Use 'help' to list all available commands."
    args = get_keyboard_input(cur_dir)
    while args[0] != 'exit':
        if args[0] == 'get':
            if len(args) > 1:
                get(master, cur_dir, args[1], 'download')
                logging.info("Download complete " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))
            else:
                print "Filename is not specified. Usage: get <filename>"
        elif args[0] == 'cat':
            if len(args) > 1:
                get(master, cur_dir, args[1], 'open')
                logging.info("viewing file completing " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))
            else:
                print "Filename is not specified. Usage: cat <filename>"
        elif args[0] == 'put':
            if len(args) > 1:
                if len(args) == 3:
                    put(master, args[1], args[2])
                    master.add_obj(cur_dir, args[2], 'file')
                    logging.info("file put " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))
                elif len(args) == 2:
                    fname = os.path.basename(args[1])
                    put(master, args[1], fname, cur_dir)
                    master.add_obj(cur_dir, fname, 'file')
                    logging.info("file put " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))
                else:
                    print "Too many arguments"
            else:
                print "File is not specified. Usage: put <file> [new filename]"
        elif args[0] == 'ls':
            obj_list = master.list(cur_dir)
            for obj in obj_list:
                if obj_list[obj] == 'file':
                    s = master.get_file_size(obj)
                    print Fore.YELLOW + obj + '\t\t' + str(s) + ' bytes'
                else:
                    print Fore.CYAN + bcolors.BOLD + obj
        elif args[0] == 'mkdir':
            if len(args) > 1:
                dirname = args[1]
                if master.check_if_directory_exists(cur_dir, dirname):
                    print "Folder exists"
                else:
                    master.add_obj(cur_dir, dirname)
                    logging.info("directory created " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))
            else:
                print "Directory name is not specified. Usage: mkdir <dirname>"
        elif args[0] == 'cd':
            if len(args) > 1:
                dirname = args[1]
                obj_list = master.list(cur_dir)
                if dirname in obj_list:
                    for obj in obj_list:
                        if obj == dirname and obj_list[obj] == 'dir' and obj != '.' and obj != '..' :
                            cur_dir = cur_dir + obj + '/'
                            break
                        if dirname == '..':
                            a, b, c = cur_dir.rsplit('/', 2)
                            cur_dir = cur_dir[:-(len(b) + 1)]
                            break
                else:
                    print "No such directory"
            else:
                print "Directory name is not specified. Usage: cd <dirname>"
        elif args[0] == 'del':
            if len(args) > 1:
                delete(master, cur_dir, args[1])
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
        logging.info("Logging finished " + str(time.strftime("%d/%m/%Y")+time.strftime("%H:%M:%S")))


if __name__ == "__main__":
    main()