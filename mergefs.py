#!/usr/bin/python

import re
import sys, os
import logging
from subprocess import *
import random
import shutil


def die(msg):
    logging.error(msg)
    sys.exit(-1)

def execute(command, echo=True):
    """
    executes 'command' in a shell

    returns a tuple with 
     1. the command's standard output as list of lines
     2. the exitcode
    """

    if echo:
        logging.debug("executing: %s" % command)
    p = Popen(command, stdout=PIPE, stderr=STDOUT)
    (stdout, stderr) = p.communicate()
    return (stdout.rsplit('\n')[:-1], p.returncode)

def filesystem_info(fn):
    """Returns the filesystem info for the given filename"""
    (lines, code) = execute(["df", "-P", fn])
    if code != 0 or len(lines) != 2:
        die("Error while doing df")
    info = re.split("\s+", lines[1])
    if len(info) != 6:
        die("Wrong format from df")
    return (info[5], {'free': int(info[2]), 'capacity': int(info[1])})

def mkdir(dirname):
    (lines, code) = execute(["mkdir", "-p", dirname])
    if  code != 0:
        die("Error while creating directory: %s\n%s" % (dirname, "\n".join(lines)))

def distribute(options):
    filesystems = {}
    stores = []
    def update_filesysteminfo():
        for store in options['stores']:
            # Absolute directory name
            store = os.path.join(os.getcwd(), store)
            (filesystem, info) = filesystem_info(store)
            stores.append((store, filesystem))
            filesystems[filesystem] = info

    update_filesysteminfo()

    def select_datastore(filename):
        """Select the datastore for a given filename"""
        # Filesize in Kilobyte approximated
        size = os.stat(filename).st_size / 1024 + 1
        st = filter(lambda store: filesystems[store[1]]['free'] > size, stores)
        st = sorted(st, key = lambda store: filesystems[store[1]]['free'])
        if len(st) == 0:
            return None

        # Select Datastore with less used space
        store = st[-1]
        filesystems[store[1]]['free'] -= size

        return store # Some appropriate filesystemstore

    # Change to Directory where the to be distributed files are located
    os.chdir(options['mergedir'])

    for root, dirs, files in os.walk('.'):
        for fn in files:
            fn = os.path.join(root, fn)
            if not os.path.isfile(fn) or os.path.islink(fn):
                logging.debug("Skipping file: %s" % fn)
                continue
            store = select_datastore(fn)
            if not store:
                logging.warn("Couldn't get appropriate store for file: %s" % fn)
                continue
            path = os.path.normpath(os.path.join(store[0], fn))
            # Create the directory in the choosen datastore
            mkdir(os.path.dirname(path))

            logging.info("%s -> %s" %(fn, path))

            shutil.copy2(fn, os.path.dirname(path))
            os.unlink(fn)
            os.symlink(path, fn)

def unused(options):
    symlinks = {}
    stores = map(lambda store: os.path.normpath(os.path.join(os.getcwd(), store)), options['stores'])

    # Change to Directory where the to be distributed files are located
    os.chdir(options['mergedir'])
    for root, dirs, files in os.walk('.'):
        for fn in files:
            fn = os.path.join(root, fn)
            if not os.path.islink(fn):
                continue
            symlinks[os.path.abspath(os.readlink(fn))] = fn
    for store in stores:
        for root, dirs, files in os.walk(store):
            for fn in files:
                fn = os.path.join(root, fn)
                if not fn in symlinks:
                    print fn

def fixup(options):
    symlinks = {}
    mergedir = os.path.abspath(options['mergedir'])
    stores = map(lambda store: os.path.normpath(os.path.join(os.getcwd(), store)), options['stores'])

    for store in stores:
        os.chdir(store)
        for root, dirs, files in os.walk("."):
            for fn in files:
                fn = os.path.join(root, fn)
                absfn = os.path.abspath(fn)
                if not os.path.isfile(fn) or os.path.islink(fn):
                    continue
                if os.path.exists(os.path.join(mergedir, fn)):
                    continue
                mergefn = os.path.normpath(os.path.join(mergedir, fn))

                logging.info("symlink %s -> %s" %(absfn, mergefn))
                mkdir(os.path.dirname(mergefn))
                os.symlink(absfn, mergefn)

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("", "--distribute", dest="distribute", action="store_true",
                      help="Distribute all normal files from mergedir into the data stores")
    parser.add_option("", "--unused", dest="unused", action="store_true",
                      help="Find all unused objects in the stores, that have no symlink")
    parser.add_option("", "--fixup", dest="fixup", action="store_true",
                      help="Add all symlinks for files, that aren't in the merge directory")
    parser.add_option('-v', '--verbose', dest='verbose', action='count',
                      help="Increase verbosity (specify multiple times for more)")

    (options, args) = parser.parse_args()

    log_level = logging.WARNING # default
    if options.verbose == 1:
        log_level = logging.INFO
    elif options.verbose >= 2:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)

    if len(args) < 2:
        die("Usage: %s <mergedir> <store1> [<store2> ...]")

    mergedir = args[0]
    stores = args[1:]
    options =  eval(str(options))
    options.update({'mergedir': args[0], 'stores': args[1:]})

    if not os.path.isdir(options['mergedir']):
        die("Mergedir doesn't exist or is no directory")
    for d in options['stores']:
        if not os.path.isdir(d):
            die("Store: %s doesn't exist or is no directory" % d)

    if options['distribute']:
        distribute(options)
    elif options['unused']:
        unused(options)
    elif options['fixup']:
        fixup(options)
    else:
        die("No actions was given (-h for details)")

