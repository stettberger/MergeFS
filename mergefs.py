#!/usr/bin/python

import re
import sys, os
import logging
from subprocess import *
import random
import shutil


real_operation = True

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

class Filesystem(object):
    _objects = {}
    @classmethod
    def get(self, path):
        (root, info) = filesystem_info(path)

        if not root in self._objects:
            self._objects[root] = Filesystem(root)
        return self._objects[root]

    def __init__(self, path):
        self.path = path
        (_, self.info) = filesystem_info(self.path)

    def enough_space(self, size):
        return self.info['free'] > size

    def free(self):
        return self.info['free']
    def consume(self, size):
        self.info['free'] += size


class Datastore(object):
    def __init__(self, path):
        if not os.path.isdir(path):
            die("Datastore '%s' doesn't exist or is no directory"%path)

        self.path = os.path.normpath(os.path.abspath(path))
        self.files = set([])
        self.symlinks = {}
        for root, dirs, files in os.walk(path):
            for fn in files:
                fn = os.path.join(root, fn)
                rel_fn = fn[len(self.path)+1:]
                if os.path.islink(fn):
                    self.symlinks[rel_fn] = os.readlink(fn)
                elif os.path.isfile(fn):
                    self.files.add(rel_fn)
        # Update Filesystem info
        self.filesystem = Filesystem.get(path)

    def store_score(self):
        """Calculate score for the filestore. This is used by choosing algorithm
for file distribution"""
        return self.filesystem.free()

    def has_symlink(self, fn):
        return fn in self.symlinks

    def has_file(self, fn):
        return fn in self.files

    def send(self, fn, datastore, remove=True):
        assert datastore != self
        old_fn = os.path.join(self.path, fn)
        new_fn = os.path.join(datastore.path, fn)
        mkdir(os.path.dirname(new_fn))

        logging.info("%s -> %s" %(old_fn, new_fn))

        if real_operation:
            shutil.copy2(old_fn, new_fn)
            if remove:
                os.unlink(old_fn)

            # Filesize in Kilobyte approximated
            size = os.stat(new_fn).st_size / 1024 + 1
        else:
            size = os.stat(old_fn).st_size / 1024 + 1

        # Update filesystem Info
        if remove:
            self.filesystem.consume(-1 * size)
            self.files.remove(fn)

        datastore.filesystem.consume(size)
        datastore.files.add(fn)

        return (old_fn, new_fn)


class DatastoreManager(object):
    def __init__(self):
        self._stores = {}
        self._mergedir = None

    def mergedir(self):
        """Return the mergedir datastore"""
        return self._stores[self.mergedir]
    def add(self, path, mergedir = False):
        if mergedir:
            self._mergedir = path
        else:
            # Mergedir can't be added twice
            if path == self._mergedir:
                die("Can't reuse mergedir as datastore")

        if path in self._stores:
            return self._stores[path]
        ds = Datastore(path)
        self._stores[path] = ds
        return ds

def distribute(mergedir, stores):
    def select_datastore(filename):
        """Select the datastore for a given filename"""
        # Filesize in Kilobyte approximated
        size = os.stat(os.path.join(mergedir.path, filename)).st_size / 1024 + 1
        st = filter(lambda store: store.filesystem.enough_space(size), stores)
        st = sorted(st, key = lambda store: store.store_score())
        if len(st) == 0:
            return None

        # Select Datastore with less used space
        store = st[-1]
        return store # Some appropriate filesystemstore

    # Iterate over all normal files in mergedir
    for fn in mergedir.files:
        selected_store = select_datastore(fn)
        if not selected_store:
            logging.warn("No appropriate store for %s was found" % fn)

        (old, new) = mergedir.send(fn, selected_store)
        mergedir.symlinks[fn] = new
        if real_operation:
            os.symlink(new, old)


def unused(mergedir, stores):
    for store in stores:
        for fn in store.files:
            if not fn in mergedir.symlinks:
                print os.path.join(mergedir.path, fn)

def fixup(mergedir, stores):
    for store in stores:
        for fn in store.files:
            if not fn in mergedir.symlinks:
                from_fn = os.path.join(store.path, fn)
                to_fn = os.path.join(mergedir.path, fn)
                logging.info("symlink %s -> %s" %(from_fn, to_fn))
                mergedir.symlinks[fn] = to_fn

                if real_operation:
                    mkdir(os.path.dirname(to_fn))
                    os.symlink(from_fn, to_fn)

def get_num_copy_dict(stores):
    d = {}
    for store in stores:
        for fn in store.files:
            (count, stores) = d.get(fn, (0, []))
            d[fn] = (count + 1, stores + [store])
    return d

def get_copies(stores):
    for fn, (count, stores) in get_num_copy_dict(stores).items():
        print count, fn, ",".join([store.path for store in stores])

def balance(stores, min_num_copies):
    for fn, (count, used_stores) in get_num_copy_dict(stores).items():
        if count < min_num_copies:
            # Filesize in Kilobyte approximated
            size = os.stat(os.path.join(used_stores[0].path, fn)).st_size / 1024 + 1
            st = filter(lambda store: store.filesystem.enough_space(size), stores)
            st = filter(lambda store: not store in used_stores, st)
            st = sorted(st, key = lambda store: store.store_score())
            if len(st) == 0:
                continue

            # Select Datastore with less used space
            store = st[-1]
            logging.info("copy %s -> %s" %(os.path.join(used_stores[0].path, fn),
                                           os.path.join(store.path, fn)))
            if real_operation:
                used_stores[0].send(fn, store, remove=False)

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()

    parser.add_option("-s", "--simulate", dest="simulate", action="store_true",
                      help="Simulate all operations", default = False)

    parser.add_option("-m", "--mergedir", dest="mergedir", action="store",
                      help="Directory where the symlinks will be located")

    parser.add_option("", "--distribute", dest="distribute", action="store",
                      help="Distribute all normal files from mergedir into the data stores")
    parser.add_option("", "--unused", dest="unused", action="store",
                      help="Find all unused objects in the stores, that have no symlink")
    parser.add_option("", "--fixup", dest="fixup", action="store",
                      help="Add all symlinks for files, that aren't in the merge directory")

    parser.add_option("", "--get-num-copies", dest="get_copies", action="store",
                      help="Get number of copies for every file in the store")

    parser.add_option("", "--min-num-copies", dest="min_num_copies", action="store",
                      help="Get number of copies for every file in the store",
                      default = 1)

    parser.add_option("-b", "--balance", dest="balance", action="store",
                      help="Get number of copies for every file in the store")

    parser.add_option('-v', '--verbose', dest='verbose', action='count',
                      help="Increase verbosity (specify multiple times for more)")

    (options, args) = parser.parse_args()

    log_level = logging.WARNING # default
    if options.verbose == 1:
        log_level = logging.INFO
    elif options.verbose >= 2:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)

    real_operation = not options.simulate


    if len(args) > 0:
        die("Don't use extra args")

    dsm = DatastoreManager()

    if options.distribute or options.fixup or options.unused:
        if not options.mergedir:
            die("No mergedir was given")
        mergedir = dsm.add(options.mergedir, mergedir = True)

    if options.distribute:
        stores = options.distribute.split(",")
        stores = [dsm.add(store) for store in stores]
        distribute(mergedir, stores)

    if options.fixup:
        stores = options.fixup.split(",")
        stores = [dsm.add(store) for store in stores]
        fixup(mergedir, stores)

    if options.balance:
        stores = options.balance.split(",")
        stores = [dsm.add(store) for store in stores]
        balance(stores, int(options.min_num_copies))

    if options.get_copies:
        stores = options.get_copies.split(",")
        stores = [dsm.add(store) for store in stores]
        get_copies(stores)

    if options.unused:
        stores = options.unused.split(",")
        stores = [dsm.add(store) for store in stores]
        unused(mergedir, stores)

