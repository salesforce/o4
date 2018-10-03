import sys
import os
from errno import *


class chdir(object):
    ''' Executes the body of a "with chdir(dir)" block in the given directory.
        Warning: NOT THREAD SAFE
    '''

    def __init__(self, path):
        self.original_dir = os.getcwd()
        self.path = path

    def __enter__(self):
        try:
            os.makedirs(self.path)
        except (OSError, IOError) as e:
            if e.errno not in (EEXIST, EISDIR):
                raise Exception('Error creating %s: %s' % (self.path, e))
        os.chdir(self.path)

    def __exit__(self, etype, evalue, traceback):
        os.chdir(self.original_dir)


def consume(iterator, n=None):
    "Advance the iterator n-steps ahead. If n is None, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        from collections import deque
        deque(iterator, maxlen=0)
    else:
        from itertools import islice
        next(islice(iterator, n, n), None)


class AtomicFile(object):
    '''A file that will be atomically replaced with new data upon closing.
       Usage:
           with AtomicFile(path) as f:
              f.write() ...
       At the conclusion of the with statement, the file will have the
       new contents. The file is not replaced if the body of the with
       raises an error.
       The file need not initially exist.
    '''

    def __init__(self, filename, mode='w'):
        self.original_name = filename
        self.dir = os.path.dirname(filename)
        self.mode = mode

    def __enter__(self):
        import uuid
        import shutil
        try:
            os.makedirs(self.dir)
        except OSError:
            pass
        self.newfile = open(os.path.join(self.dir, str(uuid.uuid1())), 'w')
        if self.mode == 'r+':
            shutil.copyfileobj(open(self.original_name), self.newfile)
            self.newfile.seek(0)
        return self.newfile

    def __exit__(self, type, value, traceback):
        self.newfile.close()
        if type is None:
            os.rename(self.newfile.name, self.original_name)
        else:
            os.remove(self.newfile.name)


class CLMapping(object):
    '''
    Relates a changelist that the user requested to the one that is actually synced.
    I.e., the highest existing changelist less than or equal to the requested one.
    '''
    map_file_name = '.o4/clmapping'

    @staticmethod
    def put(requested_cl, actual_cl, map_file_name=map_file_name):
        requested_cl = str(requested_cl) + ' '
        actual_cl = str(actual_cl) + ' '
        if requested_cl == actual_cl:
            return
        open(map_file_name, 'a')
        with open(map_file_name, 'r+') as old, AtomicFile(map_file_name) as new:
            found = False
            for line in old:
                if line.startswith(actual_cl):
                    if requested_cl not in line:
                        line = line.strip() + ' ' + requested_cl + '\n'
                    found = True
                new.write(line)
            if not found:
                new.write(actual_cl + requested_cl + '\n')

    @staticmethod
    def get(requested_cl, map_file_name=map_file_name):
        '''
        Returns the actual changelist for the requested one; returns the requested
        one if there is no mapping.
        '''
        requested_cl = ' ' + str(requested_cl) + ' '
        try:
            with open(map_file_name) as f:
                for line in f:
                    if requested_cl in line:
                        return int(line.split()[0])
        except OSError:
            pass
        return int(requested_cl)
