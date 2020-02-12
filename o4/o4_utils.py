import sys
import os
from errno import *


class chdir(object):
    """
    Executes the body of a "with chdir(dir)" block in the given
    directory. Warning: NOT THREAD SAFE
    """

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
    """
    Advance the iterator n-steps ahead. If n is None, consume
    entirely.
    """
    # Use functions that consume iterators at C speed.
    if n is None:
        from collections import deque
        deque(iterator, maxlen=0)
    else:
        from itertools import islice
        next(islice(iterator, n, n), None)


class AtomicFile(object):
    """
    A file that will be atomically replaced with new data upon
    closing.

    Usage:
        with AtomicFile(path) as f:
            f.write()
            ...

    At the conclusion of the with statement, the file will have the
    new contents. The file is not replaced if the body of the with
    raises an error. The file need not initially exist.
    """

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


def caseful_accurate(fname, dirname_cache={}):
    """
    Verifies that fname is the true caseful path to fname according to
    current file system state.
    """

    if sys.platform == 'darwin' and os.path.lexists(fname):
        while fname != '.':
            try:
                dname, bname = fname.rsplit('/', 1)
            except ValueError:
                dname = '.'
                bname = fname
            dlist = dirname_cache.get(dname)
            if not dlist:
                dlist = os.listdir(dname)
                dirname_cache[dname] = dlist
            if bname not in dlist:
                return False
            fname = dname
    return True


def o4_log(operation, *args, **kw):
    """
    Simple logging function for each invocation. Not meant to log more
    than one line per invoked command. Enables us to go back and study
    the assumptions/preconditions at the time.
    """
    import time
    with open(f'.o4/{operation}.log', 'at+') as fout:
        print(time.ctime(),
              operation,
              *[f'{k}={v}' for k, v in kw.items()],
              *args,
              sep='\t',
              file=fout)


##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
