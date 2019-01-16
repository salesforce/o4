#!/usr/bin/env python3

import sys
import os
from os import O_RDWR, O_CREAT, O_EXCL, SEEK_END
import time
import re
from glob import glob
from errno import EEXIST, EISDIR
from subprocess import check_call, check_output, CalledProcessError

if 'BLT_HOME' in os.environ:
    sys.path.insert(0, f'{os.environ["BLT_HOME"]}/blt-code/plugins/perforce')

from o4_pyforce import Pyforce
from o4_utils import chdir
from o4server import url

PACKAGE_LOCK_FILE = '.o4/packagelock'


class O4Locations:
    '''
    Used to keep a record of all o4 directories that have had
    fstat or archive files created.
    '''

    def __init__(self):
        self.registry = f'{os.getcwd()}/o4locations'
        try:
            os.close(os.open(self.registry, O_RDWR | O_CREAT | O_EXCL))
        except FileExistsError:
            pass

    def __setitem__(self, o4dir, _):
        with open(self.registry, 'r+') as f, filelock(f):
            dirs = f.readlines()
            if o4dir + '\n' not in dirs:
                dirs.append(o4dir + '\n')
                f.seek(0)
                f.truncate()
                f.writelines(dirs)

    def __contains__(self, o4dir):
        with open(self.registry, 'r+') as f, filelock(f):
            return o4dir + '\n' in f.readlines()

    def __call__(self):
        with open(self.registry, 'r+') as f, filelock(f):
            return [s.strip() for s in f.readlines()]


o4locations = O4Locations()


class filelock(object):
    '''Executes the body of a "with filelock(file)" block while holding
       a write lock on the file.
       Warning: This may change the file position.
    '''

    def __init__(self, f, block=True):
        self.f = f.fileno() if hasattr(f, 'fileno') else f
        self.block = block

    def __enter__(self):
        os.lseek(self.f, 0, 0)
        return os.lockf(self.f, os.F_LOCK if self.block else os.F_TLOCK, 1) == 0

    def __exit__(self, etype, evalue, traceback):
        os.lseek(self.f, 0, 0)
        os.lockf(self.f, os.F_ULOCK, 1)


def lock_package(depot_path):
    TIMEOUT = 90 * 60
    with local_depot(depot_path):
        now = int(time.time())
        os.makedirs(os.path.dirname(PACKAGE_LOCK_FILE), exist_ok=True)
        with create_file(PACKAGE_LOCK_FILE) as f:
            with filelock(f):
                if f.seek(0, os.SEEK_END) == 0:
                    f.write(str(now))
                    return True
                f.seek(0)
                locktime = int(f.readline())
                if now - locktime > TIMEOUT:
                    f.seek(0)
                    f.truncate()
                    f.write(str(now))
                    return True
                return False


def unlock_package(depot_path):
    with local_depot(depot_path):
        with create_file(PACKAGE_LOCK_FILE) as f:
            with filelock(f):
                f.truncate(0)


def p4_local(depot_path):
    py = list(Pyforce('where', depot_path))[0]
    if 'path' not in py:
        raise KeyError(f'Error resolving depot path {depot_path}')
    dir = py['path'].replace('/...', '')
    return dir


def set_p4_password():
    os.environ['P4PASSWD'] = shared['p4password']


def local_depot(depot_path):
    dir = p4_local(depot_path)
    return chdir(dir)


def archive_exists(depot_path, changelist):
    safe_path = depot_path.replace('//', '').replace('/', '__')
    with local_depot(depot_path):
        archive = f'{os.getcwd()}/.o4/{changelist}.{safe_path}.tgz'
        return archive if os.path.exists(archive) else None


def create_file(path):
    try:
        return os.fdopen(os.open(PACKAGE_LOCK_FILE, O_RDWR | O_CREAT | O_EXCL), 'r+')
    except FileExistsError:
        return open(PACKAGE_LOCK_FILE, 'r+')


def check_nearby(files, changelist, nearby):
    'Return a redirect status and path if a nearby request can be fulfilled.'

    cls = (os.path.basename(f) for f in files)
    allcls = sorted(int(f.split('.')[0]) for f in cls)
    cls = [c for c in allcls if (changelist - nearby) < c < changelist]
    if cls:
        cl = cls[-1]
        code = 302 if cl == allcls[-1] else 301
        return code, cl
    return None, None


def get_fstat(path, changelist, nearby):
    set_p4_password()
    o4dir = f'{p4_local(path)}/.o4'
    o4locations[o4dir] = True
    if os.path.exists(f'{o4dir}/{changelist}.fstat.gz'):
        return 200, f'{o4dir}/{changelist}.fstat.gz'
    if nearby:
        files = glob(os.path.join(o4dir, '*.gz'))
        code, cl = check_nearby(files, changelist, nearby)
        if code:
            return code, f'{o4dir}/{cl}.fstat.gz'
    out = check_output(
        ['o4', 'fstat', '-q', '--report', 'actual_cl={actual_cl}', f'{path}@{changelist}'])
    if os.path.exists(f'{o4dir}/{changelist}.fstat.gz'):
        return 200, f'{o4dir}/{changelist}.fstat.gz'
    for line in out.split(b'\n'):
        if line.startswith(b'actual_cl='):
            cl = line.replace(b'actual_cl=', b'').decode('utf-8')
            return 301, f'{o4dir}/{cl}.fstat.gz'
    raise Exception(f'Unable to determine new fstat file.\n{out}')


def get_archive(path, changelist, nearby):
    set_p4_password()
    safe_path = path.replace('//', '').replace('/...', '').replace('/', '__')
    o4dir = f'{p4_local(path)}/.o4'
    o4locations[o4dir] = True
    if lock_package(path):
        code = 200
        try:
            with local_depot(path):
                archive = archive_exists(path, changelist)
                if archive:
                    return 200, archive
                if nearby:
                    files = glob(os.path.join(o4dir, '*.tgz'))
                    code2, cl = check_nearby(files, changelist, nearby)
                    if code2:
                        return code2, f'{o4dir}/{cl}.{safe_path}.tgz'
                out = check_output(
                    ['o4', 'fstat', '-q', f'.@{changelist}', '--report', 'actual_cl={actual_cl}'])
                fstat = f'.o4/{changelist}.fstat.gz'
                if not os.path.exists(fstat):
                    for line in out.split(b'\n'):
                        if line.startswith(b'actual_cl='):
                            cl = line.replace(b'actual_cl=', b'').decode('utf-8')
                            return 301, f'{cl}.{safe_path}.tgz'
                check_call(['o4', 'sync', f'.@{changelist}', '-q'])
                archive = f'{o4dir}/{changelist}.{safe_path}.tgz'
                if not os.path.exists(archive):
                    os.link(fstat, os.path.basename(fstat))
                    try:
                        check_call(['tar', '-c', '--gzip', '-f', archive, '--exclude', '.o4', '.'])
                    finally:
                        os.unlink(os.path.basename(fstat))
        finally:
            unlock_package(path)
        return code, archive
    else:
        return 202, None


def get_available_changelists(path):
    set_p4_password()
    # Look at both .gz and .tgz files. Union the changelist numbers.
    changelists = set()
    path = os.path.join(p4_local(path), '.o4', '*gz')
    for f in glob(path):
        try:
            changelists.add(int(os.path.basename(f).partition('.')[0]))
        except ValueError:
            pass
    return [str(c) for c in sorted(changelists, reverse=True)]
