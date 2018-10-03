#!/usr/bin/env python3.6
""" Wrappers for the p4 binary.
"""

import os
import sys
import time
import pickle


class P4Error(Exception):
    """Raised when there is an error in the p4 result"""


class P4TimeoutError(Exception):
    """Raised when a p4 command times out"""


class Pyforce(object):

    def __init__(self, *args):
        self.args = [str(arg) for arg in args]
        from subprocess import Popen, PIPE
        from tempfile import NamedTemporaryFile
        self.stderr = NamedTemporaryFile()
        if os.environ.get('DEBUG', ''):
            print(f"## p4", *self.args, file=sys.stderr)
        self.pope = Popen(
            ['p4', '-vnet.maxwait=60', '-G'] + self.args, stdout=PIPE, stderr=self.stderr)
        self.transform = Pyforce.to_str
        self.errors = []

    def __iter__(self):
        return self

    def __next__(self):
        import marshal
        try:
            while True:
                res = marshal.load(self.pope.stdout)
                if res.get(
                        b'code') == b'info' and b"can't move (already opened for edit)" in res.get(
                            b'data', ''):
                    res[b'code'] = b'error'
                if res.get(b'code') != b'error':
                    return self.transform(res)
                if b'data' in res:
                    if (b'file(s) up-to-date' in res[b'data'] or
                            b'no file(s) to reconcile' in res[b'data'] or
                            b'no file(s) to resolve' in res[b'data'] or
                            b'no file(s) to unshelve' in res[b'data'] or
                            b'file(s) not on client' in res[b'data'] or
                            b'No shelved files in changelist to delete' in res[b'data']):
                        res[b'code'] = b'stat'
                    elif b'no file(s) at that changelist number' in res[b'data']:
                        # print('*** INFO: Skipping premature sync: ', res)
                        res[b'code'] = b'skip'
                    elif b'clobber writable file' in res[b'data']:
                        res[b'code'] = b'error'
                    # {b'code': b'error', b'data': b'SSL receive failed.\nread: Connection timed out: Connection timed out\n', b'severity': 3, b'generic': 38}
                    elif b'Connection timed out' in res[b'data']:
                        raise P4TimeoutError(res, self.args)
                    if res[b'code'] != b'error':
                        return self.transform(res)
                # Allow operation to complete and report errors after
                self.errors.append(Pyforce.to_str(res))
        except EOFError:
            pass
        if self.stderr.tell():
            self.stderr.seek(0)
            err = self.stderr.read().decode(sys.stdout.encoding)
            if 'timed out' in err:
                raise P4TimeoutError(err)
            self.errors.append(f'stderr: {err}')
        if self.errors:
            raise P4Error(*self.errors)
        raise StopIteration()

    def __del__(self):
        if hasattr(self, 'pope'):
            try:
                self.pope.kill()
                self.pope.wait()
            except OSError:
                pass

    @staticmethod
    def to_str(r):

        def dec(a):
            if hasattr(a, 'decode'):
                return a.decode(sys.stdout.encoding)
            return a

        return {dec(k): dec(v) for k, v in r.items()}

    @staticmethod
    def unescape(path):
        return path.replace('%40', '@').replace('%23', '#').replace('%2a', '*').replace('%25', '%')

    @staticmethod
    def escape(path):
        return path.replace('%', '%25').replace('#', '%23').replace('*', '%2a').replace('@', '%40')

    @staticmethod
    def checksum(fname, headType, fileSize=0):
        import hashlib
        hash_md5 = hashlib.md5()
        try:
            with open(fname, 'rb') as f:
                if headType == 'utf16':
                    # FIXME: Don't overflow and die if there is a giant utf16 file
                    u = f.read().decode('utf16')
                    hash_md5.update(u.encode('utf8'))
                else:
                    if headType == 'utf8':
                        fs = os.fstat(f.fileno())
                        if fs.st_size > fileSize:
                            # Skip utf8 BOM when computing digest, if filesize differs from st_size
                            bom = f.read(3)
                            if bom != b'\xef\xbb\xbf':
                                f.seek(0)
                    for chunk in iter(lambda: f.read(1024 * 1024), b''):
                        hash_md5.update(chunk)
            return hash_md5.hexdigest().upper()
        except FileNotFoundError:
            return None


def changes(depot_path, lower, upper=None):
    # Currently not used
    lower = int(lower)
    revs = '@{},@{}'.format(lower, upper)
    if not upper:
        import time
        future = time.gmtime(time.time() + 48 * 3600)
        revs = f'@{lower+1},{future.tm_year:04}/{future.tm_mon:02}/{future.tm_mday:02}'
    return sorted(
        int(f[b'change']) for f in Pyforce('changes', '-s', 'submitted', '{}{}'.format(
            Pyforce.escape(depot_path), revs)))


def _cache_get(cmd, max_age=24 * 3600):
    cname = os.path.expanduser('~/.o4/.' + cmd)
    try:
        st = os.stat(cname)
        if time.time() - st.st_ctime > max_age:
            os.unlink(cname)
        else:

            with open(cname, 'rb') as fin:
                res = pickle.load(fin).get(os.environ['P4CLIENT'], None)
                if res:
                    print(f"*** INFO: Using cached result for {cmd} from {cname}", file=sys.stderr)
                return res
    except (pickle.UnpicklingError, FileNotFoundError):
        pass


def _cache_put(cmd, pyf):
    cname = os.path.expanduser('~/.o4/.' + cmd)
    print(f"*** INFO: Caching result for {cmd} into {cname}", file=sys.stderr)
    os.makedirs(os.path.dirname(cname), exist_ok=True)
    with open(cname, 'wb') as fout:
        res = list(pyf)
        pickle.dump({os.environ['P4CLIENT']: res}, fout)
        return res


def info():
    res = _cache_get('info')
    if not res:
        res = _cache_put('info', Pyforce('info'))
    return res[0]


def client():
    res = _cache_get('client')
    if not res:
        res = _cache_put('client', Pyforce('client', '-o'))
    return res[0]


def head(depot_path):
    if not depot_path.endswith('/...'):
        depot_path += '/...'
    return int(
        list(Pyforce('changes', '-s', 'submitted', '-m1', Pyforce.escape(depot_path)))[0]['change'])
