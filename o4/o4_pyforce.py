#!/usr/bin/env python3.6
"""
Wrappers for the p4 binary.
"""

import os
import sys
import time
import pickle


class P4Error(Exception):
    '''Raised when there is an error in the p4 result'''


class P4TimeoutError(Exception):
    '''Raised when a p4 command times out'''


class Pyforce(object):

    def __init__(self, *args):
        """
        Create an iterator over results of a p4 call. The args here are p4
        CLI arguments. See p4 help for more information.
        """
        self.args = [str(arg) for arg in args]
        from subprocess import Popen, PIPE
        from tempfile import NamedTemporaryFile
        self.stderr = NamedTemporaryFile()
        if os.environ.get('DEBUG', ''):
            print(f'## p4', *self.args, file=sys.stderr)
        self.pope = Popen(['p4', '-vnet.maxwait=60', '-G'] + self.args,
                          stdout=PIPE,
                          stderr=self.stderr)
        self.transform = Pyforce.to_str
        self.errors = []

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns the next p4 result object from the command. If the p4
        command experiences a timeout, raise P4TimeoutError. All other
        errors are accumulated during the run and raised as arguments
        on a single P4Error object after the p4 process has been
        exhausted.

        Certain errors are not really errors, it's just p4 being
        silly. Such as the error "No files to reconcile" when you
        reconcile files that have the correct content. Such errors are
        converted to code=stat and passed on. Some may also produce a
        '#o4pass'-prefixed line out stdout, which, in a complete run,
        will make their way to "o4 fail" and be reported.
        """
        import marshal
        try:
            while True:
                res = marshal.load(self.pope.stdout)
                if res.get(b'code') == b'info' and res.get(b'data', ''):
                    data = res.get(b'data')
                    ## Why was this upped to error?
                    #  b"is opened and not being changed" in data or b"must resolve" in data) and
                    if data.startswith(b'Diff chunks') and not data.endswith(b'+ 0 conflicting'):
                        print("*** WARNING: There are conflicts.", file=sys.stderr)
                    elif (b"can't move (already opened for edit)" in data or
                          b"is opened for add and can't be replaced" in data or
                          b"- resolve skipped" in data):
                        res[b'code'] = b'stat'
                        print(f'#o4pass-warn#{data.decode("utf-8",errors="ignore")}')
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
                    elif (b'no file(s) at that changelist number' in res[b'data'] or
                          b'no revision(s) above those at that changelist number' in res[b'data']):
                        # print('*** INFO: Skipping premature sync: ', res)
                        res[b'code'] = b'skip'
                    elif b'clobber writable file' in res[b'data']:
                        res[b'code'] = b'error'
                    # {b'code': b'error', b'data': b'SSL receive failed.\nread: Connection timed out: Connection timed out\n', b'severity': 3, b'generic': 38}
                    # 'data': 'TCP receive exceeded maximum configured duration of 60 seconds.\n', 'severity': 3, 'generic': 38
                    # This seems like it could be 100 different messages; we probably need #TODO find out what generic means.
                    elif b'Connection timed out' in res[b'data'] or b'TCP receive exceeded' in res[
                            b'data']:
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
            self.errors.append({
                'code': 'error',
                'data': f'stderr: {err}',
                'severity': 3,
                'generic': 38
            })
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
        """
        Converts a dictionary of bytes key-values to strings using stdout
        encoding.
        """

        def dec(a):
            if hasattr(a, 'decode'):
                return a.decode(sys.stdout.encoding, errors='ignore')
            return a

        return {dec(k): dec(v) for k, v in r.items()}

    @staticmethod
    def unescape(path):
        """Reverts p4 path escaping."""
        return path.replace('%40', '@').replace('%23', '#').replace('%2a', '*').replace('%25', '%')

    @staticmethod
    def escape(path):
        """Escapes a path like perforce would."""
        return path.replace('%', '%25').replace('#', '%23').replace('*', '%2a').replace('@', '%40')

    @staticmethod
    def checksum(fname, fileSize):
        """
        Probably the only complete resource to how perforce computes a
        checksum. Fundamentally it's a MD5 checksum of the file's
        content. However utf16 files must first be converted to utf8,
        and if the file system file size is 3 bytes larger than the
        stated file size, then if those three bytes are the utf8 BOM,
        they must not be included in the checksum.

        Hence the fileSize argument can be an integer, or in the case
        of utf8 files <int>/utf8, and in the utf16 case <int>/utf16.
        """
        import hashlib
        hash_md5 = hashlib.md5()
        headType = ''
        if type(fileSize) != int:
            if '/' in fileSize:
                fileSize, headType = fileSize.split('/', 1)
            fileSize = int(fileSize)
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
                    print(f'*** INFO: Using cached result for {cmd} from {cname}', file=sys.stderr)
                return res
    except (pickle.UnpicklingError, FileNotFoundError, EOFError):
        pass


def _cache_put(cmd, pyf):
    cname = os.path.expanduser('~/.o4/.' + cmd)
    print(f'*** INFO: Caching result for {cmd} into {cname}', file=sys.stderr)
    os.makedirs(os.path.dirname(cname), exist_ok=True)
    with open(cname, 'wb') as fout:
        res = list(pyf)
        pickle.dump({os.environ['P4CLIENT']: res}, fout)
        return res


def info():
    """
    Returns the server info. Reply is cached for future reference.
    """
    res = _cache_get('info')
    if not res:
        res = _cache_put('info', Pyforce('info'))
    return res[0]


def client():
    """
    Returns the clientspec object. Reply is cached for future
    reference.
    """
    res = _cache_get('client')
    if not res:
        res = _cache_put('client', Pyforce('client', '-o'))
    return res[0]


def clear_cache(cmd):
    '''Remove the cached copy of the "cmd" data.
       Returns True if there was a cache, False otherwise.
    '''
    try:
        cname = os.path.expanduser(f'~/.o4/.{cmd}')
        os.remove(cname)
        print(f'*** INFO: Removed cached result for {cmd}')
        return True
    except:
        return False


def head(depot_path):
    """Returns the head changelist of depot_path."""
    if not depot_path.endswith('/...'):
        depot_path += '/...'
    return int(
        list(Pyforce('changes', '-s', 'submitted', '-m1', Pyforce.escape(depot_path)))[0]['change'])


##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
