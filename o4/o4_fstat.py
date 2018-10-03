#!/usr/bin/env python3.6
import os
import sys
import gzip

from o4_progress import progress_iter

F_CHANGELIST, F_REVISION, F_FILE_SIZE, F_LAST_ACTION, F_FILE_TYPE, F_CHECKSUM, F_PATH = range(7)


def fstat_sort(f):
    return int(f[F_CHANGELIST]), f[F_PATH]


def fstat_from_csv(fname, split=True):
    if fname == '-':
        for line in sys.stdin:
            if line and line[0] == '\n' or line[0] == '#':
                continue
            r = line[:-1].split(',', 6)
            assert len(r) == 7, f"LINE: {line!r} ARGS{sys.argv}"
            yield r
        return
    with gzip.open(fname, 'rt', encoding='utf8') as fin:
        for line in progress_iter(fin, fname, 'scan'):
            if line and line[0] != '\n' and line[0] != '#':
                if split:
                    r = line[:-1].split(',', 6)
                    assert len(r) == 7, f"LINE: {line!r} ARGS{sys.argv}"
                    yield r
                else:
                    yield line[:-1]


def get_fstat_cache(changelist, o4_dir='.o4'):
    import glob
    fstats = glob.glob(f'{o4_dir}/*.fstat.gz')
    cls = sorted((int(os.path.basename(f).split('.', 1)[0]) for f in fstats),
                 key=lambda x: (abs(x - changelist), x))
    cls = [c for c in cls if c <= changelist]
    if cls:
        return cls[0], f"{o4_dir}/{cls[0]}.fstat.gz"
    return None, None


def fstat_iter(depot_path, to_changelist, from_changelist=0, cache_dir='.o4'):
    from tempfile import mkstemp
    from o4_pyforce import P4TimeoutError, P4Error

    to_changelist, from_changelist = int(to_changelist), int(from_changelist)
    cache_cl, cache_fname = get_fstat_cache(to_changelist, cache_dir)
    updated = []
    filenames = set()
    CLR = '%c[2K\r' % chr(27)

    try:
        fout = temp_fname = first = None
        fh, temp_fname = mkstemp(dir=cache_dir)
        os.close(fh)
        fout = gzip.open(temp_fname, 'wt', encoding='utf8', compresslevel=9)

        if cache_cl == to_changelist:
            with gzip.open(cache_fname, 'rt', encoding='utf8') as fin:
                for line in fin:
                    if not line or line[0] == '\n' or line[0] == '#':
                        continue
                    cl, tail = line.split(',', 1)
                    if int(cl) < from_changelist:
                        break
                    yield line[:-1]
            return

        retry = 3
        while retry:
            retry -= 1
            try:
                for f in retrieve_fstats(depot_path, to_changelist, cache_cl):
                    if from_changelist < int(f[F_CHANGELIST]) <= to_changelist:
                        yield ','.join(f)
                    if cache_cl:
                        filenames.add(f[F_PATH])
                    updated.append(f)
                break
            except P4Error as e:
                fix = False
                for a in e.args:
                    if 'Too many rows scanned' in a.get('data', ''):
                        if cache_cl:
                            print(
                                f"{CLR}*** WARNING: Maxrowscan occurred, ignoring cache {cache_fname}@{cache_cl}",
                                file=sys.stderr)
                            fix = True
                            cache_cl = cache_fname = None
                            retry += 1
                if not fix:
                    raise
            except P4TimeoutError:
                updated = []
                filenames = set()
                print(
                    f"{CLR}*** WARNING: ({retry+1}/3) P4 Timeout while getting fstat",
                    file=sys.stderr)
        else:
            sys.exit(f"{CLR}*** ERROR: "
                     f"Too many P4 Timeouts for p4 fstat"
                     f"{depot_path}@{from_changelist},@{to_changelist}")

        if updated:
            updated.sort(reverse=True, key=fstat_sort)
            first = updated[0][F_CHANGELIST]
            print(
                "# COLUMNS: F_CHANGELIST, F_REVISION, F_FILE_SIZE,",
                "F_LAST_ACTION, F_FILE_TYPE, F_CHECKSUM, F_PATH",
                file=fout)
            for f in updated:
                fout.write(','.join(f))
                fout.write('\n')
            del updated[:]

        if cache_cl:
            with gzip.open(cache_fname, 'rt', encoding='utf8') as fin:
                for line in fin:
                    if not line or line[0] == '\n' or line[0] == '#':
                        continue
                    cc = line.count(',')
                    if cc < 6:
                        continue
                    cl, tail = line.split(',', 1)
                    if filenames:
                        # Extract the last column, the filename (which might have a comma in it)
                        if cc == 6:
                            filename = tail.rsplit(',', 1)[1]
                        else:
                            filename = tail.split(',', 5)[5]
                        filename = filename[:-1]  # remove \n
                        if filename in filenames:
                            filenames.remove(filename)
                            continue
                    if not first:
                        first = cl
                    if from_changelist < int(cl) <= to_changelist:
                        yield line[:-1]
                    fout.write(line)  # Has \n
        fout.close()
        fout = None
        if first:
            os.chmod(temp_fname, 0o444)
            os.rename(temp_fname, f'{cache_dir}/{first}.fstat.gz')
    finally:
        if fout:
            fout.close()
        try:
            if temp_fname:
                os.unlink(temp_fname)
        except FileNotFoundError:
            pass


def fstat_verify(client_root, depot_path, fstats):
    from o4_pyforce import Pyforce

    base = os.path.abspath(
        os.path.expanduser(os.path.join(client_root,
                                        depot_path.replace('/...', '').strip('/'))))
    for f in fstats:
        fname = os.path.join(base, f[F_PATH])
        if os.path.exists(fname):
            if f[F_LAST_ACTION].endswith('delete'):
                yield f
            elif f[F_FILE_TYPE] != 'symlink' and Pyforce.checksum(
                    fname, f[F_FILE_TYPE], int(f[F_FILE_SIZE])) != f[F_CHECKSUM]:
                yield f
        else:
            if not f[F_LAST_ACTION].endswith('delete'):
                yield f


def retrieve_fstats(depot_path, upper, lower=None):
    """ Returns an iterator of Fstat objects where changelist is in (lower, upper].
        If lower is not given, it is assumed to be 0
    """

    from o4_pyforce import Pyforce

    def fstatify(r, head=len(depot_path.replace('...', ''))):
        try:
            return (r[b'headChange'].decode('utf8'), r[b'headRev'].decode('utf8'),
                    r.get(b'fileSize', b'0').decode('utf8'), r[b'headAction'].decode('utf8'),
                    r[b'headType'].decode('utf8'), r.get(b'digest', b'').decode('utf8'),
                    Pyforce.unescape(r[b'depotFile'].decode('utf8'))[head:])
        except Exception as e:
            print("*** ERROR: Got {!r} while fstatify({!r})".format(e, r))
            raise

    revs = '@{}'.format(upper)
    if lower is not None:
        assert lower < upper
        revs = '@{},@{}'.format(lower, upper)
    pyf = Pyforce('fstat', '-Rc', '-Ol', '-Os', '-T',
                  'headAction, headType, digest, fileSize, depotFile, headChange, headRev',
                  Pyforce.escape(depot_path) + revs)
    pyf.transform = fstatify
    return pyf


def o4_fstat_diag(fnames):
    from collections import defaultdict
    from itertools import chain

    def basics(fstats):
        fstats = list(fstats)
        n = len(fstats)
        n_deleted = sum(1 for f in fstats if 'delete' in f[F_LAST_ACTION])
        n_add = sum(1 for f in fstats if 'add' in f[F_LAST_ACTION])
        fsize = sum(int(f[F_FILE_SIZE]) for f in fstats)
        avg = 0
        if n > n_deleted:
            avg = fsize / (n - n_deleted)
        return (f"Basics: {n} files ({n_deleted} deleted, {n_add} added),"
                f"avg size {avg}, total: {fsize//1024} kB")

    max_cl = 0
    prev_cl = 0
    for fname in fnames:
        kb = defaultdict(list)
        changelist = defaultdict(list)
        ftype = defaultdict(list)
        #action = defaultdict(list)
        for f in reversed(list(fstat_from_csv(fname))):
            cl = int(f[F_CHANGELIST])
            if cl < prev_cl:
                continue
            max_cl = cl
            l = int(f[F_FILE_SIZE])
            kb[l // 1024].append(f)
            changelist[cl].append(f)
            ftype[f[F_FILE_TYPE]].append(f)
            ftype[f[F_LAST_ACTION]].append(f)
        print(f"From {prev_cl} to {max_cl}")
        print(basics(chain.from_iterable(changelist.values())))

        prev_cl = max_cl


if __name__ == '__main__':
    o4_fstat_diag(sys.argv[1:])

##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
