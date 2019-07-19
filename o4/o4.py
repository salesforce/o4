#!/usr/bin/env python3.6
"""
Usage:
  o4 sync <path> [-v] [-q] [-f] [+o] [-S <seed>] [-s <seed> [--move]] [-m <ignored>]
  o4 clean <path> [-v] [-q] [--resume] [--discard]
  o4 fstat <paths>... [-q] [-f] [--changed <previous>] [--drop <fname>] [--keep <fname>] [--report <report>]
  o4 seed-from <dir> [--fstat <fstat>] [--move]
  o4 (drop|keep|keep-any) [-v] [--case|--not-case] [--open|--not-open] [--existence|--not-existence] [--checksum|--not-checksum] [--deletes|--not-deletes]
  o4 drop --havelist
  o4 [-q] pyforce [--writable] [--debug] [--no-rev] [--] <p4args>...
  o4 head <paths>...
  o4 progress
  o4 fail

Option:
  sync          Sync/verify <path>.
  clean         Clean <path>.
  <path>        Specify perforce style path, optionally specify "@changelist", if not given, head
                will be determined. If path is a directory, "/..." is implied.
                This path must always be a directory, not a file.
  -s <seed>     Seed sync with files from a path.
  -S <seed>     Old o4 compatibility flag. Do not use, deprecated.
  --resume      Automatically resumes a clean if <path>.o4-bak exists.
  --discard     Delete the files that should not exist (i.e., don't save them in a separate
                location).
  fstat         Stream fstat lines for a [depot] path. Paths can contain changelist in
                the '<path>@<changelist>' notation.
  --changed <previous>  Only output fstat for changes in (<previous>,<changelist>]
  --drop <fname>  Remove fstat with path listed in <fname>.
  --keep <fname>  Only keep fstat with path listed in <fname>.
  --report <report>  Print the report string with interpolated values after the fstat operation.
  seed-from     Copy files from the seed directory if they match what we want from Perforce.
                If the named fstat file exists in the seed's .o4, it will be used, otherwise
                the file will be checksummed. Outputs on stdout files it did not copy.
  --fstat <fstat>  The path to the the fstat file, if any
  --move        Move the file from the seed directory rather than copy it
  drop          Forward fstat lines that don't satisfy any of the given filters
  keep          Forward fstat lines that satisfy every one of the given filters
  keep-any      Forward fstat lines that satisfy at least one of the given filters
  --case           Filter files whose filesystem path is identical, case and all, with the
                   entry in the fstat stream. (On the mac, filesystem can be formatted case
                   INsensitively). On Linux this is a no-op.
  --not-case       Opposite of --case
  --open           Filter files that are open for edit.
  --not-open       Opposite of --open
  --existence      Filter files that correctly exist (or are correctly absent) in the workspace.
  --not-existence  Opposite of --existence
  --checksum       Filter files that have the correct checksum.
  --not-checksum   Opposite of --checksum
  --deletes        Filter fstat lines that are deletes.
  --not-deletes    Opposite of --deletes
  --havelist       Filter files that are at the revision that the "have" data says they should be.
  -q            Skip second pass for sync, or for pyforce/fstat to be quiet.
  -f            Force all files to be verified and synced.
  +o            Do not sync open files.
  pyforce       Use pyforce to execute the p4 command (<p4args>...) on fstat on stdin.
  --writable    Make files writable after sync and switch to read only before sync. Implies that
                no .bak files are made of writable files.
  --no-rev      Send the depot path to p4 without the revision number.
  --debug       Display the pyforce response objects on stderr.
  <p4args>      List of arguments for the p4 CLI.
  head          Update .o4/head files in listed paths.
  <paths>       List of paths to visit.
  progress      Show progress based on .o4/.fstat.
  fail          Fails if there were fstat on stdin.
  -v            Be verbose.
  -m <ignored>  Compatibility with old o4, just added to not break, not actually implementing
                anything and will be removed as soon as old o4 is gone.

Note: Although all these commands are available to use, the common users is expected only
      to use "sync". For the use of the more internal commands, see help(o4.o4_sync).

      Use of drop/keep with more than one filter can be confusing.
"""

import os
import sys
import time
import functools

from subprocess import check_call, check_output, CalledProcessError, DEVNULL
from signal import SIGINT
from errno import EPERM
import shutil

err_print = functools.partial(print, file=sys.stderr)

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from o4_pyforce import Pyforce, P4Error, P4TimeoutError, info as pyforce_info, \
    client as pyforce_client, clear_cache
from o4_fstat import fstat_from_csv, fstat_iter, fstat_path, \
    fstat_split, fstat_join, get_fstat_cache, F_REVISION, F_FILE_SIZE, F_CHECKSUM, F_PATH
from o4_progress import progress_iter, progress_show, progress_enabled
from o4_utils import chdir, consume, o4_log, caseful_accurate
from o4_git import is_git_hybrid, git_master_prep, git_o4_import, git_master_restore

CLR = '%c[2K\r' % chr(27)

SYNCED_CL_FILE = '.o4/changelist'

# This file indicates that o4 deigned to exit successfully even though
# some files didn't get synced (due to particular Perforce situations).
INCOMPLETE_INDICATOR = '.o4/sync-incomplete'


def find_o4bin():
    # "Why not just use which?" "Sparse docker base images."
    import stat
    for d in os.environ['PATH'].split(':'):
        try:
            path = os.path.join(d, 'o4')
            mode = os.stat(path).st_mode
            if stat.S_ISREG(mode):
                return path
        except OSError:
            pass
    return __file__


def _depot_path():
    """
    Returns the depot path of CWD. Result is cached in env var
    $DEPOT_PATH.
    """

    if 'DEPOT_PATH' not in os.environ:
        os.environ['DEPOT_PATH'] = os.path.dirname(
            Pyforce.unescape(list(Pyforce('where', 'dummy'))[0]['depotFile']))
    return os.environ['DEPOT_PATH']


def o4_seed_from(seed_dir, seed_fstat, op):
    """
    For each target fstat on stdin, copy the matching file from the
    seed directory if 1) the seed fstat agrees, or 2) if no fstat, the
    checksum agrees. Output the fstat entries that were not copied.
    """

    def no_uchg(*fnames):
        check_call(['chflags', 'nouchg'] + [fname for fname in fnames if os.path.exists(fname)])

    def update_target(src, dest, fsop):
        try:
            try:
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                fsop(src, dest)
            except IOError as e:
                if e.errno == EPERM and sys.platform == 'darwin':
                    no_uchg(src, dest)
                    fsop(src, dest)
                else:
                    raise
        except IOError as e:
            print(f'# ERROR MOVING {src}: {e!r}')

    fsop = shutil.move if op == 'move' else shutil.copy2

    seed_checksum = None
    if seed_fstat:
        seed_checksum = {
            f[F_PATH]: f[F_CHECKSUM] for f in fstat_from_csv(seed_fstat, fstat_split) if f
        }
    target_dir = os.getcwd()
    with chdir(seed_dir):
        for line in sys.stdin:
            if line.startswith('#o4pass'):
                print(line, end='')
                continue
            f = fstat_split(line)
            if not f:
                continue
            if f[F_CHECKSUM]:
                dest = os.path.join(target_dir, f[F_PATH])
                if os.path.lexists(dest):
                    try:
                        os.unlink(dest)
                    except IOError as e:
                        if e.errno == EPERM and sys.platform == 'darwin':
                            no_uchg(dest)
                            os.unlink(dest)
                        else:
                            raise
                if seed_fstat:
                    checksum = seed_checksum.get(f[F_PATH])
                else:
                    checksum = Pyforce.checksum(f[F_PATH], f[F_FILE_SIZE])
                if f[F_FILE_SIZE].endswith('symlink') or checksum == f[F_CHECKSUM]:
                    update_target(f[F_PATH], dest, fsop)
            print(line, end='')  # line already ends with '\n'


def o4_fstat(changelist, previous_cl, drop=None, keep=None, quiet=False, force=False):
    """
    changelist: Target changelist
    previous_cl: Previous_cl if known (otherwise 0)
    drop: Input file name with a line-by-line file list of filenames
          to exclude from output
    keep: Input file name with a line-by-line file list of filenames
          to limit output to
    force: output all fstat lines even though previous_cl is set. This only affects
           fstat when previous_cl is more recent than changelist (reverse sync).

    Missing fstat files for changelist and previous_cl are generated automatically.

    Streams to stdout the fstat CSV from .o4/<changelist>.fstat.gz

    IF not previous_cl:

        Stream every entry from changelist (essentially gzcat), while
        applying drop_keep if given.

    IF previous_cl == changelist:

        Stream nothing.

    IF previous_cl < changelist:

        Only items in changelist that are newer than previous_cl are
        streamed. Apply drop_keep.

    IF previous_cl > changelist

        This reverse sync scenario is a little complicated:

        * Use the forward sync iterator to determine all files that
          should be synced.

        * Find branched or added files and generate false entries to
          have them deleted:
          '<changelist>,0,0,reverse_sync/delete,text,,<path>'

        * Add all other file names to KEEP and stream matches at
          <changelist>.

    DROP: Exclude fstat from the stream, if the fstat path is in the
          drop-file.

    KEEP: Limit fstat from the stream to paths listed in the
          keep-file.
    """

    if os.environ.get('DEBUG', ''):
        err_print(f"""# o4 fstat {os.getcwd()}
# changelist: {changelist}
# previous_cl: {previous_cl}
# drop: {drop}
# keep: {keep}
# quiet: {quiet}""")
    o4_log('fstat',
           _depot_path(),
           changelist=changelist,
           previous_cl=previous_cl,
           drop=drop,
           keep=keep,
           quiet=quiet,
           force=force)

    if previous_cl:
        previous_cl = int(previous_cl)
        if previous_cl == changelist:
            return changelist
    else:
        previous_cl = 0

    if quiet:
        if drop or keep:
            sys.exit("*** ERROR: Quiet fstat does not support drop or keep.")
        actual_cl = max(
            int(f.split(',', 1)[0]) for f in fstat_iter(_depot_path(), changelist, previous_cl))
        print(f'*** INFO: Created {os.getcwd()}/.o4/{actual_cl}.fstat.gz')
        return actual_cl

    if drop:
        with open(drop, 'rt', encoding='utf8') as fin:
            drop = set(f[:-1] for f in fin)
    if keep:
        with open(keep, 'rt', encoding='utf8') as fin:
            keep = set(f[:-1] for f in fin)

    if previous_cl and previous_cl > changelist:
        # Syncing backwards requires us to delete files that were added
        # between the lower and higher changelist. All other files must
        # be synced to their state at the lower changelist.
        past_filenames = set(p for p, _ in map(
            fstat_path,
            progress_iter(fstat_iter(_depot_path(), changelist),
                          os.getcwd() + '/.o4/.fstat', 'fstat-reverse')) if p)
        if not keep:
            keep = set()
        if not drop:
            drop = set()
        for f in map(
                fstat_split,
                progress_iter(fstat_iter(_depot_path(), previous_cl, changelist),
                              os.getcwd() + '/.o4/.fstat', 'fstat-reverse')):
            if not f:
                continue
            if f[F_PATH] not in past_filenames:
                print(f'{changelist},{f[F_PATH]},0,0,')
                if force:
                    drop.add(f[F_PATH])
            elif not force:
                keep.add(f[F_PATH])
        previous_cl = 0

    if drop and keep:
        # Prioritize dropping over keeping, if a file is in both.
        # Any file that is currently opened by Perforce must be dropped.
        # This function assumes that a supplied drop list is a list of
        # open files (which then gets augmented (above) with files that
        # did not exist at the lower changelist).
        keep = keep.difference(drop)
    drop_n = 0 if not drop else len(drop)
    keep_n = 0 if not keep else len(keep)
    if not drop:
        drop = None
    if not keep:
        keep = None

    fstats = progress_iter(fstat_iter(_depot_path(), changelist, previous_cl),
                           os.getcwd() + '/.o4/.fstat', 'fstat')
    # Can't break out of fstat_iter without risking that the local
    # cache is not created, causing fstat_from_perforce to be called
    # twice, so we use an iterator that we can drain.
    for line in fstats:
        if keep is not None or drop is not None:
            path, line = fstat_path(line)
            if not path:
                continue
            if drop is not None:
                drop.discard(path)
                if len(drop) != drop_n:
                    drop_n -= 1
                    if not drop_n:
                        drop = None
                    continue
            if keep is not None:
                keep.discard(path)
                if len(keep) == keep_n:
                    continue
                keep_n -= 1
                if not keep_n:
                    print(line)
                    # Make sure the iterator is consumed, so that
                    # local cache is created.
                    sum(0 for line in fstats)
                    break
        print(line)
    actual_cl, fname = get_fstat_cache(changelist)
    return actual_cl


def o4_drop_have(verbose=False):
    import time
    from bisect import bisect_left
    pre = len(_depot_path().replace('/...', '')) + 1
    have = None
    # We have to wait with pulling the server havelist until we have the input in its entirety
    lines = sys.stdin.read().splitlines()
    for line in lines:
        if line.startswith('#o4pass'):
            print(line)
            continue
        if not have:
            if have is not None:
                print(line)
                continue
            t0 = time.time()
            # We are getting have list in text mode because marshalled python objects are too slow
            have = check_output(['p4', 'have', '...'], encoding=sys.stdout.encoding, stderr=DEVNULL)
            if verbose:
                t0, t1 = time.time(), t0
                err_print("# HAVELIST", t0 - t1)
            have = [h[pre:] for h in have.splitlines()]
            if verbose:
                t0, t1 = time.time(), t0
                err_print("# SPLIT", t0 - t1)
            have.sort()
            if verbose:
                t0, t1 = time.time(), t0
                err_print("# SORT", t0 - t1)
        f = fstat_split(line)
        if not f:
            continue
        needle = f"{Pyforce.escape(f[F_PATH])}#{f[F_REVISION]} -"
        i = bisect_left(have, needle)
        miss = (i == len(have)) or not have[i].startswith(needle)
        if miss and f[F_CHECKSUM]:
            print(line)
    if verbose:
        t0, t1 = time.time(), t0
        err_print("# BISECT", t0 - t1)


def o4_filter(filtertype, filters, verbose):
    from functools import partial

    # Each function implements a filter. It is called with an Fstat tuple.
    # If it "likes" the row (e.g., "checksum" likes the row if the checksum
    # matches the local file's checksum), it returns the row; otherwise it
    # returns None. It also has the option of returning an altered copy.

    def f_deletes(row):
        return not row[F_CHECKSUM]

    def f_case(row):
        return caseful_accurate(row[F_PATH])

    def f_open(row, p4open={}):
        if not p4open:
            dep = _depot_path().replace('/...', '')
            p4open.update({
                Pyforce.unescape(p['depotFile'])[len(dep) + 1:]: p['action']
                for p in Pyforce('opened', dep + '/...')
            })
            p4open['populated'] = True
        return row[F_PATH] in p4open

    def f_existence(row):
        """
        Returns True if the file presence matches the fstat. That is True
        if fstat says 'delete' and file is missing or True if fstat is
        not 'delete' and file exists.
        """
        return (os.path.lexists(row[F_PATH]) and
                not os.path.isdir(row[F_PATH])) == bool(row[F_CHECKSUM])

    def f_checksum(row):
        if os.path.lexists(row[F_PATH]):
            if not row[F_CHECKSUM]:  # File is deleted
                if os.path.isdir(row[F_PATH]):
                    return True
            elif row[F_FILE_SIZE].endswith('symlink') or Pyforce.checksum(
                    row[F_PATH], row[F_FILE_SIZE]) == row[F_CHECKSUM]:
                return True
        else:
            if not row[F_CHECKSUM]:
                return True

    def inverter(fname, invert):
        if invert:
            return f"not({fname})"
        return fname

    funcs = [inverter(f'f_{fname}(x)', invert) for fname, doit, invert in filters if doit]
    if not funcs:
        sys.exit(f'*** ERROR: No arguments supplied to filter')
    elif len(funcs) == 1 and filtertype != 'drop' and not funcs[0].startswith('not('):
        if verbose:
            err_print(f"# Filter {filtertype}:", funcs)
        combo_func = locals()[funcs[0].split('(')[0]]
    elif filtertype == 'drop':
        combo_func = 'lambda x: not(' + ' or '.join(f for f in funcs) + ')'
        if verbose:
            err_print(f"# Filter {filtertype}:", combo_func)
        combo_func = eval(combo_func, locals())
    elif filtertype == 'keep-any':
        combo_func = 'lambda x: ' + ' or '.join(f for f in funcs)
        if verbose:
            err_print(f"# Filter {filtertype}:", combo_func)
        combo_func = eval(combo_func, locals())
    elif filtertype == 'keep':
        combo_func = 'lambda x: ' + ' and '.join(f for f in funcs)
        if verbose:
            err_print(f"# Filter {filtertype}:", combo_func)
        combo_func = eval(combo_func, locals())
    else:
        sys.exit(f"*** ERROR: Invalid filtertype: {filtertype}")

    try:
        for line in sys.stdin:
            if line.startswith('#o4pass'):
                print(line, end='')
                continue
            row = fstat_split(line)
            if row:
                if combo_func(row):
                    print(fstat_join(row))
                elif verbose:
                    print('#', row)
    except KeyboardInterrupt:
        raise


def o4_pyforce(debug, no_revision, writable, args: list, quiet=False):
    """
    Encapsulates Pyforce, does book keeping to ensure that all files
    that should be operated on are in fact dealt with by p4. Handles
    retry and strips out asks for files that are caseful mismatches on
    the current file system (macOS).
    """

    from tempfile import NamedTemporaryFile
    from collections import defaultdict
    from stat import S_IWUSR, S_IFREG

    class LogAndAbort(Exception):
        'Dumps debug information on errors.'

    a_plus_w = 0o222
    a_minus_w = (0o777 ^ a_plus_w) | S_IFREG

    o4_log('pyforce', no_revision=no_revision, writable=writable, quiet=quiet, *args)

    tmpf = NamedTemporaryFile(dir='.o4')
    fstats = []
    orig_mode = {}
    for line in sys.stdin.read().splitlines():
        if line.startswith('#o4pass'):
            print(line)
            continue
        f = fstat_split(line)
        if f and caseful_accurate(f[F_PATH]):
            fstats.append(f)
            s = os.stat(f[F_PATH])
            if s.st_mode & S_IWUSR:
                orig_mode[f[F_PATH]] = s.st_mode
                os.chmod(s.st_mode & a_minus_w)
        elif f:
            err_print(f"*** WARNING: Pyforce is skipping {f[F_PATH]} because it is casefully"
                      " mismatching a local file.")
    retries = 3
    head = _depot_path().replace('/...', '')
    while fstats:
        if no_revision:
            p4paths = [Pyforce.escape(f[F_PATH]) for f in fstats]
        else:
            p4paths = [f"{Pyforce.escape(f[F_PATH])}#{f[F_REVISION]}" for f in fstats]
        tmpf.seek(0)
        tmpf.truncate()
        not_yet = []
        pargs = []
        xargs = []
        # This is a really bad idea, files are output to stdout before the actual
        # sync happens, causing checksum tests to start too early:
        #        if len(p4paths) > 30 and 'sync' in args:
        #            xargs.append('--parallel=threads=5')
        if sum(len(s) for s in p4paths) > 30000:
            pargs.append('-x')
            pargs.append(tmpf.name)
            for f in p4paths:
                tmpf.write(f.encode('utf8'))
                tmpf.write(b'\n')
            tmpf.flush()
        else:
            xargs.extend(p4paths)
        try:
            # TODO: Verbose
            #print('# PYFORCE({}, {}{})'.format(','.join(repr(a) for a in args), ','.join(
            #    repr(a) for a in paths[:3]), ', ...' if len(paths) > 3 else ''))
            errs = []
            repeats = defaultdict(list)
            infos = []
            for res in Pyforce(*pargs, *args, *xargs):
                if debug:
                    err_print("*** DEBUG: Received", repr(res))
                # FIXME: Delete this if-statement:
                if res.get('code', '') == 'info':
                    infos.append(res)
                    if res.get('data', '').startswith('Diff chunks: '):
                        continue
                if res.get('code', '') == 'error':
                    errs.append(res)
                    continue
                if 'resolveFlag' in res:
                    # TODO: resolveFlag can be ...?
                    #         m: merge
                    #         c: copy from  (not conflict!)
                    # We skip this entry as it is the second returned from p4
                    # for one input file
                    continue
                res_str = res.get('depotFile') or res.get('fromFile')
                if not res_str and res.get('data'):
                    res_str = head + '/' + res['data']
                if not res_str:
                    errs.append(res)
                    continue
                res_str = Pyforce.unescape(res_str)
                try:
                    os.chmod(res_str, orig_mode.pop(res_str))
                except KeyError:
                    pass

                for i, f in enumerate(fstats):
                    if f"{head}/{f[F_PATH]}" in res_str:
                        repeats[f"{head}/{f[F_PATH]}"].append(res)
                        not_yet.append(fstats.pop(i))
                        break
                else:
                    for f in repeats.keys():
                        if f in res_str:
                            if debug:
                                err_print(f"*** DEBUG: REPEAT: {res_str}\n {res}\n {repeats[f]}")
                            break
                    else:
                        if debug:
                            err_print("*** DEBUG: ERRS APPEND", res)
                        errs.append(res)
            if errs:
                raise LogAndAbort('Unexpected reply from p4')

            if len(p4paths) == len(fstats):
                raise LogAndAbort('Nothing recognized from p4')
        except P4Error as e:
            non_recoverable = False
            for a in e.args:
                if 'clobber writable file' in a['data']:
                    fname = a['data'].split('clobber writable file')[1].strip()
                    err_print("*** WARNING: Saving writable file as .bak:", fname)
                    if os.path.exists(fname + '.bak'):
                        now = time.time()
                        err_print(f"*** WARNING: Moved previous .bak to {fname}.{now}")
                        os.rename(fname + '.bak', f'{fname}.bak.{now}')
                    shutil.copy(fname, fname + '.bak')
                    os.chmod(fname, 0o400)
                else:
                    non_recoverable = True
            if non_recoverable:
                raise
        except P4TimeoutError as e:
            e = str(e).replace('\n', ' ')
            err_print(f"# P4 TIMEOUT, RETRIES {retries}: {e}")
            retries -= 1
            if not retries:
                sys.exit(f"{CLR}*** ERROR: Perforce timed out too many times:\n{e}")
        except LogAndAbort as e:
            import json
            fname = f'debug-pyforce.{os.getpid()}.{int(time.time())}'
            d = {
                'args': args,
                'fstats': fstats,
                'errs': errs,
                'repeats': repeats,
                'infos': infos,
            }
            json.dump(d, open(f'.o4/{fname}', 'wt'))
            sys.exit(f'{CLR}*** ERROR: {e}; detail in {fname}')
        finally:
            if not quiet:
                for fstat in not_yet:
                    # Printing the fstats after the p4 process has ended, because p4 marshals
                    # its objects before operation, as in "And for my next act... !"
                    # This premature printing leads to false checksum errors during sync.
                    print(fstat_join(fstat))
    # TODO: This should probably be in a finally clause
    for fname, omode in orig_mode.items():
        os.chmod(fname, omode)


def o4_sync(changelist,
            seed=None,
            seed_move=False,
            quick=False,
            force=False,
            skip_opened=False,
            verbose=True,
            gatling=True,
            manifold=True):
    """ Syncs CWD to changelist, as efficiently as possible.

        seed: Input dir for seeding.
        seed_move: Move seed files instead of copy.
        force: Go through every single file not just what's new.
        quick: Skip post p4 sync verification.

        gatling: Set to false to disable the use of gatling
        manifold: Set to false to disable the use of manifold

        Pseudo code to use also as inspiration for fault finding:

        CL: target changelist
        CUR_CL: Currently synced changelist
        RETRIES: How many times to attempt force sync for files that fail verification

        Sync the files open for edit (if the file is missing, it must be reverted):
          o4 fstat .<CL> [--changed <cur_CL>] | o4 keep -—open | gatling o4 pyforce sync |
               gatling o4 pyforce resolve -am | o4 drop --existence | o4 pyforce revert |
               o4 drop --existence | o4 fail

        Sync the files not open for edit, supply --case on macOS:
          o4 fstat .<CL> [--changed <cur_CL>] | o4 drop -—open [--case] |
               [| gatling -n 4 o4 seed-from --copy <seed>] | gatling o4 pyforce sync |
               | tee tmp_sync
               [| gatling -n 4 o4 drop --checksum | gatling o4 pyforce sync -f] * <RETRIES>

          o4 diff .<CL> [<cur_CL>] | o4 filter —unopen [--case] |
               [| gatling -n 4 o4 seed-from --copy <seed>] | gatling o4 pyforce sync |
               | tee tmp_sync
               [| gatling -n 4 o4 verify | gatling o4 pyforce sync -f] * <RETRIES>

        Ensure the have-list is in sync with the files:
          if seed or force:
              o4 diff .<CL> | o4 drop --havelist | gatling o4 pyforce sync -k
          else:
              cat tmp_sync | o4 drop --havelist | gatling o4 pyforce sync -k


        Note: manifold starts processes up front, so it's better suited for work
              that do not tax other equipment, such as locally calculating checksums.
              gatling starts and fills one process at a time and is best used
              with p4-related programs, to avoid lots of connections to the server.

    """
    from tempfile import NamedTemporaryFile

    def clientspec_is_vanilla():
        'Return True if every View line is the same on the left and the right.'
        # We also want to accept a clientspec which has the same prefix
        # on every line on the right. E.g.
        #    //depot/dir1  /client/pre/fix/dir1
        # is acceptable if every mapping has /client/pre/fix

        # (This is to accomodate Autobuild clientspecs, which locate the workspace at autobuild/client)
        # (Turns out ABR has non-vanilla clientspecs even aside from the
        # prefix. Just give it an escape.)
        import o4_config
        if o4_config.allow_nonflat_clientspec():
            return True

        client = pyforce_client()
        cname = client['Client']
        view = [
            v[1:].split(' //' + cname)
            for k, v in client.items()
            if k.startswith('View') and not v.startswith('-//')
        ]

        # Get the prefix (possibly zero-length) from the first entry.
        # If the first doesn't even match, it'll be empty, but then will
        # fail the match anyway.
        left, right = view[0]
        prefix = right[:-len(left)]

        for left, right in view:
            if prefix + left != right:
                return False
        return True

    def run_cmd(cmd, inputstream=None):
        timecmd = 'time ' if verbose else ''
        cmd = [c.strip() for c in cmd.split('|')]
        print("*** INFO: [{}]".format(os.getcwd()), ' |\n         '.join(cmd).replace(o4bin, 'o4'))
        cmd = '|'.join(cmd)
        try:
            check_call([
                '/bin/bash', '-c', f'set -o pipefail;{timecmd}{cmd}' +
                '|| (echo PIPESTATUS ${PIPESTATUS[@]} >.o4-pipefails; false)'
            ])
            print()
        except CalledProcessError:
            cwd = os.getcwd()
            with open('.o4-pipefails') as f:
                fails = f.readline().rstrip().split()[1:]
                os.remove('.o4-pipefails')
            cmd = cmd.split('|')
            msg = [f"{CLR}*** ERROR: Pipeline failed in {cwd}:"]
            failures = []
            for status, cmd in zip(fails, cmd):
                cmd = cmd.replace(o4bin, 'o4')
                if status == '1':
                    status = ' FAILED '
                    failures.append(cmd)
                else:
                    status = ' OK     '
                msg.append(f'{status} {cmd}')
            # Print the process list only if something besides "fail" failed.
            if len(failures) > 1 or not failures[0].endswith('o4 fail'):
                err_print('\n'.join(msg))
            sys.exit(1)

    def gat(cmd):
        if not gatling:
            return ''
        return cmd

    def man(cmd):
        if not manifold:
            return ''
        return cmd

    if not clientspec_is_vanilla():
        # If there was no cached client, or if we refresh it and
        # it's still bad, then abort.
        if not clear_cache('client') or not clientspec_is_vanilla():
            clear_cache('client')
            sys.exit('*** ERROR: o4 does not support a clientspec that maps a depot '
                     'path to a non-matching local path. '
                     'Are you aware that you have such a mapping? Do you need it? '
                     'If not, please remove it and sync again. If so, '
                     'please post to the BLT chatter group that you have such a '
                     'clientspec; meanwhile you must use p4/p4v to sync.')

    o4bin = find_o4bin()

    previous_cl = 0
    if os.path.exists(SYNCED_CL_FILE):
        with open(SYNCED_CL_FILE) as fin:
            try:
                previous_cl = int(fin.read().strip())
            except ValueError:
                err_print(f"{CLR}*** WARNING: {os.getcwd()}/{SYNCED_CL_FILE} could not be read")

    o4_log('sync',
           changelist=changelist,
           previous_cl=previous_cl,
           seed=seed,
           seed_move=seed_move,
           quick=quick,
           force=force,
           skip_opened=skip_opened,
           verbose=verbose,
           gatling=gatling,
           manifold=manifold)

    verbose = ' -v' if verbose else ''
    force = ' -f' if force else ''
    fstat = f"{o4bin} fstat{force} ...@{changelist}"
    gatling_low = gat(f"gatling{verbose} -n 4")
    if previous_cl and not force:
        fstat += f" --changed {previous_cl}"
        gatling_low = ''
    manifold_big = man(f"manifold{verbose} -m {10*1024*1024}")
    gatling_verbose = gat(f"gatling{verbose}")
    manifold_verbose = man(f"manifold{verbose}")
    progress = f"| {o4bin} progress" if sys.stdin.isatty() and progress_enabled() else ''
    pyforce = 'pyforce'  #pyforce = 'pyforce' + (' --debug --' if os.environ.get('DEBUG', '') else '')
    casefilter = ' --case' if sys.platform == 'darwin' else ''
    keep_case = f'| {o4bin} keep --case' if casefilter else ''
    if previous_cl == changelist and not force:
        print(f'*** INFO: {os.getcwd()} is already synced to {changelist}, use -f to force a'
              f' full verification.')
        return

    if os.path.exists(INCOMPLETE_INDICATOR):
        # Remove the indicator. If it is recreated, we will not create the
        # changelist file because the system is not exactly at that changelist.
        os.remove(INCOMPLETE_INDICATOR)
    if os.path.exists(SYNCED_CL_FILE):
        os.remove(SYNCED_CL_FILE)

    has_open = list(Pyforce('opened', '...'))
    openf = NamedTemporaryFile(dir='.o4', mode='w+t')
    if has_open:
        dep = _depot_path().replace('/...', '')
        print(f'*** INFO: Opened for edit in {dep}:')
        for i, p in enumerate(has_open):
            open_file_name = Pyforce.unescape(p['depotFile'])[len(dep) + 1:]
            print(open_file_name, file=openf)
            if i < 10:
                print(f'*** INFO: --keeping {open_file_name}')
        if len(has_open) > 10:
            print(f'          (and {len(has_open) - 10} more)')
        openf.flush()

        # Resolve before syncing in case there are unresolved files for other reasons
        cmd = (f"{fstat} --keep {openf.name}"
               f"| {gatling_verbose} -- {o4bin} {pyforce} --no-rev -- resolve -am"
               f"| {o4bin} {pyforce} sync"
               f"| {gatling_verbose} -- {o4bin} {pyforce} --no-rev -- resolve -am"
               f"{progress}"
               f"| {o4bin} drop --existence"
               f"| {gatling_verbose} -- {o4bin} {pyforce} --no-rev -- revert"
               f"| {o4bin} drop --existence"
               f"| {o4bin} fail")
        # Unopened only from here on
        fstat += f' --drop {openf.name}'
        if not skip_opened:
            run_cmd(cmd)
        else:
            print(f"*** INFO: Not syncing {len(has_open)} files opened for edit.")
    else:
        print(f"{CLR}*** INFO: There are no opened files.")

    prep = None
    if is_git_hybrid():
        # TODO: Is this disabled by --move or -s or -f?
        prep = git_master_prep(_depot_path(), changelist)
        pyforce = f"{pyforce} --writable"  # Make sure this is not affixed for opened files

    quiet = '-q' if seed else ''
    retry = (f"| {manifold_big} {o4bin} drop --checksum"
             f"| {gatling_verbose} {o4bin} {quiet} {pyforce} sync -f"
             f"| {manifold_big} {o4bin} drop --checksum"
             f"| {gatling_verbose} {o4bin} {quiet} {pyforce} sync -f"
             f"| {manifold_big} {o4bin} drop --checksum"
             f"| {o4bin} fail")

    syncit = f"| {gatling_verbose} {o4bin} {quiet} {pyforce} sync{force}"
    if seed:
        syncit = f"| {manifold_verbose} {o4bin} seed-from {seed}"
        _, seed_fstat = get_fstat_cache(10_000_000_000, seed + '/.o4')
        if seed_fstat:
            syncit += f" --fstat {os.path.abspath(seed_fstat)}"
        if seed_move:
            syncit += " --move"

    cmd = (f"{fstat} | {o4bin} drop --not-deletes --existence"
           f"{keep_case}"
           f"{progress}"
           f"{syncit}"
           f"{retry}")
    run_cmd(cmd)

    if seed:
        if not previous_cl:
            print(f"*** INFO: Flushing to changelist {changelist}, please do not interrupt")
            t0 = time.time()
            consume(Pyforce('-q', 'sync', '-k', f'...@{changelist}'))
            print("*** INFO: Flushing took {:.2f} minutes".format((time.time() - t0) / 60))

    cmd = (f"{fstat} "
           f"| {o4bin} drop --deletes --checksum"
           f"{keep_case}"
           f"{progress}"
           f"{retry}")
    run_cmd(cmd)

    actual_cl, _ = get_fstat_cache(changelist)
    if os.path.exists(INCOMPLETE_INDICATOR):
        os.remove(INCOMPLETE_INDICATOR)
    else:
        with open(SYNCED_CL_FILE, 'wt') as fout:
            print(actual_cl, file=fout)

    if seed or not quick:
        print("*** INFO: Sync is now locally complete, verifying server havelist.")
        cmd = (f"{fstat}"
               f"| {o4bin} drop --havelist"
               f"{keep_case}"
               f"{progress}"
               f"| {gatling_low} {o4bin} {pyforce} sync -k"
               f"| {o4bin} drop --havelist"
               f"| {o4bin} fail")
        run_cmd(cmd)
    if actual_cl != changelist:
        print(f'*** INFO: Changelist {changelist} does not affect this directory.')
        print(f'          Synced to {actual_cl} (the closest previous change that does).')
    if previous_cl == actual_cl and not force:
        print(f'*** INFO: {os.getcwd()} is already synced to {actual_cl}, use -f to force a'
              f' full verification.')

    if prep:
        err_print("*** INFO: Sync from p4 is complete. Hybrid import upstream changes to git.")
        git_o4_import(prep)
        git_master_restore(prep)


def get_clean_cl(opts):
    target = os.getcwd()
    source = target + '/.o4/cleaning'
    if os.path.exists(source) and not opts['--resume']:
        sys.exit('*** ERROR: Previous clean was interrupted; use --resume')

    if '@' in opts:
        cl = opts['@']
    elif os.path.exists('.o4/changelist'):
        with open('.o4/changelist') as f:
            cl = f.readline().strip()
    elif opts['--resume'] and os.path.exists(f'{source}/.o4/changelist'):
        with open(f'{source}/.o4/changelist') as f:
            cl = f.readline().strip()
    else:
        cl = o4_head([opts['<path>']])[0]
    if opts['-v']:
        err_print(f'*** INFO: Cleaning to changelist {cl}')
    return int(cl)


def o4_clean(changelist, quick=False, resume=False, discard=False):

    def move_except(from_dir, to_dir, but_not):
        with chdir(from_dir):
            for f in os.listdir('.'):
                if f != but_not:
                    shutil.move(f, f'{to_dir}/{f}')

    target = os.getcwd()
    source = f'{target}/.o4/cleaning'
    if resume:
        if not os.path.exists(source):
            sys.exit(f'*** ERROR: Cannot resume cleaning; {source} does not exist.')
    else:
        os.makedirs(f'{source}/.o4', exist_ok=True)
        move_except(f'{target}/.o4', f'{source}/.o4', but_not='cleaning')
        move_except(target, source, but_not='.o4')

        dep = _depot_path().replace('/...', '')
        p4open = [
            Pyforce.unescape(p['depotFile'])[len(dep) + 1:]
            for p in Pyforce('opened', dep + '/...')
            if 'delete' not in p['action']
        ]
        print(f"*** INFO: Not cleaning {len(p4open)} files opened for edit.")
        for of in p4open:
            if os.path.dirname(of):
                os.makedirs(os.path.dirname(of), exist_ok=True)
            shutil.move(os.path.join(source, of), of)

    os.chdir(target)
    o4bin = find_o4bin()
    cmd = [o4bin, 'sync', f'.@{changelist}', '-f', '+o', '-s', source, '--move']
    if quick:
        cmd.append('-q')
    check_call(cmd)
    if not discard:
        savedir = source.replace('cleaning', time.strftime('cleaned'))  # @%Y-%m-%d,%H:%M'))
        shutil.move(source, savedir)
        err_print(f'*** INFO: Directory is clean @{changelist}; detritus is in {savedir}')
    else:
        assert source.endswith('cleaning')
        shutil.rmtree(source)


def o4_fail():
    files = []
    passthroughs = []
    n = 0
    for line in sys.stdin:
        if line.startswith('#o4pass-'):
            msgtype, _, msg = line.replace('#o4pass-', '').partition('#')
            if msgtype == 'warn':
                passthroughs.append(msg)
            continue
        f = fstat_split(line)
        if not f:
            continue
        n += 1
        if n < 100:
            files.append(f"  {f[F_PATH]}#{f[F_REVISION]}")

    if not files and not passthroughs:
        sys.exit(0)

    err_print()
    hdr = ' o4 ERROR ' if files else ' o4 WARNING '
    l = (78 - len(hdr)) // 2
    hdr = '*' * l + hdr + '*' * l
    ftr = '*' * len(hdr)
    err_print(f'{CLR}{hdr}')

    if files:
        err_print('These files did not sync and are ERRORs')
        err_print('\n'.join(sorted(files)))
        if len(files) != n:
            err_print(f'  ...and {n-len(files)} others!')
        err_print(f'\n{ftr}')

    if passthroughs:
        err_print('These files did not sync and are WARNINGs')
        err_print('\n'.join(sorted(passthroughs)))
        err_print(ftr)
        if not files:
            with open(INCOMPLETE_INDICATOR, 'w') as f:
                pass
            err_print(
                f'{CLR} o4 IS EXITING WITH SUCCESS EVEN THOUGH THOSE FILES ARE NOT UP-TO-DATE')
            err_print(ftr)
            sys.exit(0)

    err_print('BECAUSE o4 DID NOT COMPLETE, THERE MAY BE OTHER FILES')
    err_print('BESIDES THOSE LISTED THAT ARE NOT CORRECTLY SYNCED.')
    s = '' if n == 1 else 's'
    sys.exit(f'{CLR}*** ERROR: Pipeline ended with {n} file{s} rejected.')


def o4_head(paths):

    def o4_head_update(args):
        res = list(args)
        for s in Pyforce('changes', '-s', 'submitted', '-m1', *res):
            for i, arg in enumerate(res):
                if type(arg) is int:
                    continue
                # p4 rewrites path if there are no files until further down
                if s['path'].startswith(arg[:-3]) or arg.startswith(s['path'][:-3]):
                    res[i] = int(s['change'])
                    o4dir = os.environ['CLIENT_ROOT'] + arg[1:].replace('/...', '/.o4')
                    os.makedirs(o4dir, exist_ok=True)
                    with open(o4dir + '/head', 'wt') as fout:
                        print(f"{s['change']}", file=fout)
                    break
            else:
                err_print("*** WARNING: Could not map result", s)
        for r in res:
            if type(r) is not int:
                try:
                    o4dir = os.environ['CLIENT_ROOT'] + r[1:].replace('/...', '/.o4')
                    os.unlink(o4dir + '/head')
                except FileNotFoundError:
                    pass
                sys.exit(f"*** ERROR: Could not get HEAD for {r}")
        return res

    args = []
    for depot_path in paths:
        if not depot_path.endswith('/...'):
            depot_path += '/...'
        args.append(Pyforce.escape(depot_path))
    for retry in range(3):
        try:
            end = '' if len(args) > 1 else args[0]
            err_print(f"# {CLR}*** INFO: ({retry+1}/3) Retrieving HEAD changelist for", end)
            if not end:
                for path in args:
                    print(f"      {path}")
            return o4_head_update(args)
        except (P4TimeoutError, IndexError):
            continue
    sys.exit(f"{CLR}*** ERROR: There was an error retrieving head change for {args}")


def depot_abs_path(path):
    if 'CLIENT_ROOT' not in os.environ:
        if 'BLT_HOME' in os.environ:
            os.environ['CLIENT_ROOT'] = os.environ['BLT_HOME']
        else:
            os.environ['CLIENT_ROOT'] = pyforce_info()['clientRoot']
    path = os.path.abspath(os.path.expanduser(path.replace('...', '').rstrip('/')))
    if not path.startswith('//'):
        path = path.replace(os.environ['CLIENT_ROOT'][1:], '')
    return path


def parallel_fstat(opts):
    from tempfile import NamedTemporaryFile

    print("*** INFO: Parallel fstat retrieve:", *opts['<paths>'])
    with NamedTemporaryFile(mode='w+t') as sin:
        for p in opts['<paths>']:
            print(p, file=sin)
        sin.seek(0, 0)
        # Makes the assumption that no path is less than 4 bytes:
        return check_call(['manifold', '-c', '4', '--', 'xargs', '-n1', 'o4', 'fstat', '-q'],
                          stdin=sin)


def add_implicit_args(args):
    import o4_config

    for i, word in enumerate(args):
        if not word.startswith('-'):
            break
    if args:
        config_args = o4_config.cmdline_args(word)
        if config_args:
            args[i + 1:i + 1] = config_args


def main():
    from docopt import docopt

    os.environ['PYTHONUNBUFFERED'] = 'true'
    args = sys.argv[1:]
    add_implicit_args(args)
    if 'pyforce' in args and '--' not in args:
        # YUCK! Since p4 args look like undocumented options to docopt we have to put the '--'
        # marker so they are not parsed
        args.insert(args.index('pyforce') + 1, '--')
    opts = docopt(__doc__, args)

    # Commands that don't parse a changelist
    ec = 0
    try:
        ran = False
        if opts['seed-from']:
            ran = True
            o4_seed_from(opts['<dir>'], opts['--fstat'], 'move' if opts['--move'] else 'copy')
        filtertype = [f for f in ('drop', 'keep', 'keep-any') if opts[f]]
        if filtertype:
            ran = True
            if opts['--havelist']:
                o4_drop_have()
            else:
                # This order is the best for performance, think before rearranging.
                o4_filter(filtertype[0], (
                    ('deletes', opts['--deletes'], False),
                    ('deletes', opts['--not-deletes'], True),
                    ('existence', opts['--existence'], False),
                    ('existence', opts['--not-existence'], True),
                    ('checksum', opts['--checksum'], False),
                    ('checksum', opts['--not-checksum'], True),
                    ('case', opts['--case'], False),
                    ('case', opts['--not-case'], True),
                    ('open', opts['--open'], False),
                    ('open', opts['--not-open'], True),
                ), opts['-v'])
        if opts['pyforce']:
            ran = True
            o4_pyforce(opts['--debug'], opts['--no-rev'], opts['--writable'], opts['<p4args>'],
                       opts['-q'])
        if opts['progress']:
            ran = True
            progress_show(os.path.join(os.getcwd(), '.o4/.fstat'))
        if opts['fail']:
            ran = True
            o4_fail()
    except KeyboardInterrupt:
        err_print('*** WARNING: aborted by user')
        ec = 0 - SIGINT
    except Exception as e:
        from traceback import print_exc
        print_exc(file=sys.stderr)
        err_print(f'*** ERROR: {e}')
        ec = 1

    if ran or ec:
        sys.exit(ec)

    if opts['head']:
        o4_head(map(depot_abs_path, opts['<paths>']))
        sys.exit(0)

    if opts['fstat'] and opts['<paths>']:
        if len(opts['<paths>']) == 1:
            opts['<path>'] = opts['<paths>'].pop()
        else:
            sys.exit(parallel_fstat(opts))

    if '@' in opts['<path>']:
        if opts['<path>'].startswith('@'):
            opts['<path>'] = '...' + opts['<path>']
        try:
            opts['@'] = int(opts['<path>'].rsplit('@', 1)[-1])
            opts['<path>'] = opts['<path>'].rsplit('@', 1)[0]
        except ValueError:
            print('*** WARNING: Could not parse @-revision, ignored.')
    opts['<path>'] = depot_abs_path(opts['<path>'])
    target = os.path.join(os.environ['CLIENT_ROOT'], opts['<path>'][2:])
    o4dir = os.path.join(target, '.o4')
    opts['<path>'] = opts['<path>'] + '/...'
    if opts['-S'] and not opts['-s']:
        # TODO: Delete when old o4 is gone
        opts['-s'] = opts['-S']
    if opts['-s']:
        opts['-s'] = os.path.abspath(opts['-s'])
    os.makedirs(target, exist_ok=True)
    os.makedirs(o4dir, exist_ok=True)
    os.chdir(target)
    os.environ['PWD'] = os.getcwd()  # p4 ignores the actual directory and relies on $PWD
    os.environ['DEPOT_PATH'] = opts['<path>']

    if opts['clean']:
        o4_clean(get_clean_cl(opts), opts['-q'], opts['--resume'], opts['--discard'])
        sys.exit(0)

    head_change = int(os.environ.get('O4HEAD', '0'))
    if not head_change:
        head_change = o4_head([opts['<path>']])[0]
        os.environ['O4HEAD'] = str(head_change)
    if '@' in opts:
        if head_change < opts['@']:
            err_print(f"*** INFO: Latest change on {os.getcwd()} is less than {opts['@']}.",
                      f"Syncing to {head_change} instead.")
            opts['@'] = head_change
    else:
        opts['@'] = head_change
    os.environ['CHANGELIST'] = str(opts['@'])
    try:
        if opts['fstat']:
            actual_cl = o4_fstat(opts['@'], opts['--changed'], opts['--drop'], opts['--keep'],
                                 opts['-q'], opts['-f'])
            if opts['--report']:
                print(opts['--report'].format(**locals()))
        if opts['sync']:
            if opts['-m']:
                print("*** WARNING: sync -m is deprecated.")
            if opts['-S']:
                print("*** WARNING: sync -S is deprecated.")
            o4_sync(opts['@'], opts['-s'], opts['--move'], opts['-q'], opts['-f'], opts['+o'],
                    opts['-v'])
    except KeyboardInterrupt:
        prog = 'sync' if opts['sync'] else 'fstat'
        err_print(f'*** WARNING: {prog} aborted by user')
        ec = 0 - SIGINT
    except BrokenPipeError:
        print('*** ERROR: broken pipe :(', file=sys.stderr)
    sys.exit(ec)


if __name__ == '__main__':
    main()

##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
