"""
Hybrid o4/git commands. Provides a bridge for users that want to migrate or coexist with git.

TODO: What to do about read-only perforce and not git?
"""
from o4_fstat import fstat_from_csv, fstat_split, F_CHANGELIST, F_PATH, F_REVISION, F_FILE_SIZE, F_CHECKSUM

import os
import sys
import functools

from subprocess import check_call, check_output, CalledProcessError, run, PIPE, STDOUT

err_print = functools.partial(print, file=sys.stderr)
err_check_call = functools.partial(check_call, stdout=sys.stderr)
txt_check_output = functools.partial(check_output, universal_newlines=True)


def git_hybrid_init():
    """Call to get started with a blank repo."""
    from fnmatch import fnmatch

    if os.path.exists('.o4/hybrid_off'):
        err_print("*** WARNING: Removing .o4/hybrid_off.")
        os.unlink('.o4/hybrid_off')
    if is_git_hybrid():
        return
    cl = int(open('.o4/changelist').read().strip())
    if not os.path.isdir('.git'):
        err_print(f"*** INFO: [git] Initializing for hybrid sync at CL {cl}.")
        err_check_call(['git', 'init', '.'])
        err_print("*** INFO: Enabling rerere and LFS.")
        err_check_call(['git', 'lfs', 'install'])
        err_check_call(['git', 'config', 'rerere.enable', 'true'])
        bin_exts = [
            '*.bin',
            '*.exe',
            '*.vdi',
            '*.jar',
            '*.war',
            '*.ttf',
            '*.mp3',
            '*.mp4',
            '*.avi',
            '*.ogv',
            '*.7z',
            '*.rar',
            '*.zip',
            '*.tgz',
            '*.gz',
            '*.rpm',
            '*.mmdb',
            '*.xlt',
            '*.xls',
            '*.svg',
            '*.jpg',
            '*.png',
            '*.wsdl',
            '*.so',
            '*.so.*',
        ]
        for ext in bin_exts:
            err_print("*** INFO: [git] LFS track", ext)
            err_check_call(['git', 'lfs', 'track', ext, '--lockable'])
        with open('.gitignore', 'wt') as fout:
            print('/.o4', file=fout)
            print('/.o4-pipefails', file=fout)
        err_print("*** INFO: Checking fstat for large files (1MB)... ")
        for i, fs in enumerate(fstat_from_csv(f".o4/{cl}.fstat.gz", fstat_split)):
            if not fs:
                continue
            if i % 10000 == 1:
                err_print(i, end=' ')
                sys.stderr.flush()
            try:
                if int(fs[F_FILE_SIZE]) >= 1e6:
                    for ext in bin_exts:
                        if fnmatch(fs[F_PATH], ext):
                            break
                    else:
                        err_print("*** INFO: [git] LFS track", fs[F_PATH],
                                  f"{int(fs[F_FILE_SIZE])/1e6:.2f}MB")
                        err_check_call(['git', 'lfs', 'track', fs[F_PATH], 'lockable'])
            except ValueError:
                # symlink
                pass
        err_check_call(['git', 'add', '.gitattributes'])
        err_check_call(['git', 'add', '.gitignore'])
        err_check_call(['git', 'commit', '-m', 'Initialized config, lfs, ignore.'])
    with open('.hybrid_changelist', 'wt') as fout:
        print(cl, file=fout)
    err_check_call(['git', 'add', '.hybrid_changelist'])
    err_check_call(['git', 'add', '.'])
    where, sep, _ = txt_check_output(['p4', 'where', '.']).partition(' //')
    if sep != ' //':
        where = '<unknown>'
    err_check_call(['git', 'commit', '-m', f"Initial import from o4 {where} @ {cl}"])
    err_print("*** INFO: [git] Hybrid initialized successfully")


def git_current_branch():
    return txt_check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip()


def is_git_hybrid():
    return os.path.isdir('.git') and not os.path.exists('.o4/hybrid_off')


def previous_hybrid_cl():
    """ Returns the most recent cl that was hybrid synced. A default of zero is not helpful
        since the original changelist that was committed to git was. That is why there is both
        a file in git and a local file.
    """
    try:
        return int(open('.hybrid_changelist').read().strip())
    except FileNotFoundError:
        return 0


def git_master_prep(depot_path, target_changelist, merge_target='master'):
    """Call before o4 sync if on a git hybrid."""

    old_changelist = previous_hybrid_cl()
    topic = git_current_branch()
    err_print(f'*** INFO: [git] Stashing local changes on {topic}...')
    try:
        stash = txt_check_output([
            'git', 'stash', 'create',
            f"o4 git stash for sync from CL {old_changelist} to {target_changelist}"
        ])
    except CalledProcessError:
        sys.exit("Failed to git stash current changes, that probably means a conflict has to not"
                 " been resolved. You need to deal with that and then retry.")
    if topic != merge_target:
        err_print(f"*** INFO: [git] Checkout merge target {merge_target}...")
        err_check_call(['git', 'checkout', merge_target])
    return dict(stash=stash,
                topic=topic,
                depot_path=depot_path,
                merge_target=merge_target,
                old_changelist=old_changelist,
                target_changelist=target_changelist)


def git_o4_import(prep_res):
    if prep_res['old_changelist'] >= prep_res['target_changelist']:
        err_print(f"*** WARNING: o4 hybrid git sync is not possible for decreasing changelists"
                  f" ({prep_res['old_changelist']} -> {prep_res['target_changelist']})")
        return
    if not prep_res['old_changelist']:
        err_print("*** WARNING: There was no original import changelist, creating .p4_original_cl")
        with open('.p4_original_cl', 'wt') as fout:
            fout.write(f"{prep_res['target_changelist']}")
        err_check_call(['git', 'add', '.p4_original_cl'])
    err_print(f"*** INFO: [git] Importing changes on {prep_res['depot_path']}"
              f"@{prep_res['old_changelist']},{prep_res['target_changelist']} "
              f"from perforce to git...")
    res = check_output(['git', 'ls-files', '--others', '--exclude-standard'],
                       universal_newlines=True)
    untracked = set(res.strip().splitlines())
    if untracked:
        adding, lfs = [], []
        for i, fs in enumerate(
                fstat_from_csv(f".o4/{prep_res['target_changelist']}.fstat.gz", fstat_split)):
            if not fs:
                continue
            if fs[F_PATH] in untracked and fs[F_CHECKSUM]:
                if os.lstat(fs[F_PATH]).st_size > 1e6:
                    # TODO: This should probably be configurable
                    lfs.append(fs[F_PATH])
                else:
                    adding.append(fs[F_PATH])
                untracked.remove(fs[F_PATH])
                if not untracked:
                    break
        with open('.o4/adding.txt', 'wt') as fout:
            for a in adding:
                print(a, file=fout)
        with open('.o4/lfs.txt', 'wt') as fout:
            for a in lfs:
                print(a, file=fout)
        err_print(f"*** INFO: [git] Adding {len(adding)} new files, and "
                  f"{len(lfs)} new large files, to git.")
        if lfs:
            err_print("*** INFO: [git] Tracking LFS files...")
            err_check_call(['git', 'lfs', 'track'] + lfs)
            adding.extend(lfs)
        if adding:
            err_print(f"*** INFO: [git] Adding {len(adding)} new files to git...", end=' ')
            while adding:
                err_print(len(adding), end=' ')
                sys.stderr.flush()
                err_check_call(['git', 'add'] + adding[:200])
                del adding[:200]
            err_print('')
        if untracked:
            err_print(
                f"*** WARNING: [git] There are {len(untracked)} untracked files not covered by o4:")
            for u in untracked:
                err_print("   ", u)
            err_print("*** WARNING: [git] The above listed files will be addeded to .gitignore")
            with open('.gitignore', 'at+') as fout:
                for u in sorted(untracked):
                    print(u, file=fout)
            err_check_call(['git', 'add', '.gitignore'])
    err_print(f"*** INFO: [git] Updating affected files in git...")
    err_check_call(['git', 'add', '-u'])
    err_print(f"*** INFO: [git] Committing changes from o4 to git {prep_res['merge_target']}...")
    res = run([
        'git', 'commit', '-m',
        (f"o4 git import {prep_res['depot_path']} from CL {prep_res['old_changelist']} to"
         f" {prep_res['target_changelist']}")
    ],
              universal_newlines=True,
              stdout=PIPE,
              stderr=STDOUT)
    err_print(f"*** {'WARNING' if res.returncode else 'INFO'}: [git] Response:",
              res.stdout.replace("\n", "\n    "))
    if res.returncode and 'nothing to commit' not in res.stdout:
        sys.exit("*** ERROR: [git] Failed to commit imported changes to git.")
    with open('.o4/hybrid_changelist', 'wt') as fout:
        print(prep_res['target_changelist'], file=fout)


def git_master_restore(prep_res):
    if prep_res['old_changelist'] >= prep_res['target_changelist']:
        return
    if git_current_branch() != prep_res['topic']:
        err_print(f"*** INFO: [git] Returning git to topic branch {prep_res['topic']}...")
        err_check_call(['git', 'checkout', prep_res['topic']])
        err_print(f"*** INFO: [git] Merging {prep_res['merge_target']} into "
                  f"topic branch {prep_res['topic']}...")
        err_check_call(['git', 'rebase', prep_res['merge_target']])

    if ':' in prep_res['stash']:
        err_print('*** INFO: Popping git stash...')
        try:
            err_check_call(['git', 'stash', 'pop'])
        except CalledProcessError:
            err_print("*** WARNING: [git] There was a problem popping the git stash, most likely "
                      "a conflict.\n"
                      "    Make sure you edit out the conflicts before you attempt to sync again.")
            return False
    return True


def main():
    cl = int(open('.o4/changelist', 'rt').read().strip())
    p = git_master_prep('//app/main/core/...', 18041051, cl)
    git_o4_import(p)
    git_master_restore(p)


if __name__ == '__main__':
    main()

##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
