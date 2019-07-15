"""
Hybrid o4/git commands. Provides a bridge for users that want to migrate or coexist with git.

TODO: What to do about read-only perforce and not git?
"""
from o4_fstat import fstat_from_csv, fstat_split, F_CHANGELIST, F_PATH, F_REVISION, F_CHECKSUM

import os
import sys
import functools

from subprocess import check_call, check_output, CalledProcessError, run, PIPE, STDOUT

err_print = functools.partial(print, file=sys.stderr)
err_check_call = functools.partial(check_call, stdout=sys.stderr)
txt_check_output = functools.partial(check_output, universal_newlines=True)


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
        return int(open('.o4/hybrid_changelist').read().strip())
    except FileNotFoundError:
        try:
            return int(open('.p4_original_cl').read().strip())
        except FileNotFoundError:
            return 0


def git_master_prep(depot_path, target_changelist, merge_target='master'):
    """Call before o4 sync if on a git hybrid."""

    old_changelist = previous_hybrid_cl()
    topic = git_current_branch()
    err_print(f'*** INFO: Stashing local changes on {topic}...')
    try:
        stash = txt_check_output([
            'git', 'stash', 'create',
            f"o4 git stash for sync from CL {old_changelist} to {target_changelist}"
        ])
    except CalledProcessError:
        sys.exit("Failed to git stash current changes, that probably means a conflict has to not"
                 " been resolved. You need to deal with that and then retry.")
    if topic != merge_target:
        err_print(f"*** INFO: Switching to merge target {merge_target}...")
        err_check_call(['git', 'checkout', merge_target])
    return dict(stash=stash,
                topic=topic,
                depot_path=depot_path,
                merge_target=merge_target,
                old_changelist=old_changelist,
                target_changelist=target_changelist)


def git_o4_import(prep_res):
    err_print(f"*** INFO: Importing changes on {prep_res['depot_path']}"
              f"@{prep_res['old_changelist']},{prep_res['target_changelist']} "
              f"from perforce to git...")
    if not prep_res['old_changelist']:
        err_print("*** WARNING: There was no original import changelist, creating .p4_original_cl")
        with open('.p4_original_cl', 'wt') as fout:
            fout.write(f"{prep_res['target_changelist']}")
        err_check_call(['git', 'add', '.p4_original_cl'])
    for i, fs in enumerate(
            fstat_from_csv(f".o4/{prep_res['target_changelist']}.fstat.gz", fstat_split)):
        if not fs:
            continue
        if int(fs[F_CHANGELIST]) <= prep_res['old_changelist']:
            if i:
                err_print(" OK")
            break
        token = '.'
        if fs[F_REVISION] == '1':
            token = 'a'
        elif not fs[F_CHECKSUM]:
            token = 'd'
        # TODO: Check if renames screw up or git figures it out
        err_print(token, end='')
        if i and not (i % 50):
            err_print(f" {i}")
        sys.stderr.flush()
        if token != 'd':
            err_check_call(['git', 'add', fs[F_PATH]])
        else:
            res = run(['git', 'rm', '-f', fs[F_PATH]],
                      universal_newlines=True,
                      stdout=PIPE,
                      stderr=STDOUT)
            if res.returncode and 'did not match any files' not in res.stdout:
                err_print(res.stdout)
                sys.exit('*** ERROR: Failed to delete file from git.')

    err_print(f"*** INFO: Committing changes from o4 to git {prep_res['merge_target']}...")
    res = run([
        'git', 'commit', '-m',
        (f"o4 git import {prep_res['depot_path']} from CL {prep_res['old_changelist']} to"
         f" {prep_res['target_changelist']}")
    ],
              universal_newlines=True,
              stdout=PIPE,
              stderr=STDOUT)
    err_print("*** INFO: Git response:", res.stdout.replace("\n", "\n    "))
    if res.returncode and 'nothing to commit' not in res.stdout:
        sys.exit("*** ERROR: Failed to commit imported changes to git.")


def git_master_restore(prep_res):
    if git_current_branch() != prep_res['topic']:
        err_print(f"*** INFO: Returning git to topic branch {prep_res['topic']}...")
        err_check_call(['git', 'checkout', prep_res['topic']])

    if ':' in prep_res['stash']:
        err_print('*** INFO: Popping git stash...')
        try:
            err_check_call(['git', 'stash', 'pop'])
        except CalledProcessError:
            err_print(
                "*** WARNING: There was a problem popping the git stash, most likely a conflict.\n"
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
