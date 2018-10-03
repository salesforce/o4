#!/usr/bin/python3.6
"""
Distributes stdin to multiple child processes executing in parallel. The child
processes are described by <args>. The output and stderr from each child is
collated and output as one stream without priority to any one process, and
without clipping or mashing together lines from different children.

The <args>... arguments specifies how to launch the child processes.

This program has two different modes determined by the name with which is invoked
or by an optional first argument (gatling|manifold):
 * gatling: best for commands that communicate with remote servers (e.g. p4)
 * manifold: best commands that consume mainly local resources (e.g. md5)

In manifold mode a child process is created for each block read, until max <procs>
is reached, after that blocks are forwarded to the child processes round-robin.
When a child has received <max> bytes it is not given more and much finish to make
room for a new child process to be launched.

In gatling mode one child process is created and blocks are forwarded until <max>
bytes is reached. Not until a child is saturated, is another child processes
created and filled up. As long as there is more input new child processes are
created until max <procs> is reached. At this point one child must finish to make
room for a new child so that more blocks can be forwarded.

Usage:
  {PROGRAM} [(gatling|manifold)] [-c <bytes>] [-n <procs>] [-m <max>] [-v] [--] <args>...

Options:
  (gatling|manifold)  Override automatic behavior detection.
  -n <procs>  Maximum number of subprocesses to launch running "<args>" [default: #cpu]
  -m <max>    Maximum bytes to pass to each subprocess [default: 1048576]
  -c <bytes>  Input reading chunk size in bytes [default: 4096]
  -v          Be verbose.

"""
import sys
import os
from signal import SIGINT


def distribute(cmd, max_bytes, max_procs, chunk_size, round_robin, verbose):
    from sys import stdin, stdout, stderr
    from subprocess import Popen, PIPE
    from time import sleep, time
    from selectors import DefaultSelector, EVENT_READ
    from threading import Thread
    from fcntl import fcntl, F_SETFL, F_GETFL
    from os import O_NONBLOCK

    def sink(selector, not_done):
        n = 0
        while len(not_done) > 1 or list(selector.get_map()):
            for (fileobj, _, _, p), _ in selector.select():
                chunk = fileobj.read(4096)
                if chunk:
                    i = chunk.rfind('\n') + 1
                    if i:
                        n += fileobj._trg.write(fileobj._buf)
                        n += fileobj._trg.write(chunk[:i])
                        fileobj._buf = chunk[i:]
                    else:
                        fileobj._buf += chunk
                else:
                    if p.returncode is not None:
                        n += fileobj._trg.write(fileobj._buf)
                        selector.unregister(fileobj)
            not_done[0] = n

    selector = DefaultSelector()
    res = []
    p_filled = []  # Child processes that have had their maximum input supplied
    # (p_open) Child processes that can take more input
    # In non-round-robin mode, there will only be one such child,
    # which will continually be popped and re-appended.
    # In round-robin mode, this list is rotated each time there is
    # a buffer to be written to a child.
    p_open = []
    b_in = 0
    buf = ''
    not_done = [0, 1]
    sel_t = None
    t0 = time()
    try:
        for chunk in iter(lambda: stdin.read(chunk_size), ''):
            b_in += len(chunk)
            i = chunk.rfind('\n') + 1
            if i:
                p = None
                if round_robin:
                    if len(p_open) + len(p_filled) == max_procs:
                        p = p_open.pop(0)
                else:
                    if p_open:
                        p = p_open.pop(0)
                if not p:
                    if verbose:
                        running = len(p_filled) + len(p_open)
                        print(
                            f"# {my_name} STARTED A PROCESS (1 + {running} + {len(res)}):",
                            *cmd,
                            file=sys.stderr)
                    p = Popen(cmd, encoding=stdout.encoding, stdin=PIPE, stdout=PIPE, stderr=PIPE)
                    p._n = 0
                    p._t0 = time()
                    for fo, trg in [(p.stdout, stdout), (p.stderr, stderr)]:
                        fo._buf = ''
                        fo._trg = trg
                        fcntl(fo, F_SETFL, fcntl(fo, F_GETFL) | O_NONBLOCK)
                        selector.register(fo, EVENT_READ, p)
                    if not sel_t:
                        sel_t = Thread(target=sink, args=(selector, not_done))
                        sel_t.daemon = True
                        sel_t.start()

                p._n += p.stdin.write(buf)
                p._n += p.stdin.write(chunk[:i])
                buf = chunk[i:]
                if p._n >= max_bytes:
                    t1 = time()
                    p._t1 = t1
                    td = (t1 - t0) * 1024
                    ptd = (t1 - p._t0) * 1024
                    p.stdin.close()
                    p_filled.append(p)
                    if verbose and ptd and td:
                        running = len(p_filled) + len(p_open)
                        print(
                            f"# {my_name} PROCESS LIMIT {p._n:,}/{max_bytes:,}",
                            f"({p._n/ptd:.2f} kb/s).",
                            f"INPUT: {b_in:,} ({b_in/td:.2f} kb/s) OUTPUT: {not_done[0]:,}.",
                            f"PROCESSES: {running}/{running+len(res)}",
                            file=sys.stderr)
                    while len(p_filled) == max_procs:
                        done = [d for d in p_filled if d.poll() is not None]
                        if done and verbose:
                            print(f"# {my_name} CLOSED {len(done)} PROCESSES", file=sys.stderr)
                        for d in done:
                            if verbose and p._t0 != t1:
                                print(
                                    f"# {my_name} CLOSED PROCESS INPUT: {p._n:,} TIME:",
                                    f"{t1-p._t0:.1f}/{t1-p._t1:.1f}",
                                    f"KB/S: {p._n/(t1-p._t0)/1024:.2f}",
                                    file=sys.stderr)
                            p_filled.remove(d)
                            res.append(d.returncode)
                        if not done:
                            sleep(0.5)
                else:
                    p_open.append(p)
            else:
                buf += chunk
        for p in p_open:
            p.stdin.write(buf)
            buf = ''
            p.stdin.close()
            p_filled.append(p)
    except (KeyboardInterrupt, SystemExit):
        not_done.pop()
        for p in p_open:
            p.stdin.close()
            p.kill()
        for d in p_filled:
            d.kill()
        raise
    while p_filled:
        res.append(p_filled.pop(0).wait())

    if sel_t:
        not_done.pop()
        sel_t.join()
    selector.close()
    return res


def cli():
    from docopt import docopt
    from multiprocessing import cpu_count

    global my_name
    args = sys.argv[1:]
    if args and args[0] in ('gatling', 'manifold'):
        my_name = args.pop(0)
    else:
        my_name = os.path.basename(sys.argv[0])

    if '--' not in args:
        for i, arg in enumerate(args):
            if not arg.startswith('-'):
                try:
                    int(arg)
                except ValueError:
                    args.insert(i, '--')
                    break
    opts = docopt(__doc__.replace('{PROGRAM}', my_name), args)
    if opts['-n'] == '#cpu':
        opts['-n'] = cpu_count()
    else:
        opts['-n'] = min(int(opts['-n']), cpu_count())
    opts['-c'] = int(opts['-c'])
    opts['-m'] = int(opts['-m'])
    return opts


def main():
    opts = cli()
    try:
        res = distribute(opts['<args>'], opts['-m'], opts['-n'], opts['-c'],
                         my_name.startswith('manifold'), opts['-v'])
        errs = [r for r in res if r]
        if errs:
            sys.exit(f"*** ERROR: There were {len(errs)} errors out of {len(res)} processes.")
        if opts['-v']:
            print(f"# {my_name} RAN {len(res)} PROCESSES", file=sys.stderr)
    except KeyboardInterrupt:
        print(f'*** WARNING: {my_name} aborted by user', file=sys.stderr)
        sys.exit(0 - SIGINT)
    except BrokenPipeError:
        print(f'*** ERROR: broken pipe from {opts["<args>"]}', file=sys.stderr)
        raise


if __name__ == '__main__':
    main()

##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
