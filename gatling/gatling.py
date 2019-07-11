#!/usr/bin/python3.6
"""
Distributes stdin to multiple child processes executing in
parallel. The child processes are described by <args>. The output and
stderr from each child is collated and output as one stream without
priority to any one process, and without clipping or mashing together
lines from different children.

The <args>... arguments specifies how to launch the child processes.

This program has two different modes determined by the name with which
is invoked or by an optional first argument (gatling|manifold):

 * gatling: best for commands that communicate with remote servers
   (e.g. p4)

 * manifold: best for commands that consume mainly local resources 
   (e.g. md5)

In manifold mode a child process is created for each block read, until
max <procs> is reached, after that blocks are forwarded to the child
processes round-robin. When a child has received <max> bytes it is not
given more and must finish to make room for a new child process to be
launched.

In gatling mode one child process is created and blocks are forwarded
until <max> bytes is reached. Not until a child is saturated, is
another child processes created and filled up. As long as there is
more input new child processes are created until max <procs> is
reached. At this point one child must finish to make room for a new
child so that more blocks can be forwarded.

Usage:
  {PROGRAM} [(gatling|manifold)] [-c <bytes>] [-n <procs>] [-m <max>] [-v] [--] <args>...

Options:
  (gatling|manifold)  Override automatic behavior detection. Has to be first argument.
  -n <procs>  Maximum number of subprocesses to launch running "<args>" [default: #cpu]
  -m <max>    Maximum bytes to pass to each subprocess [default: 1048576]
  -c <bytes>  Input reading chunk size in bytes [default: 4096]
  -v          Be verbose.
"""
import sys
import os
from signal import SIGINT


def distribute(cmd, max_bytes, max_procs, chunk_size, round_robin, verbose):
    """
    Blocking function that manages all the delegation of chunks from
    stdin to subprocesses, spawning of subprocess, and collation of
    stdout and stderr from each subprocess.

    Broadly speaking, gatling reads chunks of data from stdin and
    disperses those among subprocesses to achieve parallel execution.

    Stdin is read in chunks of chunk_size bytes and truncated to the
    last newline, keeping the remainder to be prepended on the next
    chunk. Multiple chunks without newline is allowed, nothing is
    passed on until a newline is found or stdin closes.

    The stdout and stderr from each child is read in a similar
    fashion. Whenever a newline is found the output is written onto
    stdout or stderr, respectively. This collation preserves the
    format of the output, but only weakly adheres to the chronology.
    The output from gatling is the result of the subprocesses with a
    line-by-line integrity preserved with no guarantee that the order
    is exactly maintained.

    There are two different behaviors for subprocess spawning,
    manifold or gatling:

    * In manifold-mode each new chunk read from stdin spawns a new
      subprocess until max_procs is reached and then each of the
      subprocesses are fed a chunk in round robin fashion. This is an
      excellent model for programs that do not tax an external
      resource and programs that can act on stdin as soon as it is
      available.

    * In gatling-mode chunks are fed to a single subprocess until that
      processes' max_bytes is reached, the subprocess' stdin is closed
      and on the next chunk a new subprocess is spawned and fed until
      its max_bytes is reached and so on until max_procs is reached.
      If max_procs is reached gatling is blocked until a subprocess
      finishes. This mode works well for programs that connect to an
      external service or programs that don't start processing stdin
      until it is closed.

    cmd (list): The full command line for spawning subprocesses.

    max_bytes (int): Maximum bytes to pass to each subprocess before
        closing the subprocess' stdin. Increase this value if
        subprocesses do not have memory management problems from large
        input and if subprocesses process stdin as stream. Decrease
        this value if programs can not handle a large input set on
        stdin or if programs do not start processing until stdin is
        closed.

    max_procs (int): Maximum number of simultaneos subprocesses.
        Increase this number if the subprocess is largely CPU bound,
        decrease it to match the hardware if the subprocesses are IO
        bound.

    chunk_size (int): Stdin content streamed to gatling is consumed in
        chunks of this size (bytes) for efficiency reasons. Originally
        it was line by line, but that was too slow to keep
        subprocesses fed continuously. To force line-by-line behavior,
        set chunk to a size always less than the length of an input
        line (worst case 1, but try to keep it as high as possible).
        Experiment with this number to maximize pipeline throughput.

    round_robin (bool): True is manifold, false is gatling, see above
        for details.
    """

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

    my_name = 'manifold' if round_robin else 'gatling'

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
                        print(f"# {my_name} STARTED A PROCESS (1 + {running} + {len(res)}):",
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
                        print(f"# {my_name} PROCESS LIMIT {p._n:,}/{max_bytes:,}",
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
                                print(f"# {my_name} CLOSED PROCESS INPUT: {p._n:,} TIME:",
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


def main():
    """
    Parses the CLI using docopt. See the file docstring for details.
    """

    from docopt import docopt
    from multiprocessing import cpu_count

    args = sys.argv[1:]

    # Determine my_name from the invoked name (zeroth argument) or
    # the first argument if it is gatling or manifold.
    my_name, _ = os.path.splitext(os.path.basename(sys.argv[0]))
    if '--' not in args:
        for i, arg in enumerate(args):
            if not i and arg in ('gatling', 'manifold'):
                my_name = arg
                continue
            if not arg.startswith('-'):
                try:
                    int(arg)
                except ValueError:
                    args.insert(i, '--')
                    break
    opts = docopt(__doc__.replace('{PROGRAM}', my_name), args)
    if not opts['gatling'] and not opts['manifold']:
        opts['manifold'] = my_name.startswith('manifold')
    if opts['-n'] == '#cpu':
        opts['-n'] = cpu_count()
    else:
        opts['-n'] = int(opts['-n'])
    opts['-c'] = int(opts['-c'])
    opts['-m'] = int(opts['-m'])

    try:
        res = distribute(opts['<args>'], opts['-m'], opts['-n'], opts['-c'], opts['manifold'],
                         opts['-v'])
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
