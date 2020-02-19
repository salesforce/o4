"""
Very primitive method for giving some indication of progress when
the o4 command is long running. It's not fail safe. Sometimes a few
small files are synced first and then no update while downloading a
few very large files.

Output is written directly to the TTY so it's neither stderr or
stdout.
"""
import os
import sys

# ANSI sequence to clear a line and carriage return
CLR = '%c[2K\r' % chr(27)


class TTY(object):

    def __init__(self):
        self.tty = open('/dev/tty', 'wb+', buffering=0)

    def __call__(self, *args, **kw):
        from io import StringIO
        sio = StringIO()
        kw['file'] = sio
        print(*args, **kw)
        self.tty.write(sio.getvalue().encode(sys.stdout.encoding))

    def write(self, s):
        self.tty.write(s.encode(sys.stdout.encoding))
        return self

    def flush(self):
        self.tty.flush()


def swap_filename(path, replacement='.fstat'):
    """Replaces the basename in path with replacement."""
    if not os.path.isdir(path):
        path = os.path.dirname(path)
    return os.path.join(path, replacement)


def progress_enabled():
    """
    Checks if progress is enabled. To disable:
        export O4_PROGRESS=false
    """
    return os.environ.get('O4_PROGRESS', 'true') == 'true'


def progress_iter(it, path, desc, delay=0.5, delta=500):
    """
    Use this to wrap an iterator you would like to present progress
    for.
    """
    with open(swap_filename(path), 'wt') as pout:
        try:
            for n, r in enumerate(it):
                if n % delta == 0:
                    pout.seek(0)
                    print(f"{desc}: {n}", file=pout)
                    pout.flush()
                yield r
        finally:
            pout.seek(0)
            pout.truncate()
            print("-", file=pout)


def progress_off_iter(it, path, desc, delay=0.5, delta=500):
    return it


if not progress_enabled():
    progress_iter = progress_off_iter


def progress_show(path, delay=0.45):
    from time import sleep
    from threading import Thread

    CSI = '\033[%dm'
    OFF = CSI % 0
    COL = CSI % 35
    tty = TTY()
    n = [0]

    def _follow(path=path, tty=tty, n=n):

        def avg(n):
            if len(n) < 2:
                return '-'
            res = sum([float(a - b) / (len(n) - 1) for a, b in zip(n[1:], n)])
            res /= delay
            if res > 100:
                res = f'{res:.0f}'
            elif res >= 10:
                res = f'{res:.1f}'
            else:
                res = f'{res:.2f}'
            while len(n) > 15:
                n.pop(0)
            return res

        try:
            fin = None
            p_i = []
            p_n = []
            p_lbl = None
            while n:
                try:
                    fin = open(swap_filename(path), 'rt')
                except FileNotFoundError:
                    sleep(delay)
                    continue

                fin.seek(0)
                t = fin.read()
                if ':' in t and '\n' in t:
                    lbl, i = t.strip().rsplit(':', 1)
                    if lbl != p_lbl:
                        del p_i[:]
                        del p_n[:]
                        p_lbl = lbl
                    i = i.strip()
                    try:
                        i = int(i)
                        p_i.append(i)
                        p_n.append(n[0])
                        s = f" ({avg(p_i)} -> {avg(p_n)} per second)"
                    except:
                        s = ''
                    tty.write(f"{CLR}{COL}{i} {lbl} -> {n[0]} processed{s}{OFF}\r").flush()
                sleep(delay)
        except IndexError:
            return
        finally:
            if fin:
                fin.close()

    if progress_enabled():
        t = Thread(target=_follow)
        t.daemon = True
        t.start()

    for chunk in iter(lambda: sys.stdin.read(4096), ''):
        sys.stdout.write(chunk)
        n[0] += chunk.count('\n')
    n.pop()


##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
