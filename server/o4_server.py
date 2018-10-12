#!/usr/bin/env python3.6
"""
Usage: o4_server.py [-b <address>] [-p <port>] [-s <storage>] [-U <p4user>] [-W <p4pass>] [-P <p4port>] [-C <p4client>]

Options:
-b <address>   Bind address
-p <port>      Bind port
-s <storage>   Storage directory for state, default CWD
-P <p4port>    P4 port, default $P4PORT
-U <p4user>    P4 user, default $P4USER or $USER
-W <p4pass>    P4 password, default $P4PASSWD
-C <p4client>  P4 clientspec name
"""
import os
import sys


def main():
    import docopt
    opts = docopt.docopt(__doc__)
    if opts['-W']:
        os.environ['P4PASSWD'] = opts['-W']
    if opts['-P']:
        os.environ['P4PORT'] = opts['-P']
    if opts['-U']:
        os.environ['P4USER'] = opts['-U']
    if opts['-C']:
        os.environ['P4CLIENT'] = opts['-C']
    print(opts)


if __name__ == '__main__':
    main()
