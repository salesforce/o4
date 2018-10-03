#!/usr/bin/env python3
"""
Usage:
  versioning.py -r <requirements> -z <zipapp> -o <versionfile>

Options:
  -r <requirements>  Path to the zipapp's requirements.txt
  -z <zipapp>        Path to the directory that is going to be compressed
  -o <versionfile>   Path to the input/output version file inside the zipapp folder
"""

TEMPLATE = """
# This file is generated, do not modify

import datetime.datetime

VERSION = ({major}, {minor}, {step})
VERSION_STR = '{major}.{minor}.{step}'
PRODUCT = {product!r}

REQ_MD5 = {rm!r}
PY_MD5 = {py!r}

TIMESTAMP = {ts!r}
USER_NAME = {name!r}
USER_EMAIL = {mail!r}
"""
import sys


def crc(paths):
    import hashlib
    hash_md5 = hashlib.md5()
    for path in paths:
        with open(path, 'rb') as fin:
            hash_md5.update(fin.read())
    return hash_md5.hexdigest()


def main():
    from glob import iglob
    from datetime import datetime
    from docopt import docopt
    from subprocess import check_output, CalledProcessError

    def parse(t):
        return eval(t, {}, {})

    def git_config(key):
        try:
            return check_output(['git', 'config', '--get', key],
                                encoding=sys.stdin.encoding).strip()
        except CalledProcessError:
            return ''

    opts = docopt(__doc__)
    name = git_config('user.name')
    mail = git_config('user.email')
    from configparser import ConfigParser
    cfg = ConfigParser(strict=False, interpolation=None)
    try:
        with open(opts['-o'], 'rt') as fin:
            lines = [line for line in fin if '=' in line]
    except FileNotFoundError:
        lines = TEMPLATE.format(
            major=1,
            minor=0,
            step=0,
            ts=datetime.now(),
            rm='',
            py='',
            name=name,
            mail=mail,
            product=opts['-z'])
        lines = [line + '\n' for line in lines.split('\n') if '=' in line]
    cfg.read_string('[DEFAULT]\n' + ''.join(lines))
    cfg = cfg['DEFAULT']

    rm = crc([opts['-r']])
    py = crc(
        path for path in iglob(opts['-z'] + '/*.py', recursive=True)
        if not path.endswith('/version.py'))
    major, minor, step = parse(cfg['version'])
    if rm != parse(cfg['req_md5']):
        step = 0
        minor += 1
    elif py != parse(cfg['py_md5']):
        step += 1

    with open(opts['-o'], 'wt') as fout:
        try:
            fout.write(
                TEMPLATE.format(
                    major=major,
                    minor=minor,
                    step=step,
                    ts=datetime.now(),
                    rm=rm,
                    py=py,
                    name=name,
                    mail=mail,
                    product=opts['-z']))
            print(f"{opts['-o']}: {major}.{minor}.{step}")
        except:
            from traceback import print_exc
            print_exc()
            fout.seek(0, 0)
            fout.truncate()
            fout.write(''.join(lines))


if __name__ == '__main__':
    main()
