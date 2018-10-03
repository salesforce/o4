import sys
import os
import configparser


def init():

    def read_conf_file(filename):
        lines = (line.strip() for line in open(filename))
        lines = [line for line in lines if line and not line.startswith('#')]
        bad = [line for line in lines if '=' not in line]
        if bad:
            print('*** WARNING: Ignoring these lines in ' + filename)
            for line in bad:
                print('    ' + line)
            lines = [line for line in lines if '=' in line]
        return '\n'.join(lines) + '\n'

    conf = ''
    try:
        conf_file = os.environ.get('O4CONFIG')
        if conf_file:
            conf += read_conf_file(conf_file)
        else:
            conf_file = os.path.expanduser('~/o4.config')
            if os.path.exists(conf_file):
                conf += read_conf_file(conf_file)
        blt_dir = os.environ.get('BLT_HOME', '')
        if blt_dir:
            conf_file = os.path.join(blt_dir, 'config.blt')
            if os.path.exists(conf_file):
                conf += read_conf_file(conf_file)
    except IOError as e:
        exit(f'Error reading o4 configuration: {e}')

    if not conf:
        exit(f'No o4 configuration file found')

    c = configparser.ConfigParser(strict=False, interpolation=None)
    c.read_string('[DEFAULT]\n' + conf)
    return dict(c['DEFAULT'].items())


props = init()


def use_zsync():
    return props.get('o4.use_zsync', False) == 'true'


def p4():
    return {
        'user': os.environ['P4USER'],
        'password': os.environ['P4PASSWD'],
        'port': os.environ['P4PORT'],
        'client': os.environ['P4CLIENT']
    }


def allow_nonflat_clientspec():
    x = props.get('o4.allow_nonflat_clientspec', False)
    if x:
        return x == 'true'
    x = props.get('blt.edition.dev')
    if x:
        return x == 'false'
    return False


##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
