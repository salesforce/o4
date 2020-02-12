import sys
import os
import configparser

# When included on a request to the fstat server, this parameter results in a
# redirection to an existing file (rather than creating one) if it's this close
# to an existing one.
# The more sparse is your submitted changelist sequence, the larger the default
# should be.
NEARBY = 5000

# The fstat server occasionally removes fstat/archive files if they're
# using too much space. There are two properties that can be set
# to give priority to that method. (Both can be set; if these defaults
# are both set to None, and the properties are not set in the configuration
# file, no space reclamation is done.) The value for each is a number of
# bytes optionally followed by a scale factor, which is k, m, or g.
DEFAULT_DISK_FREE = '5g'  # This keeps at least this much disk space free.
# The file system queried is the one where the fstat server is run from;
# it is assumed that all o4 directories are on that file system.
DEFAULT_MAX_DIR = None  # This relates to each o4 directory separately,

# and is the maximum amount of space it should use.


def init():

    def read_conf_file(filename):
        lines = (line.strip() for line in open(filename))
        lines = [line for line in lines if line and not line.startswith('#')]
        bad = [line for line in lines if '=' not in line]
        if bad:
            print(f'*** WARNING: Ignoring these lines in {filename}', file=sys.stderr)
            bad = ['             ' + line for line in bad]
            print(*bad, sep='\n', file=sys.stderr)
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
        return {}

    c = configparser.ConfigParser(strict=False, interpolation=None)
    c.read_string('[DEFAULT]\n' + conf)
    if os.environ.get('DEBUG', ''):
        for k, v in c['DEFAULT'].items():
            print(f'*** INFO: CONF {k} = {v}', file=sys.stderr)
    return dict(c['DEFAULT'].items())


props = init()


def _expand(value):
    import re

    def lookup(m):
        value = props.get(m.group(1), '')
        if not value:
            print(f'*** WARNING: Configuration variable not found: {m.group(1)}', file=sys.stderr)
        return value

    return re.sub(r'\${(.*?)}', lookup, value)


def use_zsync():
    return props.get('o4.use_zsync', False) == 'true'


def p4():
    return {
        'user': os.environ['P4USER'],
        'password': os.environ['P4PASSWD'],
        'port': os.environ['P4PORT'],
        'client': os.environ['P4CLIENT']
    }


def cmdline_args(o4_cmd):
    import shlex

    args = []
    if o4_cmd:
        a = props.get(f'o4.args.{o4_cmd}')
        if a:
            args.extend(shlex.split(a))
    a = props.get('o4.args')
    if a:
        args.extend(shlex.split(a))
    return args


def allow_nonflat_clientspec():
    allow = props.get('o4.allow_nonflat_clientspec', False)
    if allow:
        return allow == 'true'
    allow = props.get('blt.edition.dev')
    if allow:
        return allow == 'false'
    return False


def fstat_server():
    if not os.environ.get('NOO4SERVER'):
        return props.get('o4.fstat_server_url', None)


def fstat_server_nearby():
    return int(props.get('o4.fstat_server.nearby', NEARBY))


def fstat_server_auth():
    authspec = props.get('o4.fstat_server_auth', 'basic:${nexus.token.id}:${nexus.token.hash}')
    method = authspec.partition(':')[0]
    if method == 'digest':
        from requests.auth import HTTPDigestAuth
        user, password = authspec.split(':')[1:]
        user = _expand(user)
        password = _expand(password)
        return HTTPDigestAuth(user, password)
    elif method == 'basic':
        from requests.auth import HTTPBasicAuth
        user, password = authspec.split(':')[1:]
        user = _expand(user)
        password = _expand(password)
        return HTTPBasicAuth(user, password)
    return None


def fstat_server_cert():
    path = props.get('o4.fstat_server_cert', '')
    if not path or path == 'none':
        return None
    path = _expand(path)
    if not os.path.exists(path):
        print('*** WARNING: Specified certificate file for the fstat server does not exist.',
              file=sys.stderr)
        print('             ' + path, file=sys.stderr)
        return None
    return path


def maximum_o4_dir_size():
    value = props.get('o4.cache.maximum_dir_size', DEFAULT_MAX_DIR)
    return _scaled_int(value) if value else value


def minimum_disk_free():
    value = props.get('o4.cache.minimum_disk_free', DEFAULT_DISK_FREE)
    return _scaled_int(value) if value else value


def _scaled_int(val):
    val = val.strip()
    if not val:
        raise ValueError('Blank value passed to scaled_int')
    try:
        # Default to unscaled if a bare number is supplied
        val = int(val)
        scale = 'u'
    except (TypeError, ValueError):
        # Otherwise, extract the number and the scale
        val, scale = int(val[:-1]), val[-1]
    if scale == 'u':
        pass
    elif scale == 'k':
        val = val * 1024
    elif scale == 'm':
        val = val * 1024 * 1024
    elif scale == 'g':
        val = val * 1024 * 1024 * 1024
    else:
        raise ValueError('Scale factor must be k, m, or g. Got ' + scale)
    return val


##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
