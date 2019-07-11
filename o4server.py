#!/usr/bin/env python3

import sys
import os
import time
import logging
import contextlib
from multiprocessing import Pool, Manager
from subprocess import check_output, CalledProcessError
import json

from flask import Flask, request, send_file, redirect, abort, make_response
from flask.logging import default_handler
import o4package

LOG_FORMAT = '[%(asctime)s] remote_addr=%(remote_addr)s forwarded=%(forwarded)s %(message)s'


class RequestFormatter(logging.Formatter):

    def format(self, record):
        record.remote_addr = getattr(request, 'remote_addr', '-')
        record.forwarded = request.environ.get('http_x_forwarded_for', 'not-forwarded')
        return super().format(record)


formatter = RequestFormatter(LOG_FORMAT)
default_handler.setFormatter(formatter)

app = Flask(__name__)
workers = None
app.logger.setLevel(logging.INFO)
shared = Manager().dict()  # Object shared among all workers

if 'O4_LOG' in os.environ:
    o4_log = open(os.environ['O4_LOG'], 'at')
else:
    o4_log = sys.stdout


def url(content_type, changelist, depot):
    depot = depot.replace('//', '')
    return f'/o4-http/{content_type}/{changelist}/{depot}'


def uncached(status, body, headers={}):
    resp = make_response(body, status)
    for k, v in headers.items():
        resp.headers[k] = v
    resp.headers['Cache-Control'] = 'no-cache'
    return resp


@app.route('/o4-http/help')
def help():
    return send_file('o4server.txt', mimetype='text/plain')


@app.route('/o4-http/p4password', methods=['POST'])
def change_password():
    if request.content_type != 'application/json':
        abort(401, 'Content type must be json')
    j = request.get_json()
    prev = j.get('previous-password')
    if prev != os.environ['P4PASSWD']:
        abort(403, '')
    new = j.get('new-password')
    if not new:
        abort(403, '')
    os.environ['P4PASSWD'] = new
    try:
        out = check_output(['p4', 'counter', 'change'])
        app.logger.info('Perforce password changed by request')
        shared['p4password'] = new
        return uncached(204, '')
    except Exception as e:
        os.environ['P4PASSWD'] = prev
        app.logger.info(f'Problem checking new password: {e}')
    return uncached(400, '')


@contextlib.contextmanager
def log_time(operation, changelist, depot):
    redir_cl = None
    ctx = {}
    start = time.time()
    yield ctx
    stop = time.time()
    changelist = f'@{changelist}' if changelist else ''
    msg = f'op={operation} object={depot}{changelist} elapsed={stop-start:.3f}'
    if 'redir_cl' in ctx:
        msg += f' redir={ctx["redir_cl"]}'
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    o4_log.write(f'{now} {msg}\n')
    o4_log.flush()


@app.route(url('fstat', '<int:changelist>', '<path:depot>'))
def get_fstat(changelist, depot):
    with log_time('fstat', changelist, depot) as ctx:
        nearby = request.args.get('nearby')
        if nearby:
            nearby = int(nearby)

        status, fstat = workers.apply(o4package.get_fstat, ('//' + depot, changelist, nearby))
        if status == 200:
            return send_file(fstat,
                             mimetype='application/gzip',
                             as_attachment=True,
                             attachment_filename=os.path.basename(fstat))
        if status // 100 == 3:
            fstat = os.path.basename(fstat)
            ctx['redir_cl'] = redir_cl = fstat.partition('.')[0]
            return redirect(url('fstat', int(redir_cl), depot), status)
        abort(404)


@app.route(url('archive', '<int:changelist>', '<path:depot>'))
def get_archive(changelist, depot):
    with log_time('archive', changelist, depot) as ctx:
        nearby = request.args.get('nearby')
        if nearby:
            nearby = int(nearby)

        code, archive = workers.apply(o4package.get_archive, ('//' + depot, changelist, nearby))
        if code // 100 == 3:
            archive = os.path.basename(archive)
            ctx['redir_cl'] = redir_cl = archive.partition('.')[0]
            return redirect(url('archive', int(redir_cl), depot), code)
        if archive:
            return send_file(archive,
                             mimetype='application/gzip',
                             as_attachment=True,
                             attachment_filename=os.path.basename(archive))
        if code == 202:
            return 'In progress', 202
        abort(code)


@app.route('/o4-http/changelists/<path:depot>')
def get_changelists(depot):
    with log_time('get_changelists', None, depot):
        changelists = workers.apply(o4package.get_available_changelists, ('//' + depot,))
        format = request.headers.get('accept', 'text/plain')
        if format == 'text/html':
            body = '<ol><li>' + '</li><li>'.join(changelists) + '</li></ol>'
        elif format == 'application/json':
            body = json.dumps(changelists)
        else:
            body = '\n'.join(changelists) + '\n'
        return uncached(200, body)


def purge():
    '''
    A never-returning function that periodically removes fstat and archive
    files if need be.
    '''
    from random import shuffle
    import o4_config
    import o4_fstat

    max_single_dir = o4_config.maximum_o4_dir_size()
    keep_free = o4_config.minimum_disk_free()
    if not max_single_dir and not keep_free:
        return

    def purge_all(o4dirs):
        cmd = "df -k . | tail -1 | awk '{print $4}'"
        pa = o4_fstat.prune_archive_cache
        pf = o4_fstat.prune_fstat_cache

        for prune, dir in [(pa, d) for d in o4dirs] + [(pf, d) for d in o4dirs]:
            out = check_output(cmd, shell=True, encoding=sys.stdout.encoding)
            free = int(out.strip()) * 1024
            if free < keep_free:
                prune(dir)
            else:
                break

    def purge_one(d):
        cmd = f"du -sk {d} | awk '{{print $1}}'"
        out = check_output(cmd, shell=True, encoding=sys.stdout.encoding)
        used = int(out.strip()) * 1024
        if used > max_single_dir:
            o4_fstat.prune_archive_cache(d)
        out = check_output(cmd, shell=True, encoding=sys.stdout.encoding)
        used = int(out.strip()) * 1024
        if used > max_single_dir:
            o4_fstat.prune_fstat_cache(d)

    while True:
        time.sleep(60)
        o4dirs = o4package.o4locations()
        shuffle(o4dirs)
        if keep_free:
            purge_all(o4dirs)
        if max_single_dir:
            for d in o4dirs:
                purge_one(d)


if __name__ == '__main__':
    import threading
    threading.Thread(target=purge, daemon=True).start()
    os.environ['NOO4SERVER'] = 'true'
    shared['p4password'] = os.environ['P4PASSWD']

    def share(*args):
        o4package.shared = args[0]

    workers = Pool(processes=4, initializer=share, initargs=(shared,))
    try:
        app.run(host='0.0.0.0')
    except:
        workers.close()
        workers.join()
