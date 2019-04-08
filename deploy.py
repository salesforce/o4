#!/usr/bin/env python3
"""
USAGE:
  deploy.py [-u TARGET] [--folder=BUCKET]
  deploy.py -h

Options:
  -h --help         show this help message and exit
  -u TARGET         Target file to deploy [default: all]
  --folder=BUCKET   Upload TARGET to s3 bucket

Notes:
    Example deploy:
      $ python3 deploy.py -u build/manifold --folder o4-staging
"""


def aws_upload(uploads, folder):
    import sys
    import os
    try:
        from awscli import clidriver
    except ModuleNotFoundError:
        sys.exit('*** ERROR: Make sure you are in ansible supported python3 virtualenv \n'
                 '            and install awscli via pip if not available')
    c = clidriver.CLIDriver()
    # push to s3
    for filename in uploads:
        fpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
        c.main([
            's3', 'put-object', '--body', fpath, '--bucket', 'sfdc-ansible', '--key',
            f'{folder}/' + fpath.split('/')[-1]
        ])


if __name__ == "__main__":
    import docopt
    opts = docopt.docopt(__doc__)
    uploads = None
    if uploads == 'all':
        uploads = ['build/manifold', 'build/gatling', 'build/o4', 'o4/o4_pyforce.py']
    else:
        uploads = [opts['-u']]

    print(f'Deploying: {" ".join(uploads)}')
    aws_upload(uploads, opts['--folder'])
