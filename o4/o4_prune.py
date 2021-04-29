from pathlib import Path
from time import time

from o4_fstat import prune_fstat_cache


def prune_data(o4_dir):
    prune_fstat_cache(o4_dir)
    prune_cleaned(o4_dir)


def prune_cleaned(o4_dir):
    '''
    Remove all read-only files from an abandoned cleaned directory
    (unless it's too recent).
    '''
    cleaned = Path(o4_dir) / 'cleaned'
    if not cleaned.exists():
        return
    if not cleaned.is_dir():
        print('*** WARNING: "cleaned" is not a directory! Not trying to prune it.')
        return
    age = time() - cleaned.stat().st_mtime
    if age < 86400 * 2:
        print('*** INFO: You have files left behind by a recent "clean" operation.')
        print('          It will be pruned soon.')
        return
    print('*** INFO: Pruning abandoned "cleaned" directory')
    prune_item(cleaned)
    if not cleaned.exists():
        return
    n_left = sum(1 for f in cleaned.glob('**/*') if not f.is_dir())
    s = '' if n_left == 1 else 's'
    print(f'*** WARNING: Abandoned "cleaned" directory still contains {n_left:,d}')
    print(f'             possibly valuable file{s}.')
    print(f'             Please check if they contain work that you do not want')
    print(f'             to lose. If not, remove your cleaned directory with')
    print(f'             rm -rf {cleaned.resolve()}')


def prune_item(p):
    if p.is_symlink():
        print(f'*** WARNING: Doing nothing with symlink {p} -> {p.readlink()}')
    elif p.is_dir():
        for child in p.iterdir():
            prune_item(child)
        if not list(p.iterdir()):
            p.rmdir()
    elif p.is_file():
        if p.stat().st_mode & 0o700 == 0o400:
            p.unlink()
    else:
        print(f'*** WARNING: Doing nothing with non-regular file {p.resolve()}')
