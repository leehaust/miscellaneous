import os

import re
import sys

import pandas as pd

def walk(root):
    _walk = os.walk(root)
    return pd.DataFrame(
        [(root, dirs, files) for root, dirs, files in _walk],
        columns=['root', 'dirs', 'files']
    ).set_index('root', verify_integrity=True)


def _expand_series_of_iterables(srs):
    return srs.apply(pd.Series, dtype=str).stack().droplevel(-1).rename(srs.name)


def make_files(walk_results):
    return _expand_series_of_iterables(walk_results['files'].rename('file'))


def get_files(root):
    walk_results = walk(root)
    return make_files(walk_results)


def make_dirs(walk_results):
    return _expand_series_of_iterables(walk_results['dirs'].rename('dir'))


def get_dirs(root):
    walk_results = walk(root)
    return make_dirs(walk_results)


def make_paths(file_results):
    _file_results = file_results.reset_index()
    _file_results['path'] = _file_results.apply(
            lambda srs: os.path.join(srs.loc['root'], srs.loc['file']),
            axis=1)
    return _file_results.set_index(['root', 'file'], verify_integrity=True)


def grep_files(pattern, file_paths, encoding=None):

    _found_records = list()

    for path in file_paths:
        try:
            with open(path, 'r', encoding=encoding) as f:
                for line in f.readlines():
                    if re.search(pattern, line):
                        _found_records.append([path, line.strip(), pattern])
        except Exception as e:
            print(f"Failed on {path} with exception {e}")

    return pd.DataFrame(_found_records, columns=['path', 'line', 'pattern']).set_index(
        ['path'])


def get_file_stats(file_paths):
    return file_paths.map(os.stat)


if __name__ == '__main__':
    walk_results = walk(r'.')
    print(walk_results.to_markdown())
    print(make_files(walk_results).to_markdown())
    print(make_dirs(walk_results).to_markdown())
    print(make_paths(make_files(walk_results)).to_markdown())

    get_file_stats(make_paths(make_files(walk_results))['path']).apply(pd.Series)

    grep_result = grep_files(
        pattern='walk',
        file_paths=make_paths(make_files(walk_results))['path'],
        encoding='latin-1'
    )
    print(grep_result)
