#! /usr/local/bin/python3
"""Check sizes of files in query_downloads.

If OK, archive current query and replace it with the new one.
"""
import argparse
import re
import sys

from datetime import date
from pathlib import Path

if __name__ == '__main__':
  parser = argparse.ArgumentParser('Check query files')
  parser.add_argument('-l', '--log_changes', action='store_true')
  parser.add_argument('-v', '--verbose', action='store_true')
  args = parser.parse_args()

  home_dir = Path.home()
  download_dir = Path(home_dir, 'Projects/transfer_timeline/query_downloads/')
  queries_dir = Path(home_dir, 'Projects/transfer_timeline/queries/')
  archive_dir = Path(home_dir, 'Projects/transfer_timeline/query_archive/')

  assert download_dir.is_dir()
  assert queries_dir.is_dir()
  assert archive_dir.is_dir()

  # For each file in queries, see if there is a newer one that is within 10% of its size.
  queries = queries_dir.glob('*.csv')
  new_queries = download_dir.glob('*.csv')

  # Remove CF job IDs, if present
  for new_query in new_queries:
    new_stem = re.sub(r'[\-0-9]+', '', new_query.stem)
    new_query.rename(Path(download_dir, f'{new_stem}.csv'))

  # HTML-ize logging info
  print('<pre>')
  # Do each existing query file
  for query in queries:
    query_stats = query.stat()
    new_query = Path(download_dir, query.name)
    if new_query.is_file():
      new_stats = new_query.stat()
      if new_stats.st_mtime < query_stats.st_mtime:
        print(f'{new_query.name} download is OLDER')
      else:
        if args.verbose:
          print(f'{new_query.name} download date is ok')
        if abs(query_stats.st_size - new_stats.st_size) < 0.1 * query_stats.st_size:
          if args.verbose:
            print(f'{new_query.name} size is ok')
          # Archive query
          new_stem = f'{date.fromtimestamp(query_stats.st_mtime)}.{query.stem}'
          if args.log_changes:
            print(f'Move {queries_dir.name}/{query.name} to {archive_dir.name}/{new_stem}.csv')
          query.rename(Path(archive_dir, f'{new_stem}.csv'))
          # Move download to queries_dir
          if args.log_changes:
            print(f'Move {download_dir.name}/{new_query.name} to '
                  f'{queries_dir.name}/{new_query.name}')
          new_query.rename(Path(queries_dir, new_query.name))
        else:
          print(f'{new_query.name} size check FAILED: {query_stats.st_size} :: {new_stats.st_size}')
    else:
      print(f'{new_query.name} download NOT FOUND')

  print('</pre>')
