#! /usr/local/bin/python3
"""Check query files to see whether it’s safe to do a timeline analysis.

Copascetic Pre-Check (may be suppressed with no_precheck option):
  Check sizes of files in query_downloads/ against corresponding files in queries/.

  If all OK and sizes match, archive current query and replace it with the new one.

Copascetic Post-Check:
  If all queries in queries_dir have the same dates, delete oldest queries from the archive and exit
  normally.

  Otherwise exit errorly.

"""
import argparse
import re
import sys

from datetime import date
from pathlib import Path

if __name__ == '__main__':
  parser = argparse.ArgumentParser('Check query files')
  parser.add_argument('-l', '--log_changes', action='store_true')
  parser.add_argument('-nop', '--no_precheck', action='store_true')
  parser.add_argument('-v', '--verbose', action='store_true')
  args = parser.parse_args()
  do_precheck = not args.no_precheck

  home_dir = Path.home()
  download_dir = Path(home_dir, 'Projects/transfer_timeline/query_downloads/')
  queries_dir = Path(home_dir, 'Projects/transfer_timeline/queries/')
  archive_dir = Path(home_dir, 'Projects/transfer_timeline/query_archive/')

  assert download_dir.is_dir()
  assert queries_dir.is_dir()
  assert archive_dir.is_dir()

  is_copacetic = True
  queries = queries_dir.glob('*.csv')
  if do_precheck:
    # For each file in queries, see if there is a newer one that is within 10% of its size.
    new_queries = download_dir.glob('*.csv')

    # Remove CF job IDs, if present
    for new_query in new_queries:
      new_stem = re.sub(r'[\-0-9]+', '', new_query.stem)
      new_query.rename(Path(download_dir, f'{new_stem}.csv'))

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
            is_copacetic = False
            print(f'{new_query.name} size check FAILED: {query_stats.st_size} :: '
                  f'{new_stats.st_size}', file=sys.stderr)
      else:
        is_copacetic = False
        print(f'{new_query.name} download NOT FOUND', file=sys.stderr)

  if not is_copacetic:
    exit('Query Download, Size, and/or Age Checks failed.')

  # Postcheck: be sure all the queries/ files are all dated the same
  reference_date = None
  for query in queries:
    query_date = date.fromtimestamp(query.stat().st_mtime)
    if reference_date is None:
      reference_name = query.name
      reference_date = query_date
    else:
      if query_date != reference_date:
        print(f'{query.name} date ({query_date}) does not match {reference_name} date '
              f'{reference_date}', file=sys.stderr)
        is_copacetic = False

  if is_copacetic:
    print('Query dates match\n  prune archive')
    today = date.today()
    for file in archive_dir.glob('*csv'):
      age = (today - date.fromtimestamp(file.stat().st_ctime)).days
      if age > 1:
        file.unlink()
        print(f'  {file.name} deleted')
    print('  done')
    exit()
  else:
    exit('Query dates DON’T match.')

