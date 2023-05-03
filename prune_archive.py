#! /usr/local/bin/python3
"""Remove queries from the archive that are older than the most-recently-archived set."""

from datetime import date
from pathlib import Path

archive_dir = Path('./queries_archive')
assert archive_dir.is_dir(), f'./queries_archive not here.'
latest = None
for query_file in archive_dir.glob('*.csv'):
  if latest is None or query_file.stat().st_mtime > latest:
    latest = query_file.stat().st_mtime

print('latest query set date is', date.fromtimestamp(latest))

latest_date = date.fromtimestamp(latest)
for victim in archive_dir.glob('*.csv'):
  if (diff := (latest_date - date.fromtimestamp(victim.stat().st_mtime)).days) > 0:
    print(f'{victim.name:40} was {diff:3} days older than {latest_date}')
    victim.unlink()
  else:
    print(f'{victim.name:40} LIVES')
