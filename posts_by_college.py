#! /usr/local/bin/python3
""" Build a frequency distribution of posted dates for a transfers_applied CF query file.
    It's python so it can exceed Excel's and Numbers' row limits.
    Next: posted date by articulation and enrollment terms
"""

import csv
import datetime
import sys
import argparse
from collections import namedtuple, defaultdict
from pathlib import Path

parser = argparse.ArgumentParser('Update Transfers')
parser.add_argument('-np', '--no_progress', action='store_true')
parser.add_argument('file', nargs='?')
args = parser.parse_args()
progress = not args.no_progress

# If a file was specified on the command line, use that. Otherwise use the latest one found in
# downloads.
the_file = args.file
if the_file is None:
  # No snapshot specified; use latest available.
  files = Path('./downloads').glob('CV*ALL*')
  for file in files:
    if the_file is None or file.stat().st_mtime > the_file.stat().st_mtime:
      the_file = file
else:
  the_file = Path(the_file)

if the_file is None:
  sys.exit('No input file.')

# Using the date the file was transferred to Tumbleweed as a proxy for CF SYSDATE
file_name = the_file.name
file_date = datetime.datetime.fromtimestamp(the_file.stat().st_mtime)
iso_file_date = file_date.strftime('%Y-%m-%d')

print('Using:', the_file,
      file_date.strftime('%B %d, %Y'),
      file=sys.stderr)

posted_dates = defaultdict(int)

# Progress indicators
m = 0
num_records = len(open(the_file, encoding='ascii', errors='backslashreplace').readlines()) - 1

with open(the_file, encoding='ascii', errors='backslashreplace') as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    if reader.line_num == 1:
      headers = [h.lower().replace(' ', '_') for h in line]
      cols = [h for h in headers]

      values_added = None
      Row = namedtuple('Row', headers)
    else:
      if reader.line_num == 2 and 'sysdate' in cols:
        # SYSDATE is available: substitute it for file_date
        mo, da, yr = [int(x) for x in line[-1].split('/')]
        file_date = datetime.datetime(yr, mo, da)
        iso_file_date = file_date.strftime('%Y-%m-%d')
      m += 1
      if progress:
        print(f'  {m:06,}/{num_records:06,}\r', end='', file=sys.stderr)
      row = Row._make(line)

      if '/' in row.posted_date:
        mo, da, yr = row.posted_date.split('/')
        posted_date = datetime.date(int(yr), int(mo), int(da)).strftime('%Y-%m-%d')
        posted_dates[(row.dst_institution, posted_date)] += 1
        posted_dates['total', posted_date] += 1
      else:
        pass


with open('posted_dates/' + iso_file_date, 'w') as report:
  print(iso_file_date, file=report)
  for key in sorted(posted_dates.keys()):
    print(f'{key} {posted_dates[key]}', file=report)
