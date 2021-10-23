#! /usr/local/bin/python3

import os
import sys

from datetime import datetime
from time import time
from pathlib import Path

from pgconnection import PgConnection

from build_timeline_tables import min_sec

""" To update the timeline data, the baseline tables need to be up to date. Updating them is a
    a manual process. Then the timeline tables can be re-built.
"""

# Report the ages of the queries used to build baseline tables
queries = ['CV_QNS_ADMISSIONS', 'CV_QNS_STUDENT_SUMMARY', 'QNS_CV_SESSION_TABLE',
           'ADMIT_ACTION_TBL', 'ADMIT_TYPE_TBL', 'PROG_REASON_TBL']
project_dir = Path('/Users/vickery/Projects/transfers_applied/')
query_dir = Path(project_dir, 'Admissions_Registrations')
today = datetime.today().timestamp()
sec_per_day = 3600 * 24

warnings = []
for query in queries:
  latest = None
  for file in Path(query_dir).glob(f'{query}*'):
    if latest is None or file.stat().st_mtime > latest.stat().st_mtime:
      latest = file
  days = int((today - latest.stat().st_mtime) / sec_per_day)
  suffix = '' if days == 1 else 's'
  if days > 0 and query in ['CV_QNS_ADMISSIONS', 'CV_QNS_STUDENT_SUMMARY', 'QNS_CV_SESSION_TABLE']:
    warnings.append(query)
  print(f'Latest {query} is {days} day{suffix} old.')

if warnings:
  is_are = 'is' if len(warnings) == 1 else 'are'
  print(f'WARNING: {len(warnings)} of the key query files {is_are} out of date.'
        f'\n Proceed anyway? (yN) ', end='')
  if not input().lower().startswith('y'):
    sys.exit('Update abandoned.')
# Normal exit
print('Query Check OK', file=sys.stderr)
exit(0)
