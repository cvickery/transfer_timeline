#! /usr/local/bin/python3
"""Rebuild all of the timeline tables other than transfers_applied."""


from datetime import date
from pathlib import Path
from subprocess import run
from time import time
from timeline_utils import min_sec

# Queries and the scripts to rebuild their tables.
initializers = {
    'CV_QNS_ADMISSIONS.csv': 'admissions.py',
    'ADMIT_ACTION_TBL.csv': 'admit_actions.py',
    'ADMIT_TYPE_TBL.csv': 'admit_types.py',
    'PROG_REASON_TBL.csv': 'program_reasons.py',
    'CV_QNS_STUDENT_SUMMARY.csv': 'registrations.py',
    'QNS_CV_SESSION_TABLE.csv': 'sessions.py'
}

if __name__ == '__main__':
  start_time = time()
  """Verify that the queries and their corresponding initializers are available and that the query
     files all have the same date.
  """
  query_date = None
  for query, initializer in initializers.items():
    query_file = Path(f'./queries/{query}')
    assert query_file.is_file(), f'{query_file} not found'
    if query_date is None:
      query_date = date.fromtimestamp(query_file.stat().st_ctime)
    else:
      if query_date != date.fromtimestamp(query_file.stat().st_ctime):
        exit(f'{query_file} has wrong date ({date.fromtimestamp(query_file.stat().st_ctime)})')
    initializer_script = Path(f'./{initializer}')
    assert initializer_script.is_file(), f'{initializer_script} not found'

  # Run each initializer
  for query, initializer in initializers.items():
    print(f'{initializer:20}  {query}')
    run(initializer)

  print(f'Total time: {min_sec(time() - start_time)}')
