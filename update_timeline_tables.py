#! /usr/local/bin/python3
"""Rebuild all of the timeline tables other than transfers_applied."""

import psycopg
import sys

from pathlib import Path
from psycopg.rows import namedtuple_row
from subprocess import run

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
  """Verify that the queries and their corresponding initializers are valid"""
  for query, initializer in initializers.items():
    query_file = Path(f'./queries/{query}')
    assert query_file.is_file(), f'{query_file} not found'
    initializer_script = Path(f'./{initializer}')
    assert initializer_script.is_file(), f'{initializer_script} not found'

  # Run each initializer
  for query, initializer in initializers.items():
    print(f'{initializer:20}  {query}')
    run(initializer)
