#! /usr/local/bin/python3
""" Generate frequency distributions of different potential baseline measures, by college.
"""

import csv

from collections import namedtuple, defaultdict
from datetime import datetime
from pathlib import Path

from pgconnection import PgConnection

# Connect to data sources
# -------------------------------------------------------------------------------------------------

# Transfer Evaluations
trans_conn = PgConnection('cuny_transfers')
trans_cursor = trans_conn.cursor()

# Sessions
session_table_files = Path('./downloads').glob('*SESSION*')
session_table_file = None
for file in session_table_files:
  if session_table_file is None or file.stat().st_mtime > session_table_file.stat().st_mtime:
    session_table_file = file
print(f'Session Table: {session_table_file}')

# Admissions
admissions_table_files = Path('./Admissions_Registrations').glob('*ADMISSIONS*')
admissions_table_file = None
for file in admissions_table_files:
  if admissions_table_file is None or file.stat().st_mtime > admissions_table_file.stat().st_mtime:
    admissions_table_file = file
print(f'Admissions: {admissions_table_file}')

# Registrations
registrations_table_files = Path('./Admissions_Registrations').glob('*STUDENT*')
registrations_table_file = None
for file in registrations_table_files:
  if (registrations_table_file is None
     or file.stat().st_mtime > registrations_table_file.stat().st_mtime):
    registrations_table_file = file
print(f'Registrations: {registrations_table_file}')


# Sessions Cache
def session_factory(args):
  return Session._make(args)


Session = namedtuple('Session', 'first_enrollmen_date open_enrollment_date session_start')
sessions = defaultdict(session_factory)
with open(session_table_file) as stf:
  session_reader = csv.reader(stf)
  for line in session_reader:
    if session_reader.line_num == 1:
      Row = namedtuple('Row', [col.lower().replace(' ', '_') for col in line])
    else:
      row = Row._make(line)
      if row.career != 'UGRD' or row.term < '1199' or row.term > '1219':
        continue
      print(row)

