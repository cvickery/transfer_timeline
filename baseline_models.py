#! /usr/local/bin/python3
""" Generate frequency distributions of different potential baseline measures, by college.
"""

import csv

from collections import namedtuple, defaultdict
import datetime
from pathlib import Path

from pgconnection import PgConnection

# Development connection
debug = open('./debug', 'w')

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
# -------------------------------------------------------------------------------------------------
def session_factory(args):
  return Session._make(args)


Session = namedtuple('Session', 'first_enrollment_date open_enrollment_date last_enrollment_date '
                     'session_start_date session_end_date')
Session_Key = namedtuple('Session_Key', 'institution term session')
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

      try:
        m, d, y = row.first_date_to_enroll.split('/')
        first_enrollment_date = datetime.date(int(y), int(m), int(d))
        m, d, y = row.last_date_to_enroll.split('/')
        last_enrollment_date = datetime.date(int(y), int(m), int(d))
        m, d, y = row.open_enrollment_date.split('/')
        open_enrollment_date = datetime.date(int(y), int(m), int(d))
        m, d, y = row.session_beginning_date.split('/')
        session_start_date = datetime.date(int(y), int(m), int(d))
        m, d, y = row.session_end_date.split('/')
        session_end_date = datetime.date(int(y), int(m), int(d))
      except ValueError as ve:
        print(f'Session Date situation: {row}\n', file=debug)
        continue
      session_key = Session_Key._make([row.institution[0:3], int(row.term), row.session])
      sessions[session_key] = Session._make([first_enrollment_date, open_enrollment_date,
                                            last_enrollment_date, session_start_date,
                                            session_end_date])

# for session_key in sorted(sessions.keys()):
#   if session_key.term % 10 == 6:
#     print(f'{session_key.institution} {session_key.term} {session_key.session}: '
#           f'{sessions[session_key]}', file=debug)


# Admissions Cache
# -------------------------------------------------------------------------------------------------
admittees = defaultdict(dict)
Admittee_Key = namedtuple('Admittee_key', 'student_id institution admit_term, requirement_term')
Admission_Event = namedtuple('Admission_Event', 'action_date effective_date')
"""
"ID","Career","Career Nbr","Appl Nbr","Prog Nbr","Institution","Acad Prog","Status","Eff
Date","Effective Sequence","Program Action","Action Date","Action Reason","Admit Term","Expected
Graduation Term","Requirement Term","Approved Academic Load","Campus","Application Center","Admit
Type","Financial Aid Interest","Housing Interest","Application Fee Status","Application Fee
Date","Notification Plan","Region","Recruiter","Last School Attended","Created On","Created
By","Last Updated On","Updated By","Application Complete","Completed Date","Application
Date","Graduation Date","Acad Level","Override Deposit","External Application"
"""
with open(admissions_table_file, encoding='ascii', errors='backslashreplace') as atf:
  admissions_reader = csv.reader(atf)
  for line in admissions_reader:
    if admissions_reader.line_num == 1:
      Row = namedtuple('Row', [col.lower().replace(' ', '_') for col in line])
    else:
      row = Row._make(line)
      admit_term = int(row.admit_term)
      try:
        requirement_term = int(row.requirement_term)
      except ValueError as ve:
        requirement_term = 0
      if row.career != 'UGRD' or admit_term < 1199 or admit_term > 1219:
        continue
      try:
        admittee_key = Admittee_Key._make([int(row.id), row.institution[0:3], admit_term,
                                          requirement_term])
      except ValueError as ve:
        # print(f'Admittee Key situation: {row}') # Only one bogus role found.
        continue
      if row.program_action in ['APPL', 'ADMT', 'DEIN', 'MATR']:
        try:
          m, d, y = row.action_date.split('/')
          action_date = datetime.date(int(y), int(m), int(d))
          m, d, y = row.eff_date.split('/')
          effective_date = datetime.date(int(y), int(m), int(d))
        except ValueError as ve:
          print(f'Admittee Date situation: {row}\n', file=debug)
          continue
        admittees[admittee_key][row.program_action] = \
            Admission_Event._make([action_date, effective_date])
print(f'{len(admittees.keys())} Admittees')
for admittee_key in admittees.keys():
  print(f'{admittee_key}: {len(admittees[admittee_key])} events')
