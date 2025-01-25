#! /usr/local/bin/python3

# Build local session table, including only institutions of interest; sessions with enrollment dates

import csv
import sys
from collections import namedtuple, defaultdict
import datetime
from pathlib import Path

from pgconnection import PgConnection

# Build session dict (registration_start, registration_end, classes_start) keyed by (college,
# term, session).

SessionKey = namedtuple('SessionKey', 'term session college')
Session = namedtuple('Session', 'registration_start classes_start weeks')
sessions = dict()

# Put limit on which enrollment terms are of interest: from 1169 to one year from now

one_year_from_now = datetime.datetime.today() + datetime.timedelta(days=365.25)
max_term = (1000 + 10 * (one_year_from_now.year % 100) +
            [0, 2, 2, 2, 2, 6, 6, 6, 9, 9, 9, 9, 9][one_year_from_now.month])

query_file = None
query_files = Path('./downloads').glob('*SESS*')
for file in query_files:
  if query_file is None or query_file.stat().st_mtime < file.stat().st_mtime:
    query_file = file
if query_file is None:
  sys.exit('No session query files')
print(f'Using {query_file}', file=sys.stderr)
with open(query_file) as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    if reader.line_num == 1:
      col_names = [col.lower().replace(' ', '_') for col in line]
      Row = namedtuple('Row', col_names)
    else:
      row = Row._make(line)
      if row.career != 'UGRD':
        continue  # Undergrad only
      key = SessionKey._make([int(row.term), row.session, row.institution[0:3]])
      if '/' not in row.first_date_to_enroll:
        print(f'No start date on line {reader.line_num:4}: '
              f'{key.term} {key.session:4}, {key.college}', file=sys.stderr)
        continue
      m, d, y = row.first_date_to_enroll.split('/')
      registration_start = datetime.date(int(y), int(m), int(d))
      m, d, y = row.session_beginning_date.split('/')
      classes_start = datetime.date(int(y), int(m), int(d))
      session = Session._make([registration_start,
                               classes_start,
                               row.weeks_of_instruction])
      if key in sessions.keys():
        print(f'Duplicate key on line {reader.line_num:4}: '
              f'{key.term} {key.session:4}, {key.college}\n old: {sessions[key]}\n new: {session}',
              file=sys.stderr)
      sessions[key] = session

# Last digit of term is the month; 2, 6, and 9 are Spring, Summer, and Fall if session is 1.
semesters = ['?', 'Jan', 'Spring', 'Mar', 'Apr', 'May',
             'Summer', 'Jul', 'Aug', 'Fall', 'Oct', 'Nov', 'Dec']

# How many weeks between first date to enroll and session begin?
by_college = defaultdict(int)
for key in sorted(sessions.keys()):
  delta = sessions[key].classes_start - sessions[key].registration_start
  weeks = round(delta.days / 7)
  by_college[key.college, key.term % 10, weeks] += 1

with open('./weeks_by_college.csv', 'w') as report:
  print('College, Semester, Weeks, Frequency', file=report)
  for key in sorted(by_college.keys()):
    print(f'{key[0]}, {semesters[key[1]]}, {key[2]}, {by_college[key]}',
          file=report)

# Time, in weeks, between enrollment start and first, last posted date
#   sending_college, receiving_college, term/session, number of students, array of weeks, median
with open('./bad_keys', 'w') as key_errors:
  trans_conn = PgConnection('cuny_transfers')
  trans_cursor = trans_conn.cursor()
  trans_cursor.execute("""
  select count(*), student_id,
         enrollment_term as term, enrollment_session as session, dst_institution as college,
         posted_date
         from transfers_applied
         where dst_institution in ('LEH01', 'BKL01', 'QNS01')
         and enrollment_term in (1209, 1212)
         group by student_id, term, session, college, posted_date
         order by student_id, term, session, college, posted_date
  """)
  print('College, Term, Session, Start to Post Weeks')
  for row in trans_cursor.fetchall():
    try:
      session_key = SessionKey._make([int(row.term), row.session, row.college[0:3]])
      session = sessions[session_key]
      if row.posted_date is None:
        print(f'No posted_date for {row.college}, {row.term}, {row.session}', file=sys.stderr)
        continue
      print(f'{row.college}, {row.term}, {row.session}, '
            f'{(session.classes_start - row.posted_date).days / 7:.1f}')
    except KeyError as ke:
      print(ke, file=key_errors)
      continue
