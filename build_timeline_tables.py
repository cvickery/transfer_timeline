#! /usr/local/bin/python3
"""Generate database tables for transfer timeline events."""

import csv
import datetime
import resource
import sys
import time

from collections import namedtuple, defaultdict
from pathlib import Path

from pgconnection import PgConnection
from timeline_utils import min_sec

exit('NO LONGER USED. SEE update_timeline_tables.py FOR REPLACEMENT.')

soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, [0x800, hard])


# Development connection
logfile = open('./build_baseline_tables.log', 'w')
print('Start Build Timeline Tables', file=sys.stderr)

# Connect to data sources
# -------------------------------------------------------------------------------------------------

# Transfer Evaluations
trans_conn = PgConnection('cuny_transfers')
trans_cursor = trans_conn.cursor()

# Admissions
admissions_table_files = Path('./Admissions_Registrations').glob('*ADMISSIONS*')
admissions_table_file = None
for file in admissions_table_files:
  if admissions_table_file is None or file.stat().st_mtime > admissions_table_file.stat().st_mtime:
    admissions_table_file = file
print(f'Admissions file: {admissions_table_file}', file=sys.stderr)

# Registrations
registrations_table_files = Path('./Admissions_Registrations').glob('*STUDENT*')
registrations_table_file = None
for file in registrations_table_files:
  if (registrations_table_file is None
     or file.stat().st_mtime > registrations_table_file.stat().st_mtime):
    registrations_table_file = file
print(f'Registrations file: {registrations_table_file}', file=sys.stderr)


# Admissions Table
# -------------------------------------------------------------------------------------------------
""" First build the program_reasons table in order to have the descriptions of the program actions.
"""
trans_cursor.execute("""
    drop table if exists program_reasons;
    create table program_reasons (
    institution text,
    program_action text,
    action_reason text,
    description text,
    primary key (institution, program_action, action_reason)
    );
    """)
with open('./Admissions_Registrations/prog_reason_table.csv') as infile:
  reader = csv.reader(infile)
  for line in reader:
    if reader.line_num == 1:
      cols = [col.lower().replace(' ', '_') for col in line]
      Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      if row.setid.startswith('GRD') or row.setid.startswith('UAC') or row.status != 'A':
        continue
      description = row.short_description
      if len(row.description) > len(description):
        description = row.description
      if len(row.long_description) > len(description):
        description = row.long_description
      institution = row.setid[0:3]
      trans_cursor.execute(f"""
    insert into program_reasons values('{institution}',
                                       '{row.program_action}',
                                       '{row.action_reason}',
                                       '{description}')
    """)

""" Now build the admissions table.
"""
admittees = defaultdict(dict)
Admittee_Key = namedtuple('Admittee_key',
                          'student_id application_number last_school_attended institution '
                          'admit_term requirement_term')
Admission_Event = namedtuple('Admission_Event',
                             'admit_type program_action action_reason action_date effective_date')
"""
    "ID","Career","Career Nbr","Appl Nbr","Prog Nbr","Institution","Acad Prog","Status","Eff
    Date","Effective Sequence","Program Action","Action Date","Action Reason","Admit Term","Expected
    Graduation Term","Requirement Term","Approved Academic Load","Campus","Application
    Center","Admit Type","Financial Aid Interest","Housing Interest","Application Fee
    Status","Application Fee Date","Notification Plan","Region","Recruiter","Last School
    Attended","Created On","Created By","Last Updated On","Updated By","Application
    Complete","Completed Date","Application Date","Graduation Date","Acad Level","Override
    Deposit","External Application"
"""
start_time = time.time()  # Everything before this happens sooo fast!
print('Read Admissions file', file=sys.stderr)
with open(admissions_table_file, encoding='ascii', errors='backslashreplace') as atf:
  admissions_reader = csv.reader(atf)
  for line in admissions_reader:
    if admissions_reader.line_num == 1:
      Row = namedtuple('Row', [col.lower().replace(' ', '_') for col in line])
    else:
      row = Row._make(line)
      admit_term = int(row.admit_term)
      # Fall admits are allowed to matriculate in the summer
      if (admit_term % 10) == 6:
        admit_term += 3
      try:
        requirement_term = int(row.requirement_term)
      except ValueError as ve:
        requirement_term = 0
      if row.career not in ['UGRD', 'UKCC', 'ULAG']:
        continue
      try:
        admittee_key = Admittee_Key._make([int(row.id), int(row.appl_nbr), row.last_school_attended,
                                          row.institution[0:3],
                                          admit_term, requirement_term])
      except ValueError as ve:
        print(f'Admittee Key situation: {row}\n', file=logfile)
        continue
      if row.program_action in ['APPL', 'ADMT', 'DEIN', 'MATR', 'WADM'] \
         and row.admit_type in ['TRN', 'TRD']:
        try:
          m, d, y = row.action_date.split('/')
          action_date = datetime.date(int(y), int(m), int(d))
          m, d, y = row.eff_date.split('/')
          effective_date = datetime.date(int(y), int(m), int(d))
        except ValueError as ve:
          print(f'Admittee Date situation: {row}\n', file=logfile)
          continue
        admittees[admittee_key][row.program_action] = \
            Admission_Event._make([row.admit_type,
                                   row.program_action,
                                   row.action_reason,
                                   action_date,
                                   effective_date])
print(f'{len(admittees.keys()):,} Transfer Admittees', file=sys.stderr)
end_read_admit = time.time()
print(f'That took {min_sec(end_read_admit - start_time)}', file=sys.stderr)

print('Build admissions table', file=sys.stderr)
trans_cursor.execute("""
drop table if exists admissions;

create table admissions (
student_id int,
application_number int,
last_school_attended text,
institution text,
admit_term int,
requirement_term int,
admit_type text,
program_action text,
action_reason text,
action_date date,
effective_date date,
primary key (student_id,
             application_number,
             institution,
             admit_term,
             requirement_term,
             program_action)
);
""")
for key in admittees.keys():
  for program_action in admittees[key].keys():
    trans_cursor.execute(f"""
insert into admissions values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
on conflict do nothing;
""", (key.student_id, key.application_number, key.last_school_attended, key.institution,
      key.admit_term, key.requirement_term,
      admittees[key][program_action].admit_type,
      program_action,
      admittees[key][program_action].action_reason,
      admittees[key][program_action].action_date,
      admittees[key][program_action].effective_date))
    if trans_cursor.rowcount == 0:
      print(f'Admissions Data situation: {trans_cursor.query.decode()}', file=logfile)
trans_conn.commit()

# Report: difference between action date and effective date
counts = defaultdict(int)
for admittee_key in admittees.keys():
  for program_action in admittees[admittee_key].keys():
    delta = round((((admittees[admittee_key][program_action].effective_date
                     - admittees[admittee_key][program_action].action_date).days)) / 7)
    counts[delta] += 1
with open('./reports/action-effective_differences.csv', 'w') as aed:
  print('Weeks, Frequency', file=aed)
  for days in sorted(counts.keys()):
    print(f'{days:4}, {counts[days]}', file=aed)

end_build_admit = time.time()
print(f'That took {min_sec(end_build_admit - end_read_admit)}', file=sys.stderr)


# registraton_factory()
# -------------------------------------------------------------------------------------------------
def registration_factory():
  """Generate enrollment date dictionaries."""
  return {'early_enrollment_date': None, 'end_enrollment_date': None}


# Process registration
"""
    "ID","Career","Institution","Term","Class Nbr","Course Career","Session","Student Enrollment
    Status","Enrollment Status Reason","Last Enrollment Action","Enrollment Add Date","Enrollment
    Drop Date","Units Taken","Units Taken-Academic Progress","Units Taken-Fin Aid Progress","Course
    Count","Grading Basis","Official Grade","Grade Input","Repeat Code","Include in GPA","Units
    Attempted","Grade Points","Designation","RD Option","RD Grade","Academic
    Group","Subject","Catalog Nbr","Description","Last Enrl Action Reason","Last Enrollment Action
    Process","Status"

"""
# Get first and last registration add dates for each term by student/college
Registration_Key = namedtuple('Registration_Key', 'student_id institution term')
registration_events = defaultdict(registration_factory)
m = 0
n = len(open(registrations_table_file, encoding='ascii', errors='backslashreplace').readlines()) - 1
print('Read Registrations file', file=sys.stderr)
with open(registrations_table_file, encoding='ascii', errors='backslashreplace') as rtf:
  registrations_reader = csv.reader(rtf)
  for line in registrations_reader:
    if registrations_reader.line_num == 1:
      Row = namedtuple('Row', [col.lower().replace(' ', '_').replace('-', '_') for col in line])
    else:
      m += 1
      print(f'{m:6,} / {n:,}\r', end='', file=sys.stderr)
      row = Row._make(line)
      term = int(row.term)
      if row.career != 'UGRD':
        continue
      registration_key = Registration_Key._make([row.id, row.institution[0:3], term])
      try:
        mo, da, yr = row.enrollment_add_date.split('/')
        enrollment_date = datetime.date(int(yr), int(mo), int(da))
      except ValueError as ve:
        print(f'Enrollment date situation: {row}', file=logfile)
        continue
      first = registration_events[registration_key]['early_enrollment_date']
      last = registration_events[registration_key]['end_enrollment_date']
      changed = False
      if first is None or enrollment_date < first:
        first = enrollment_date
        changed = True
      if last is None or enrollment_date > last:
        last = enrollment_date
        changed = True
      if changed:
        registration_events[registration_key] = {'early_enrollment_date': first,
                                                 'end_enrollment_date': last}

end_read_regis = time.time()
print(32 * ' ', f'\rThat took {min_sec(end_read_regis - end_build_admit)}', file=sys.stderr)
print('Build registrations table', file=sys.stderr)

trans_cursor.execute("""
drop table if exists registrations;
create table registrations (
  student_id int,
  institution text,
  term int,
  first_date date,
  last_date date,
  primary key (student_id, institution, term))
""")
for registration_key in registration_events.keys():
  trans_cursor.execute(f"""
insert into registrations values (%s, %s, %s, %s, %s)
""", (registration_key.student_id, registration_key.institution, registration_key.term,
      registration_events[registration_key]['early_enrollment_date'],
      registration_events[registration_key]['end_enrollment_date']))
end_build_regis = time.time()
print(f'That took {min_sec(end_build_regis - end_read_regis)}', file=sys.stderr)
print(f'Build Timeline Tables took {min_sec(end_build_regis - start_time)}', file=sys.stderr)

trans_conn.commit()
