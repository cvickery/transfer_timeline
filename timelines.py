#! /usr/local/bin/python3
""" Event Dates:
      Session: Early Registration, Open Registration, Classes Start
      Admissions: Apply, Admit, Matric
      Registrations: First, Latest
      Transfers: First, Latest

    Algorithm:
      Get session event dates: these are fixed for all members of the cohort
      Get all students in the cohort who were admitted
      Get all students in the cohort who matriculated, along with their articulation terms
      For each student who matriculated, for the articulation term:
        First transfer fetch
        Latest transfer fetch
        First registration
        Latest registration

    Report, as of report date:
      Number of admitted; number matriculated
      [Do transfers get fetched if the student does not matriculate?]
      Descriptive statistics: mean, std dev, median, mode, range, siqr for the following intervals:
        Apply to Admit
        Admit to Matric
        Matric to First Register
        Matric to Latest Register
        Admit to first Fetch
        Admit to Latest Fetch
        Matric to First Fetch
        Matric to Latest Fetch

    Not looked at, but potentially useful info:
      Students who belong to more than one transfer cohort for a semester. Does transfer fetch
      timing make them go to another college?
"""

import sys
import argparse

from collections import namedtuple, defaultdict

from pgconnection import PgConnection

institutions = {'BAR': 'Baruch', 'BCC': 'Bronx', 'BKL': 'Brooklyn', 'BMC': 'BMCC',
                'CSI': 'Staten Island', 'CTY': 'City', 'HOS': 'Hostos', 'HTR': 'Hunter',
                'JJC': 'John Jay', 'KCC': 'Kingsborough', 'LAG': 'LaGuardia', 'LEH': 'Lehman',
                'MEC': 'Medgar Evers', 'NCC': 'Guttman', 'NYT': 'City Tech', 'QCC': 'Queensborough',
                'QNS': 'Queens', 'SLU': 'Labor/Urban', 'SOJ': 'Journalism',
                'SPH': 'Public Health', 'SPS': 'SPS', 'YRK': 'York'}


# Initialize
# -------------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser('Timelines by Cohort')
parser.add_argument('-a', '--admit_term', default=None)  # Or "articulation" term
parser.add_argument('-i', '--institution', default=None)
args = parser.parse_args()
if args.admit_term is None or args.institution is None:
  sys.exit(f'Missing cohort information: -a admit_term -i institutions')
try:
  admit_term = int(args.admit_term)
  year = 1900 + 100 * int(admit_term / 1000) + int(admit_term / 10) % 100
  assert year > 1989 and year < 2026, f'Admit Term year ({year}) must be between 1990 and 2025'
  month = admit_term % 10
  if month == 2:
    semester = 'Spring'
  elif month == 9:
    semester = 'Fall'
  else:
    print(f'Warning: month ({month}) should be 2 for Spring or 9 for Fall.\n'
          '  Continue anyway? (yN)',
          end='', file=sys.stderr)
    if not input().lower().startswith('y'):
      exit('Exit')
    semester = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][month - 1]
  semester = f'{semester}, {year}'
except ValueError as ve:
  sys.exit(f'“{args.admit_term}” is not a valid CUNY term')

institution = args.institution.strip('01').upper()
if institution not in institutions:
  sys.exit(f'“{args.institution}” is not a valid CUNY institution')

conn = PgConnection('cuny_transfers')
cursor = conn.cursor()

# Get session events
# -------------------------------------------------------------------------------------------------
Session = namedtuple('Session', 'institution term session first_enrollment open_enrollment '
                     'last_enrollment session_start session_end ')
cursor.execute(f"""
    select * from sessions where institution = '{institution}' and term = {admit_term}
    """)
session = None
for row in cursor.fetchall():
  if session is None or row.session == '1':
    session = Session._make(row)
if session is None:
  sys.exit(f'No session found for {admit_term}')

# Create a spreadsheet with the cohort's events for debugging/tableauing
# ------------------------------------------------------------------------------------------------
with open(f'./timelines/{institution}_{admit_term}.csv', 'w') as spreadsheet:
  print('Student_ID, Apply, Admit, Matric, First_Fetch, Latest_Fetch, '
        'First_Register, Latest_Register', file=spreadsheet)

  # Get students and their admission events
  # -----------------------------------------------------------------------------------------------
  Admission = namedtuple('Admission', 'student_id application_number institution '
                         'admit_term requirement_term event_type admit_type action_date '
                         'effective_date ')
  cursor.execute(f"""
      select * from admissions
       where institution = '{institution}'
         and admit_term = '{admit_term}'
         and event_type in ('APPL', 'ADMT', 'MATR')
      """)
  students = defaultdict(dict)
  for row in cursor.fetchall():
    students[int(row.student_id)][row.event_type] = row.effective_date

  # Provide default (None) values for all events
  for student_id in students.keys():
    appl, admt, matr, first_fetch, latest_fetch, first_register, latest_register = (None, None,
                                                                                    None, None,
                                                                                    None, None,
                                                                                    None)
    if 'APPL' not in students[student_id].keys():
      students[student_id]['APPL'] = None
    if 'ADMT' not in students[student_id].keys():
      students[student_id]['ADMT'] = None
    if 'MATR' not in students[student_id].keys():
      students[student_id]['MATR'] = None
    print(f"{student_id}, {students[student_id]['APPL']}, {students[student_id]['APPL']}, "
          f"{students[student_id]['APPL']}", file=spreadsheet)
  print(f'{len(students):,} students in cohort', file=sys.stderr)

  # Transfers Applied dates
  # -----------------------------------------------------------------------------------------------
  cursor.execute(f"""
    select student_id, min(posted_date), max(posted_date)
      from transfers_applied
     where dst_institution ~* '{institution}'
       and articulation_term = {admit_term}
  group by student_id
    """)
  for row in cursor.fetchall():
    if int(row.student_id) in students.keys():
      students[row.student_id][first_fetch] = row.min
      students[row.student_id][latest_fetch] = row.max
  exit()

for institution in requested_institutions:
  # Get Cohort
  # -----------------------------------------------------------------------------------------------
  cursor.execute(f"""
  select admit_term, institution, student_id  from admissions
   where admit_term = {admit_term}
     and institution = '{institution}'
     and event_type = 'ADMT'
  group by admit_term, institution, student_id
  """)
  cohort = [(CohortKey._make([row.admit_term, row.institution, row.student_id]))
            for row in cursor.fetchall()]
  print(f'{len(cohort):6,} transfer students admitted to {institutions[institution]} for '
        f'{semester}', file=sys.stderr)
  print(f'{len(cohort):6,} transfer students admitted to {institutions[institution]} for '
        f'{semester}')

  # Get Timeline Events
  # -------------------------------------------------------------------------------------------------
  student_events = defaultdict(dict)

  # For development purposes, make separate queries for each table rather than a faster but harder
  # to debug single query.

  # Admission Events
  print('Lookup Admission Events', file=sys.stderr)
  AdmissionEvent = namedtuple('AdmissionEvent', 'action_date effective_date')
  for cohort_key in cohort:
    cursor.execute(f"""
    select event_type, action_date, effective_date
      from admissions
     where institution = '{institution}'
       and admit_term = {admit_term}
       and student_id = {cohort_key.student_id}
  """)
    for row in cursor.fetchall():
      student_events[cohort_key][row.event_type] = AdmissionEvent._make([row.action_date,
                                                                         row.effective_date])

  # Transfer Evaluations and Registrations
  print('Lookup Evaluations and Registrations', file=sys.stderr)
  EvaluationEvent = namedtuple('EvaluationEvent', 'src_institution posted_date')
  RegistrationDates = namedtuple('RegistrationDates', 'first_date last_date')
  for cohort_key in cohort:
    # Evaluations
    cursor.execute(f"""
    select src_institution, posted_date
      from transfers_applied
     where student_id = {cohort_key.student_id}
       and dst_institution ~* '{institution}'
  group by src_institution, posted_date,
           student_id, dst_institution
  """)
    evaluation_list = [EvaluationEvent._make([row.src_institution, row.posted_date])
                       for row in cursor.fetchall()]
    cursor.execute(f"""
      select src_institution, posted_date
        from transfers_changed
       where student_id = {cohort_key.student_id}
         and dst_institution ~* '{institution}'
         and articulation_term = {admit_term}
    group by src_institution, posted_date,
            student_id, dst_institution
    """)
    evaluation_list += [EvaluationEvent._make([row.src_institution, row.posted_date])
                        for row in cursor.fetchall()]
    student_events[cohort_key]['evaluations'] = evaluation_list

    # Registrations
    cursor.execute(f"""
        select * from registrations
        where institution = '{cohort_key.institution}'
          and student_id = {cohort_key.student_id}
    """)
    registration_events = dict()
    for row in cursor.fetchall():
      dates = RegistrationDates._make([row.first_date, row.last_date])
      assert row.term not in registration_events.keys()
      registration_events[row.term] = dates
    student_events[cohort_key]['registrations'] = registration_events

  # Generate CSV
  print('Generate CSV Report', file=sys.stderr)
  with open(f'./reports/{institution}_{semester.replace(", ", "_")}.csv', 'w') as report:
    print('Student, Term, From_College(s), To_College, Apply, Admit, Matric, Eval_Dates, '
          'Admit_Term_Registrations, Other_Term_Registrations',
          file=report)
    for cohort_key in cohort:
      if 'APPL' in student_events[cohort_key].keys():
        appl_date = student_events[cohort_key]['APPL'].action_date.isoformat()
      else:
        appl_date = '--'
      if 'ADMT' in student_events[cohort_key].keys():
        admt_date = student_events[cohort_key]['ADMT'].action_date.isoformat()
      else:
        admt_date = '--'
      if 'MATR' in student_events[cohort_key].keys():
        matr_date = student_events[cohort_key]['MATR'].action_date.isoformat()
      else:
        matr_date = '--'
      # Sets of Dates and Sending Colleges
      eval_dates = set()
      sending_colleges = set()
      for e in student_events[cohort_key]['evaluations']:
        if e.posted_date is not None:
          eval_dates.add(e.posted_date.isoformat())
        sending_colleges.add(e.src_institution[0:3])
      if len(eval_dates) > 0:
        eval_dates = ' | '.join(sorted(eval_dates))
      else:
        eval_dates = '--'
      sending_colleges = ' '.join(sorted(sending_colleges))

      # Registration dates for admit term and for other terms at the same college
      regs = student_events[cohort_key]['registrations']
      if cohort_key.admit_term in regs.keys():
        admit_term_regs = (f'{regs[cohort_key.admit_term].first_date.isoformat()} | '
                           f'{regs[cohort_key.admit_term].last_date.isoformat()}')
        del(regs[cohort_key.admit_term])
      else:
        admit_term_regs = '--'
      other_term_regs = '; '.join([f'{term}: {regs[term].first_date.isoformat()} | '
                                   f'{regs[term].last_date.isoformat()}'for term in regs.keys()])

      print(f'{cohort_key.student_id:08} , {cohort_key.admit_term}, {sending_colleges}, '
            f'{cohort_key.institution}, {appl_date}, {admt_date}, {matr_date}, {eval_dates}, '
            f'{admit_term_regs}, {other_term_regs}', file=report)
