#! /usr/local/bin/python3

import sys
import argparse

from collections import namedtuple, defaultdict

from pgconnection import PgConnection

CohortKey = namedtuple('CohortKey', 'admit_term institution student_id')

institutions = {'BAR': 'Baruch', 'BCC': 'Bronx', 'BKL': 'Brooklyn', 'BMC': 'BMCC',
                'CSI': 'Staten Island', 'CTY': 'City', 'HOS': 'Hostos', 'HTR': 'Hunter',
                'JJC': 'John Jay', 'KCC': 'Kingsborough', 'LAG': 'LaGuardia', 'LEH': 'Lehman',
                'MEC': 'Medgar Evers', 'NCC': 'Guttman', 'NYT': 'City Tech', 'QCC': 'Queensborough',
                'QNS': 'Queens', 'SLU': 'Labor/Urban', 'SOJ': 'Journalism',
                'SPH': 'Public Health', 'SPS': 'SPS', 'YRK': 'York'}


# Initialize
# -------------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser('Timelines by Cohort')
parser.add_argument('-a', '--admit_term', default=None)
parser.add_argument('-i', '--institution', default=None)
args = parser.parse_args()
if args.admit_term is None or args.institution is None:
  sys.exit(f'Missing cohort information: -a admit_term -i institution')
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

# Get Cohort
# -------------------------------------------------------------------------------------------------
conn = PgConnection('cuny_transfers')
cursor = conn.cursor()
cursor.execute(f"""
select admit_term, institution, student_id  from admissions
 where admit_term = {admit_term}
   and institution = '{institution}'
   and event_type = 'ADMT'
group by admit_term, institution, student_id
""")
cohort = [(CohortKey._make([row.admit_term, row.institution, row.student_id]))
          for row in cursor.fetchall()]
print(f'{len(cohort):6,} transfer students admitted to {institutions[institution]} for {semester}.',
      file=sys.stderr)

# Get Timeline Events
# -------------------------------------------------------------------------------------------------
student_events = defaultdict(dict)

# For development purposes, make separate queries for each table rather than a faster but harder to
# debug single query.

# Admission Events
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

# Transfer Evaluations
EvaluationEvent = namedtuple('EvaluationEvent', 'src_institution posted_date')
for cohort_key in cohort:
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

with open(f'./reports/{institution}_{semester.replace(", ", "_")}.csv', 'w') as report:
  print('Student, Term, College, Apply, Admit, Matric, Evals', file=report)
  for cohort_key in cohort:
    print(f'{cohort_key.student_id:08}, {cohort_key.admit_term}, {cohort_key.institution}',
          end='', file=report)
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
    evals = 'None'
    if len(student_events[cohort_key]['evaluations']) > 0:
      evals = []
      for e in student_events[cohort_key]['evaluations']:
        if e.posted_date is not None:
          posted_date = e.posted_date.isoformat()
        else:
          posted_date = '--'
        evals.append(f'{e.src_institution} {posted_date}')
      evals = ' : '.join(evals)
    print(f', {appl_date}, {admt_date}, {matr_date}, {evals}', end='', file=report)
    print(file=report)
