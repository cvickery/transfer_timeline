#! /usr/local/bin/python3
""" Event Dates:
      Session: Early Registration, Open Registration, Classes Start
      Admissions: Apply, Admit, Matric
      Registrations: First, Latest
      Transfers: First, Latest

    Algorithm:
      Get session event dates: these are fixed for all members of the cohort
      Get all students in the cohort
      For each student, the articulation term:
        First transfer fetch
        Latest transfer fetch
        First registration
        Latest registration

    Report, as of report date:
      Number of admitted; number matriculated
      Descriptive statistics: mean, std dev, median, mode, range, siqr for the following intervals:
        Apply to Admit
        Admit to Matric
        Matric to First Register
        Matric to Latest Register
        Admit to First Fetch
        Admit to Latest Fetch
        Matric to First Fetch
        Matric to Latest Fetch

    Not looked at, but potentially useful info:
      Students who belong to more than one transfer cohort for a semester. Does transfer fetch
      timing make them go to another college?
"""

import sys
import argparse
import datetime
import statistics

from collections import namedtuple, defaultdict

from pgconnection import PgConnection


class Stats:
  """ mean, median, mode, etc.
  """
  def __init__(self):
    self.n = self.mean = self.std_dev = self.median = self.mode = self.min_val = self.max_val =\
        self.q_1 = self.q_2 = self.q_3 = self.siqr = None


institution_names = {'BAR': 'Baruch', 'BCC': 'Bronx', 'BKL': 'Brooklyn', 'BMC': 'BMCC',
                     'CSI': 'Staten Island', 'CTY': 'City', 'HOS': 'Hostos', 'HTR': 'Hunter',
                     'JJC': 'John Jay', 'KCC': 'Kingsborough', 'LAG': 'LaGuardia', 'LEH': 'Lehman',
                     'MEC': 'Medgar Evers', 'NCC': 'Guttman', 'NYT': 'City Tech',
                     'QCC': 'Queensborough', 'QNS': 'Queens', 'SLU': 'Labor/Urban',
                     'SOJ': 'Journalism', 'SPH': 'Public Health', 'SPS': 'SPS', 'YRK': 'York'}


event_names = {'appl': 'Application',
               'admt': 'Admission',
               'dein': 'Deposit',
               'matr': 'Matriculation',
               'first_fetch': 'First Evaluation',
               'latest_fetch': 'Latest Evaluation',
               'first_enr': 'First Enrollment',
               'latest_enr': 'Latest Enrollment',
               'start_reg': 'Start Registration',
               'open_reg': 'Open Registration',
               'first_cls': 'Classes Start'
               }
event_types = [key for key in event_names.keys()]

EventPair = namedtuple('EventPair', 'earlier later')

# Where the evaluation posted_date was missing, I substituted January 1, 1901
missing_date = datetime.date(1901, 1, 1)


# events_dict()
# -------------------------------------------------------------------------------------------------
def events_dict():
  """ Factory method to produce default dates (None) for a student's events record.
  """
  events = {key: None for key in event_types}
  events['start_reg'] = session.first_registration
  events['open_reg'] = session.open_registration
  events['first_cls'] = session.classes_start
  return events


# Initialize
# -------------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser('Timelines by Cohort')
parser.add_argument('-t', '--admit_term', default=None)  # Or "articulation" term
parser.add_argument('-i', '--institutions', nargs='*')
parser.add_argument('-e', '--event_pairs', nargs='*')
parser.add_argument('-d', '--debug', action='store_true')
args = parser.parse_args()

event_pairs = []
if len(args.event_pairs) < 1:
  print('NOTICE: no event pairs. No statistical reports will be produced.')
for arg in args.event_pairs:
  try:
    earlier, later = arg.lower().split(':')
    if earlier in event_types and later in event_types:
      event_pairs.append(EventPair(earlier, later))
    else:
      raise ValueError('Unrecognized event_pair')
  except ValueError as ve:
    sys.exit(f'“{arg}” does not match earlier:later event_pair structure.\n'
             f'Valid event types are {event_types}')

if args.admit_term is None or args.institutions is None:
  sys.exit(f'Missing cohort information: -t admit_term -i institutions event_pairs')
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
          '  Continue anyway? (yN) ',
          end='', file=sys.stderr)
    if not input().lower().startswith('y'):
      exit('Exit')
    semester = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][month - 1]
  semester = f'{semester} {year}'
except ValueError as ve:
  sys.exit(f'“{args.admit_term}” is not a valid CUNY term')

institutions = [i.strip('01').upper() for i in args.institutions]
if len(institutions) < 1:
  sys.exit('No institutions')
for institution in institutions:
  if institution not in institutions:
    sys.exit(f'“{args.institution}” is not a valid CUNY institution')

conn = PgConnection('cuny_transfers')
cursor = conn.cursor()

# Get session events
# -------------------------------------------------------------------------------------------------
Session = namedtuple('Session', 'institution term session first_registration open_registration '
                     'last_registration classes_start classes_end ')
cursor.execute(f"""
    select * from sessions where institution = '{institution}' and term = {admit_term}
    """)
session = None
for row in cursor.fetchall():
  if session is None or row.session == '1':
    session = Session._make(row)
if session is None:
  sys.exit(f'No session found for {admit_term}')

# Loop through institutions
# =================================================================================================
""" Generate separate reports in Markdown for each institution.
    Generate separate spreadsheets for each measure, with colleges as columns and statistical
    values as the rows. Preserve the order of the colleges from the command line.
"""
# Collect statistic values. Index outer dict by measure (separate sheets), inner dict by institution
stat_values = defaultdict(defaultdict)


# Build datasets for each institution's student cohort
# =================================================================================================
for institution in institutions:
  # Get students and their admission events
  # -----------------------------------------------------------------------------------------------
  Admission = namedtuple('Admission', 'student_id application_number institution '
                         'admit_term requirement_term event_type admit_type action_date '
                         'effective_date ')
  cursor.execute(f"""
      select * from admissions
       where institution = '{institution}'
         and admit_term = '{admit_term}'
         and event_type in ('APPL', 'ADMT', 'DEIN', 'MATR')
      """)
  students = defaultdict(events_dict)
  for row in cursor.fetchall():
    students[int(row.student_id)][row.event_type.lower()] = row.effective_date

  if args.debug:
    print(f'{len(students):,} students in cohort', file=sys.stderr)

  # Transfers Applied dates
  # -----------------------------------------------------------------------------------------------
  cursor.execute(f"""
    select student_id, min(posted_date), max(posted_date)
      from transfers_applied
     where dst_institution ~* '{institution}'
       and articulation_term = {admit_term}
       and model_status = 'Posted'
  group by student_id
    """)
  for row in cursor.fetchall():
    if int(row.student_id) in students.keys():
      students[row.student_id]['first_fetch'] = row.min
      students[row.student_id]['latest_fetch'] = row.max

  # Registration dates
  # -----------------------------------------------------------------------------------------------
  cohort_ids = ','.join([f'{student_id}' for student_id in students.keys()])
  cursor.execute(f"""
    select student_id, first_date, last_date
      from registrations
     where institution ~* '{institution}'
       and term = {admit_term}
       and student_id in ({cohort_ids})
    """)
  for row in cursor.fetchall():
    students[int(row.student_id)]['first_enr'] = row.first_date
    students[int(row.student_id)]['latest_enr'] = row.last_date

  # Create a spreadsheet with the cohort's events for debugging/tableauing
  # -----------------------------------------------------------------------------------------------
  with open(f'./timelines/{institution}-{admit_term}.csv', 'w') as spreadsheet:
    print('Student ID,', ','.join([f'{event_names[name]}' for name in event_names.keys()]),
          file=spreadsheet)
    for student_id in students.keys():
      dates = ','.join([f'{students[student_id][event_date]}' for event_date in event_types])
      print(f'{student_id}, {dates}', file=spreadsheet)

  # For each measure, generate a Markdown report, and collect stats for the measures spreadsheets
  # -----------------------------------------------------------------------------------------------
  for event_pair in event_pairs:
    s = Stats()
    stat_values[event_pair][institution] = s
    earlier, later = event_pair
    with open(f'./reports/{institution}-{admit_term}-{earlier} to {later}.md', 'w') as report:
      print(f'# {institution_names[institution]}: {semester}\n\n'
            f'## Days from {event_names[earlier]} to {event_names[later]}\n'
            f'| Statistic | Value |\n| :--- | :--- |', file=report)
      # Build frequency distributions of earlier and later event date pair differences
      frequencies = defaultdict(int)  # Maybe plot these later
      deltas = []
      for student_id in students.keys():
        if (students[student_id][earlier] is not None
           and students[student_id][earlier] != missing_date
           and students[student_id][later] is not None
           and students[student_id][later] != missing_date):
          delta = students[student_id][later] - students[student_id][earlier]

          deltas.append(delta.days)
          frequencies[delta.days] += 1
      s.n = len(deltas)
      print(f'| N | {s.n}', file=report)
      if len(deltas) > 5:
        s.mean = statistics.fmean(deltas)
        s.std_dev = statistics.stdev(deltas)
        s.median = statistics.median_grouped(deltas)
        s.mode = statistics.mode(deltas)
        s.min_val = min(deltas)
        s.max_val = max(deltas)
        min_max_str = f'{min(deltas)} : {max(deltas)}'
        quartile_list = statistics.quantiles(deltas, n=4, method='exclusive')
        s.q_1 = quartile_list[0]
        s.q_2 = quartile_list[1]
        s.q_3 = quartile_list[2]
        quartile_str = f'{s.q_1:.0f} : {s.q_2:.0f} : {s.q_3:.0f}'
        s.siqr = (s.q_3 - s.q_1) / 2.0
        print(f'| Mean | {s.mean:.0f}', file=report)
        print(f'| Std Dev | {s.std_dev:.1f}', file=report)
        print(f'| Medan | {s.median:.0f}', file=report)
        print(f'| Mode | {s.mode:.0f}', file=report)
        print(f'| Range | {min_max_str}', file=report)
        print(f'| Quartiles | {quartile_str}', file=report)
        print(f'| SIQR | {s.siqr:.1f}', file=report)
      else:
        print('### Not enough data.', file=report)

# Generate each spreadsheet from the saved spreadsheets dict
# ------------------------------------------------------------------------------------------------
for event_pair in event_pairs:
  earlier, later = event_pair
  with open(f'./stat_sheets/{earlier}-to-{later}-{admit_term}.csv', 'w') as stat_sheet:
    col_headings = 'Statistic,' + ', '.join([f'{institution}' for institution in institutions])
    print(f'{col_headings}', file=stat_sheet)

    # Everbody should have an N value
    vals = ','.join([f'{stat_values[event_pair][institution].n}' for institution in institutions])
    print(f'N, {vals}', file=stat_sheet)

    # The remainder is messy because there will be None values where N < 6 for some institution, and
    # because different statistics have different formatting rules

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].mean
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.0f}')
    vals = ','.join(vals)
    print(f'Mean, {vals}', file=stat_sheet)

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].std_dev
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.1f}')
    vals = ','.join(vals)
    print(f'Std Dev, {vals}', file=stat_sheet)

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].median
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.0f}')
    vals = ','.join(vals)
    print(f'Median, {vals}', file=stat_sheet)

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].mode
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.0f}')
    vals = ','.join(vals)
    print(f'Mode, {vals}', file=stat_sheet)

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].min_val
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.0f}')
    vals = ','.join(vals)
    print(f'Min, {vals}', file=stat_sheet)

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].max_val
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.0f}')
    vals = ','.join(vals)
    print(f'Max, {vals}', file=stat_sheet)

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].q_1
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.0f}')
    vals = ','.join(vals)
    print(f'Q1, {vals}', file=stat_sheet)

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].q_2
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.0f}')
    vals = ','.join(vals)
    print(f'Q2, {vals}', file=stat_sheet)

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].q_3
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.0f}')
    vals = ','.join(vals)
    print(f'Q3, {vals}', file=stat_sheet)

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].siqr
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.1f}')
    vals = ','.join(vals)
    print(f'SIQR, {vals}', file=stat_sheet)

exit()
