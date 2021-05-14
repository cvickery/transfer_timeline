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
from openpyxl import Workbook

from pgconnection import PgConnection


class Term:
  """ CF term code and semester name
  """
  def __init__(self, term_code, semester_name):
    self.term = term_code
    self.name = semester_name

  def __repr__(self):
    return self.name


# Statistics
# -------------------------------------------------------------------------------------------------
""" Descriptive statistics for a cohort's measures
      stat_values[institution][admit_term][measure].n = 12345, etc
"""


def institution_factory():
  return defaultdict(term_factory)


def term_factory():
  return defaultdict(stat_factory)


def stat_factory():
  return Stats()


class Stats:
  """ mean, median, mode, etc.
  """
  def __init__(self):
    self.n = self.mean = self.std_dev = self.median = self.mode = self.min_val = self.max_val =\
        self.q_1 = self.q_2 = self.q_3 = self.siqr = None


stat_values = defaultdict(institution_factory)

# App Parameters
# -------------------------------------------------------------------------------------------------
institution_names = {'BAR': 'Baruch', 'BCC': 'Bronx', 'BKL': 'Brooklyn', 'BMC': 'BMCC',
                     'CSI': 'Staten Island', 'CTY': 'City', 'HOS': 'Hostos', 'HTR': 'Hunter',
                     'JJC': 'John Jay', 'KCC': 'Kingsborough', 'LAG': 'LaGuardia', 'LEH': 'Lehman',
                     'MEC': 'Medgar Evers', 'NCC': 'Guttman', 'NYT': 'City Tech',
                     'QCC': 'Queensborough', 'QNS': 'Queens', 'SLU': 'Labor/Urban',
                     'SOJ': 'Journalism', 'SPH': 'Public Health', 'SPS': 'SPS', 'YRK': 'York'}


event_names = {'appl': 'Apply',
               'admt': 'Admit',
               'dein': 'Commit',
               'matr': 'Matric',
               'first_fetch': 'First Eval',
               'latest_fetch': 'Latest Eval',
               'start_reg': 'Start Registration',
               'first_cls': 'Start Classes',
               'first_enr': 'First Registered',
               'latest_enr': 'Latest Registered',
               'wadm': 'Admin',
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
  # Session info is same for all students in cohort
  events['start_reg'] = session.first_registration
  events['first_cls'] = session.classes_start
  events['wadm'] = []   # List of action/reason events with their dates
  return events


# Validate Command Line
# -------------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser('Timelines by Cohort')
parser.add_argument('-t', '--admit_terms', type=int, nargs='*', default=[])
parser.add_argument('-i', '--institutions', nargs='*', default=[])
parser.add_argument('-e', '--event_pairs', nargs='*', default=[])
parser.add_argument('-d', '--debug', action='store_true')
args = parser.parse_args()

event_pairs = []
event_type_list = '\n  '.join([t for t in event_types if t != 'wadm'])
if len(args.event_pairs) < 1:
  print('NOTICE: no event pairs. No statistical reports will be produced.', file=sys.stderr)
for arg in args.event_pairs:
  try:
    earlier, later = arg.lower().split(':')
    if earlier in event_types and later in event_types:
      event_pairs.append(EventPair(earlier, later))
    else:
      raise ValueError('Unrecognized event_pair')
  except ValueError as ve:
    sys.exit(f'“{arg}” does not match earlier:later event_pair structure.\n'
             f'Valid event types are:\n  {event_type_list}')

if len(args.admit_terms) < 1 or len(args.institutions) < 1:
  sys.exit(f'Usage: -t admit_term... -i institution... -e event_pair...')

try:
  admit_terms = []
  for admit_term in args.admit_terms:
    year = 1900 + 100 * int(admit_term / 1000) + int(admit_term / 10) % 100
    assert year > 1989 and year < 2026, f'Admit Term year ({year}) must be between 1990 and 2025'
    month = admit_term % 10
    if month == 2:
      semester = 'Spring'
    elif month == 6:
      semester = 'Summer'
    elif month == 9:
      semester = 'Fall'
    else:
      print(f'{admit_term}: month ({month}) should be 2 for Spring, 6 for Summer, or 9 for Fall.\n'
            '  Continue anyway? (yN) ',
            end='', file=sys.stderr)
      if not input().lower().startswith('y'):
        exit('Exit')
      semester = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][month - 1]
    semester = f'{semester} {year}'
    admit_terms.append(Term(admit_term, semester))

except ValueError as ve:
  sys.exit(f'“{args.admit_term}” is not a valid CUNY term')

institutions = [i.strip('01').upper() for i in args.institutions]
for institution in institutions:
  if institution not in institution_names.keys():
    sys.exit(f'“{institution}” is not a valid CUNY institution')

conn = PgConnection('cuny_transfers')
cursor = conn.cursor()

# Initialize Data Structures
# =================================================================================================
""" A cohort is a set of (students, institution, admit_term). Collect all 12 event dates for each
    cohort, then report each measure for each cohort.
"""
""" Generate separate reports in Markdown for each institution.
    Generate separate spreadsheets for each measure, with colleges as columns and statistical
    values as the rows. Preserve the order of the colleges from the command line.
"""
# Cohorts key is (institution, term), value is a dict with student_id as key, and dict of event
# dates as their values.
#   cohorts[(QNS, 1212)] = {12345678: {appl: 2020-10-10, admt: 2020-10-20, wadm: ...},
#                           87654321: {appl: 2020-11-11, admt: 2020-11-22, wadm: ...},
#                           ...}
cohorts = dict()
for institution in institutions:
  for admit_term in sorted(admit_terms, key=lambda x: x.term):
    cohort_key = (institution, admit_term.term)
    student_ids = set()
    cohorts[cohort_key] = defaultdict(events_dict)

    # Get session events, which are the same for all students in the cohort
    # ---------------------------------------------------------------------------------------------
    Session = namedtuple('Session', 'institution term session first_registration open_registration '
                         'last_registration classes_start classes_end ')
    cursor.execute(f"""
        select * from sessions where institution = '{institution}' and term = {admit_term.term}
        """)
    session = None
    for row in cursor.fetchall():
      if session is None or row.session == '1':
        session = Session._make(row)
    if session is None:
      print(f'No session found for {institution_names[institution]} {admit_term}')
      continue

    # Add the students and their admission events to the cohort
    # ---------------------------------------------------------------------------------------------
    cursor.execute(f"""
        select student_id, program_action, action_reason, effective_date from admissions
         where institution = '{institution}'
           and admit_term = {admit_term.term}
           and program_action in ('APPL', 'ADMT', 'DEIN', 'MATR', 'WADM')
        """)
    for row in cursor.fetchall():
      student_ids.add(int(row.student_id))
      event_type = row.program_action.lower()
      if event_type == 'wadm':
        if row.action_reason == '':
          event_name = 'WADM'
        else:
          event_name = row.action_reason
        cohorts[cohort_key][int(row.student_id)][event_type].append(f'{row.effective_date} '
                                                                    f'{event_name}')
      else:
        cohorts[cohort_key][int(row.student_id)][event_type] = row.effective_date
    print(f'{len(cohorts[cohort_key]):,} students in {cohort_key} cohort.', file=sys.stderr)
    assert len(student_ids) == len(cohorts[cohort_key])
    student_id_list = ','.join(f'{id}' for id in student_ids)

    # Transfers-Applied (credits evaluated) dates
    # ---------------------------------------------------------------------------------------------
    cursor.execute(f"""
      select student_id, min(posted_date), max(posted_date)
        from transfers_applied
       where dst_institution ~* '{institution}'
         and articulation_term = {admit_term.term}
         and student_id in ({student_id_list})
    group by student_id
      """)
    for row in cursor.fetchall():
      student_id = int(row.student_id)
      cohorts[cohort_key][student_id]['first_fetch'] = row.min
      cohorts[cohort_key][student_id]['latest_fetch'] = row.max

    # Enrollment dates
    # ---------------------------------------------------------------------------------------------
    cursor.execute(f"""
      select student_id, first_date, last_date
        from registrations
       where institution ~* '{institution}'
         and term = {admit_term.term}
         and student_id in ({student_id_list})
      """)
    for row in cursor.fetchall():
      cohorts[cohort_key][row.student_id]['first_enr'] = row.first_date
      cohorts[cohort_key][row.student_id]['latest_enr'] = row.last_date

    # Create a spreadsheet with the cohort's events for debugging/tableauing
    # ---------------------------------------------------------------------------------------------
    with open(f'./timelines/{institution}-{admit_term.term}.csv', 'w') as spreadsheet:
      print('Student ID,', ','.join([f'{event_names[name]}' for name in event_names.keys()]),
            file=spreadsheet)
      for student_id in sorted(student_ids):
        dates = ','.join([f'{cohorts[cohort_key][student_id][event]}' for event in event_types
                         if event != 'wadm'])
        if len(cohorts[cohort_key][student_id]['wadm']) == 0:
          dates += ',None'
        else:
          dates += ',' + '; '.join(sorted(cohorts[cohort_key][student_id]['wadm']))
        print(f'{student_id}, {dates}', file=spreadsheet)

    # For each measure, generate a Markdown report, and collect stats for the measures spreadsheets
    # ---------------------------------------------------------------------------------------------
    for event_pair in event_pairs:
      s = stat_values[institution][admit_term.term][event_pair]
      earlier, later = event_pair
      with open(f'./reports/{institution}-{admit_term}-{earlier} to {later}.md', 'w') as report:
        print(f'# {institution_names[institution]}: {admit_term}\n\n'
              f'## Days from {event_names[earlier]} to {event_names[later]}\n'
              f'| Statistic | Value |\n| :--- | :--- |', file=report)
        # Build frequency distributions of earlier and later event date pair differences
        frequencies = defaultdict(int)  # Maybe plot these later
        deltas = []
        for student_id in cohorts[cohort_key].keys():
          if (cohorts[cohort_key][student_id][earlier] is not None
             and cohorts[cohort_key][student_id][earlier] != missing_date
             and cohorts[cohort_key][student_id][later] is not None
             and cohorts[cohort_key][student_id][later] != missing_date):
            delta = (cohorts[cohort_key][student_id][later]
                     - cohorts[cohort_key][student_id][earlier])

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
          print(f'| Medan | {s.median:.0f}', file=report)
          print(f'| Mean | {s.mean:.0f}', file=report)
          print(f'| Mode | {s.mode:.0f}', file=report)
          print(f'| Range | {min_max_str}', file=report)
          print(f'| Quartiles | {quartile_str}', file=report)
          print(f'| SIQR | {s.siqr:.1f}', file=report)
          print(f'| Std Dev | {s.std_dev:.1f}', file=report)
        else:
          print('### Not enough data.', file=report)
exit()
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
      val = stat_values[event_pair][institution].median
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.0f}')
    vals = ','.join(vals)
    print(f'Median, {vals}', file=stat_sheet)

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

    vals = []
    for institution in institutions:
      val = stat_values[event_pair][institution].std_dev
      if val is None:
        vals.append('')
      else:
        vals.append(f'{val:.1f}')
    vals = ','.join(vals)
    print(f'Std Dev, {vals}', file=stat_sheet)

exit()
