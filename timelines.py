#! /usr/local/bin/python3

import sys
import argparse

from pgconnection import PgConnection

# Initialize
# -------------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser('Timelines by Cohort')
parser.add_argument('-t', '--term', default=None)
parser.add_argument('-d', '--destination', default=None)
args = parser.parse_args()
if args.term is None or args.destination is None:
  sys.exit(f'Missing cohort information: -t term -d destination')
try:
  term = int(args.term)
  year = 1900 + 100 * int(term / 1000) + int(term / 10) % 100
  assert year > 1989 and year < 2026, f'Term year ({year}) must be between 1990 and 2025'
  month = term % 10
  if month != 2 and month != 9:
    print(f'Warning: month ({month}) should be 2 for Spring or 9 for Fall.\n'
          '  Continue anyway? (yN) ',
          end='', file=sys.stderr)
    if not input().lower().startswith('y'):
      exit('Exit')
except ValueError as ve:
  sys.exit(f'“{args.term}” is not a valid CUNY term')
institution = args.destination.strip('01').upper()

# Get Cohort
# -------------------------------------------------------------------------------------------------
conn = PgConnection('cuny_transfers')
cursor = conn.cursor()
cursor.execute(f"""
select student_id, count(*) from admissions
 where admit_term = {term}
   and institution = '{institution}'
group by student_id
""")
cohort = ','.join([f'{row.student_id}' for row in cursor.fetchall])

# Get Timeline Events
# -------------------------------------------------------------------------------------------------
cursor.execute("""
select '-- insert timeline events selector here --'
""")
