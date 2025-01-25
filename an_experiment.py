#! /usr/local/bin/python3

""" Experiment: can one course can transfer as multiple destination courses?
    Conclusion: yes, and there are two cases where QCC receives two+ bkcr courses for one sending
    course, but that's not covered by transfer_rules, so they must be overrides. (Pathways RDs)

    After lunch, look at sending course credits, receiving course credits, and units_taken.
    Or not.

    Time to rethink transfers_applied schema. It needs to serve two purposes: to give posted_date
    ranges for students and frequency of transfer_rules applied, in particular bkcr ones.
"""

import argparse
import csv
import datetime
import sys

from collections import namedtuple, defaultdict
from pathlib import Path
from pgconnection import PgConnection

conn = PgConnection('cuny_transfers')
cursor = conn.cursor()

blankets_cache = dict()


# lookup()
# -------------------------------------------------------------------------------------------------
def lookup(send_id: int, recv_ids: list):
  """ Look up info about a sending course and the set of receiving courses.
  """
  recv_id_str = ','.join(f'{id}' for id in recv_ids)
  cursor.execute(f"""
    select substr(institution, 0, 4) as snd,
    lpad(course_id::text, 6, '0') || ':' || offer_nbr || ' ' || discipline || '-' || catalog_number
    as course, designation in ('MLA', 'MNL') as msg, attributes ~ 'BKCR' as bkt
    from cuny_courses
    where course_id = {send_id}
    """)
  r = cursor.fetchone()
  return_str = f'{r.snd} {r.course} {r.msg} {r.bkt} => '
  cursor.execute(f"""
    select substr(institution, 0, 4) as rcv,
    lpad(course_id::text, 6, '0') || ':' || offer_nbr || ' ' || discipline || '-' || catalog_number
    as course, designation in ('MLA', 'MNL') as msg, attributes ~ 'BKCR' as bkt
    from cuny_courses
    where course_id in ({recv_id_str})
    """)
  return_str += ' and '.join([f'{r.rcv} {r.course} {r.msg} {r.bkt}' for r in cursor.fetchall()])
  return return_str


# is_bkcr()
# -------------------------------------------------------------------------------------------------
def is_bkcr(course_id: int, offer_nbr: int) -> bool:
  """ Is a course blanket credit?
  """
  if (course_id, offer_nbr) not in blankets_cache.keys():
    cursor.execute(f"""
    select attributes ~ 'BKCR' as is_blanket
    from cuny_courses
    where course_id = {course_id}
    and offer_nbr = {offer_nbr}
    """)
    if cursor.rowcount != 1:
      print(f'blanket lookup returned {cursor.rowcount} rows for {course_id:06}:{offer_nbr}',
            file=sys.stderr)
      value = False
    else:
      value = cursor.fetchone().is_blanket
    blankets_cache[(course_id, offer_nbr)] = value
  return blankets_cache[(course_id, offer_nbr)]


# __main__
# -------------------------------------------------------------------------------------------------
Course = namedtuple('Course', 'course_id offer_nbr')
Course_Info = namedtuple('Course_info', 'blanket_units_taken other_units_taken')

course_infos = defaultdict(namedtuple)
parser = argparse.ArgumentParser('Update Transfers')
parser.add_argument('-np', '--no_progress', action='store_true')
parser.add_argument('file', nargs='?')
args = parser.parse_args()
progress = not args.no_progress

csv_file = args.file

if csv_file is None:
  # No snapshot specified; use latest available.
  files = Path('./downloads').glob('CV*ALL*')
  for file in files:
    if csv_file is None or file.stat().st_mtime > csv_file.stat().st_mtime:
      csv_file = file
else:
  csv_file = Path(csv_file)

if csv_file is None:
  sys.exit('No input file.')

print(f'Using {csv_file}', file=sys.stderr)
file_date = datetime.date.fromtimestamp(csv_file.stat().st_mtime)

Send_Key = namedtuple('Send_Key',
                      'posted_date src_institution, dst_institution, src_course_id, src_offer_nbr')
Dest_Tuple = namedtuple('Dest_Tuple', 'dst_course_id, dst_offer_nbr')
courses = defaultdict(set)

with open(csv_file) as infile:
  reader = csv.reader(infile)
  for line in reader:
    if reader.line_num == 1:
      headers = [h.lower().replace(' ', '_') for h in line]
      cols = [h for h in headers]

      # Adjust for columns not present in queries prior to 2021-03-18
      if 'comment' not in cols:
        cols += ['user_id', 'reject_reason', 'transfer_overridden', 'override_reason', 'comment']
        values_added = ('00000000', '', False, '', '')
      else:
        cols.remove('sysdate')
        values_added = None

      # columns {src_is_repeatable, dst_is_message, dst_is_blanket} from CF catalog (cached above).
      cols.insert(1 + cols.index('src_offer_nbr'), 'src_is_repeatable')
      cols.insert(1 + cols.index('dst_gpa'), 'dst_is_message')
      cols.insert(1 + cols.index('dst_is_message'), 'dst_is_blanket')

      placeholders = ((len(cols)) * '%s,').strip(', ')
      cols = ', '.join([c for c in cols])
      Row = namedtuple('Row', headers)
    else:
      # If SYSDATE is available, substitute it for file_date
      if reader.line_num == 2 and values_added is None:
        mo, da, yr = [int(x) for x in line[-1].split('/')]
        file_date = datetime.date(yr, mo, da)
      row = Row._make(line)

      # Collect cases where one src course transfers as two different dst courses for the same
      # student at the same time. Key is source course, destination institution, and posted date;
      # value is set of destination courses. Print keys and values where destination set size > 1,
      # looking up blanket creditness of destination courses.
      key = Send_Key(row.posted_date, row.src_institution, row.dst_institution,
                     int(row.src_course_id), int(row.src_offer_nbr))
      courses[key].add(Dest_Tuple(int(row.dst_course_id),
                                  int(row.dst_offer_nbr)))
print(len(courses), 'courses', file=sys.stderr)
for key in sorted(courses.keys()):
  if len(courses[key]) > 1:
    key_str = f'{key.src_institution} {key.src_course_id:06}:{key.src_offer_nbr}'
    dest_str = '; '.join([f'{d.dst_course_id:06}:{d.dst_offer_nbr}, '
                          f'{is_bkcr(d.dst_course_id, d.dst_offer_nbr)}'
                         for d in sorted(courses[key])])
    print(lookup(key.src_course_id, [d.dst_course_id for d in courses[key]]))
exit()


# List of all source courses that transfer as blanket credit, then see if any of them transfer as
# a non-blanket course too.

cursor.execute("""
    select src_course_id, src_offer_nbr
      from transfers_applied
     where src_is_repeatable = 'N'
     and dst_is_blanket
     group by src_course_id, src_offer_nbr
    """)
print(cursor.rowcount, 'rows')

blanket_ids = [f'{c.src_course_id:06}' for c in cursor.fetchall()]
blanket_ids_str = ','.join(blanket_ids)
cursor.execute(f"""
    select src_course_id, src_offer_nbr
    from transfers_applied
    where src_course_id in ({blanket_ids_str})
      and not dst_is_blanket
      group by src_course_id, src_offer_nbr
    """)
print(cursor.rowcount, 'others')
for row in cursor.fetchall():
  print(row, file=sys.stderr)
