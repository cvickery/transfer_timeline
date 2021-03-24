#! /usr/local/bin/python3
""" Replace rows in transferred_courses where the posted_date is newer; archive replaced rows.
    Add new rows.
"""

import csv
import datetime
import sys
import argparse

from collections import namedtuple, defaultdict
from pathlib import Path
from pgconnection import PgConnection

parser = argparse.ArgumentParser('Update Transfers')
parser.add_argument('-np', '--no_progress', action='store_true')
parser.add_argument('file', nargs='?')
args = parser.parse_args()
progress = not args.no_progress

curric_conn = PgConnection()
curric_cursor = curric_conn.cursor()
trans_conn = PgConnection('cuny_transfers')
trans_cursor = trans_conn.cursor()

# If a file was specified on the command line, use that. Otherwise use the latest one found in
# downloads. The idea is to allow history from previous snapshots to be captured during development,
# then to use the latest snapshot on a daily basis.
the_file = args.file
if the_file is None:
  # No snapshot specified; use latest available.
  files = Path('./downloads').glob('CV*ALL*')
  for file in files:
    if the_file is None or file.stat().st_mtime > the_file.stat().st_mtime:
      the_file = file
else:
  the_file = Path(the_file)

if the_file is None:
  sys.exit('No input file.')

# Using the date the file was transferred to Tumbleweed as a proxy for CF SYSDATE
file_date = datetime.datetime.fromtimestamp(the_file.stat().st_mtime)
file_name = the_file.name

print('Using:', the_file,
      file_date.strftime('%B %d, %Y'),
      file=sys.stderr)
iso_file_date = file_date.strftime('%Y-%m-%d')
debug = open(f'./debugs/{iso_file_date}', 'w')
print(file_name, file=debug)

# Record types, indexed by dst_institution
num_new = defaultdict(int)      # never before seen
num_alt = defaultdict(int)      # real changes
num_old = defaultdict(int)      # posted date <= max already in db
num_already = defaultdict(int)  # new data, but duplicates existing

# Multiple existing records and missing courses, indexed by src_institution
num_mult = defaultdict(int)
num_miss = defaultdict(int)

# Miscellaneous anomalies, indexed by src_institution, dst_institution
num_debug = defaultdict(int)

# Data for the update_history table
last_post = None
num_added = 0
num_changed = 0
num_missing = 0
num_skipped = 0

# Cache of repeatable courses found
repeatable = dict()

# Anything posted before the latest in our DB is ignored
max_post_added = None
trans_cursor.execute('select max(last_post) from update_history')
max_posted_date = trans_cursor.fetchone().max
if max_posted_date is None:
  # Nothing in the update history yet, get the max posted_date from the transfers_applied file
  # trans_cursor.execute('select max(posted_date) from transfers_applied')
  # max_posted_date = trans_cursor.fetchone().max
  sys.exit('No last posted date in history table')
max_posted_date_str = max_posted_date.strftime('%Y-%m-%d')

print(f"Using only transactions posted after {max_posted_date_str}.")

# Progress indicators
m = 0
num_records = len(open(the_file, encoding='ascii', errors='backslashreplace').readlines()) - 1

with open(the_file, encoding='ascii', errors='backslashreplace') as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    if reader.line_num == 1:
      headers = [h.lower().replace(' ', '_') for h in line]
      cols = [h for h in headers]

      values_added = None
      if 'comment' not in cols:
        # Columns not present in queries prior to 2021-03-18
        cols += ['user_id', 'reject_reason', 'transfer_overridden', 'override_reason', 'comment']
        values_added = ('00000000', '', False, '', '')
      else:
        cols.remove('sysdate')

      cols.insert(1 + cols.index('src_offer_nbr'), 'src_repeatable')
      # print('cols array', len(cols), cols)
      placeholders = ((len(cols)) * '%s,').strip(', ')
      # print('placeholders', placeholders.count('s'), placeholders)
      cols = ', '.join([c for c in cols])
      # print('cols string', cols.count(',') + 1, cols)
      Row = namedtuple('Row', headers)
    else:
      if reader.line_num == 2 and values_added is None:
        # SYSDATE is available: substitute it for file_date
        mo, da, yr = [int(x) for x in line[-1].split('/')]
        file_date = datetime.datetime(yr, mo, da)

      m += 1
      if progress:
        print(f'  {m:06,}/{num_records:06,}\r', end='', file=sys.stderr)
      row = Row._make(line)

      if '/' in row.posted_date:
        mo, da, yr = row.posted_date.split('/')
        posted_date = datetime.date(int(yr), int(mo), int(da))
      else:
        posted_date = None

      # Ignore old records
      if posted_date and posted_date <= max_posted_date:
        num_old[row.dst_institution] += 1
        num_skipped += 1
        posted_date_str = posted_date.strftime('%Y-%m-%d')
        print(f'Line {m:6}: posted {posted_date_str} <= max_posted {max_posted_date_str}',
              file=debug)
        continue

      # yr = 1900 + 100 * int(row.enrollment_term[0]) + int(row.enrollment_term[1:3])
      # mo = int(row.enrollment_term[-1])
      # da = 1
      # enrollment_term = datetime.date(yr, mo, da)

      # yr = 1900 + 100 * int(row.articulation_term[0]) + int(row.articulation_term[1:3])
      # mo = int(row.articulation_term[-1])
      # articulation_term = datetime.date(yr, mo, da)

      src_course_id = int(row.src_course_id)
      src_offer_nbr = int(row.src_offer_nbr)
      src_catalog_nbr = row.src_catalog_nbr.strip()
      dst_course_id = int(row.dst_course_id)
      dst_offer_nbr = int(row.dst_offer_nbr)
      dst_catalog_nbr = row.dst_catalog_nbr.strip()

      """ Categorize the row
            {student_id, src_institution, src_course_id, src_offer_nbr, dst_institution} is new:
              Increment num_new
              Add the record to transfers_applied
            Record exists with no change
              Increment num_already
              Ignore
            The destination course has changed
              Increment num_alt
              Add current row from transfers_applied to transfer_applied_history
              Add the new record to transfers_applied
      """

      # Look up existing transfers_applied record
      trans_cursor.execute(f"""
select * from transfers_applied
 where student_id = {row.student_id}
   and src_course_id = {src_course_id}
   and src_offer_nbr = {src_offer_nbr}
   and dst_institution = '{row.dst_institution}'
""")
      if trans_cursor.rowcount == 0:
        # Not in transfers_applied yet: add new record
        # --------------------------------------------
        # Determine whether the src course is repeatable or not
        try:
          src_repeatable = repeatable[(src_course_id, src_offer_nbr)]
        except KeyError:
          curric_cursor.execute(f'select repeatable from cuny_courses where course_id = '
                                f'{src_course_id} and offer_nbr = {src_offer_nbr}')
          if curric_cursor.rowcount != 1:
            trans_cursor.execute(f"insert into missing_courses values({src_course_id}, "
                                 f"{src_offer_nbr}, '{row.src_institution}', '{row.src_subject}', "
                                 f"'{row.src_catalog_nbr}') on conflict do nothing")
            src_repeatable = None
          else:
            src_repeatable = curric_cursor.fetchone().repeatable
          repeatable[(int(row.src_course_id), (row.src_offer_nbr))] = src_repeatable
        # print('values tuple', len(values_tuple), values_tuple)
        values_tuple = (row.student_id, row.src_institution, row.transfer_model_nbr,
                        row.enrollment_term, row.enrollment_session, row.articulation_term,
                        row.model_status, posted_date, row.src_subject, src_catalog_nbr,
                        row.src_designation, row.src_grade, row.src_gpa, row.src_course_id,
                        row.src_offer_nbr, src_repeatable, row.src_description,
                        row.academic_program, row.units_taken, row.dst_institution,
                        row.dst_designation, row.dst_course_id, row.dst_offer_nbr, row.dst_subject,
                        dst_catalog_nbr, row.dst_grade, row.dst_gpa)
        if values_added is None:
          values_tuple += (row.user_id, row.reject_reason, row.transfer_overridden == 'Y',
                           row.override_reason, row.comment)
        else:
          values_tuple += values_added
        # print(values_tuple, file=debug)
        try:
          trans_cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders}) ',
                               values_tuple)
        except IndexError as ie:
          print('New situation', file=debug)
          print(cols.count(',') + 1, cols, file=debug)
          print(placeholders.count('s'), placeholders, file=debug)
          print(len(values_tuple), values_tuple, file=debug)
          exit()
        num_new[row.dst_institution] += 1
        num_added += 1

      elif trans_cursor.rowcount == 1:
        # One matching row already exists in transfers_applied
        # ----------------------------------------------------
        for record in trans_cursor.fetchall():
          # ... be sure this posted date is newer
          if record.posted_date and (record.posted_date >= posted_date):
            # Existing record is not older: skip this one, and add to debug file for verification
            num_debug[(row.src_institution, row.dst_institution)] += 1
            print(f'*** CF query {the_file}: posted dated is not newer than existing '
                  f'tranfers_applied posted_date\n',
                  f'CF row: {line}\nDB record: {[v for v in record]}\n', file=debug)
            num_already[row.dst_institution] += 1
            num_skipped += 1
            continue

          #   has destination course changed?
          if int(record.dst_course_id) == dst_course_id \
             and int(record.dst_offer_nbr) == dst_offer_nbr:
             # Most common case: nothing more to do
             num_already[row.dst_institution] += 1
             num_skipped += 1
          else:
            # Different destination course: Write the new record to the transfers_changed table
            #   Determine whether the src course is repeatable or not
            try:
              src_repeatable = repeatable[(src_course_id, src_offer_nbr)]
            except KeyError:
              curric_cursor.execute(f'select repeatable from cuny_courses where course_id = '
                                    f'{src_course_id} and offer_nbr = {src_offer_nbr}')
              if curric_cursor.rowcount != 1:
                num_miss[row.src_instituion] += 1
                num_missing += 1
                trans_cursor.execute(f"insert into missing_courses values({src_course_id}, "
                                     f"{src_offer_nbr}, '{row.src_institution}', "
                                     f"'{row.src_subject}', '{row.src_catalog_nbr}') "
                                     f"on conflict do nothing")
                src_repeatable = None
              else:
                src_repeatable = curric_cursor.fetchone().repeatable
                repeatable[(int(row.src_course_id), (row.src_offer_nbr))] = src_repeatable
              # print('values tuple', len(values_tuple), values_tuple)
            values_tuple = (row.student_id, row.src_institution, row.transfer_model_nbr,
                            row.enrollment_term, row.enrollment_session, row.articulation_term,
                            row.model_status, posted_date, row.src_subject, src_catalog_nbr,
                            row.src_designation, row.src_grade, row.src_gpa, row.src_course_id,
                            row.src_offer_nbr, src_repeatable, row.src_description,
                            row.academic_program, row.units_taken, row.dst_institution,
                            row.dst_designation, row.dst_course_id, row.dst_offer_nbr,
                            row.dst_subject, dst_catalog_nbr, row.dst_grade, row.dst_gpa)
            if values_added is None:
              values_tuple += (row.user_id, row.reject_reason,
                               row.transfer_overridden == 'Y', row.override_reason,
                               row.comment)
            else:
              values_tuple += values_added
            # print(values_tuple, file=debug)
            try:
              trans_cursor.execute(f'insert into transfers_changed ({cols}) values ({placeholders}) ',
                                   values_tuple)
            except IndexError as ie:
              print('Altered situation', file=debug)
              print(cols.count(',') + 1, cols, file=debug)
              print(placeholders.count('s'), placeholders, file=debug)
              print(len(values_tuple), values_tuple, file=debug)
              exit()
            if (max_post_added is None) or (posted_date > max_post_added):
              max_post_added = posted_date
            num_alt[row.dst_institution] += 1
            num_changed += 1


if max_post_added is None:
  max_post_added = 'NULL'
else:
  max_post_added = f"'{max_post_added}'"

trans_cursor.execute(f"""
insert into update_history values(
DEFAULT, '{file_name}', '{file_date}', {max_post_added},
          {num_records}, {num_added}, {num_changed}, {num_skipped}, {num_missing})
""")

trans_conn.commit()
trans_conn.close()

with open('reports/' + iso_file_date, 'w') as report:
  for key in sorted(num_old.keys()):
    print(f'{iso_file_date}       old {key[0:3]} {num_old[key]:7,}', file=report)

  for key in sorted(num_already.keys()):
    print(f'{iso_file_date} unchanged {key[0:3]} {num_already[key]:7,}', file=report)

  for key in sorted(num_new.keys()):
    print(f'{iso_file_date}       new {key[0:3]} {num_new[key]:7,}', file=report)

  for key in sorted(num_alt.keys()):
    print(f'{iso_file_date}   altered {key[0:3]} {num_alt[key]:7,}', file=report)

  for key in sorted(num_mult.keys()):
    print(f'{iso_file_date}  multiple {key[0:3]} {num_mult[key]:7,}', file=report)

  for key in sorted(num_miss.keys()):
    print(f'{iso_file_date}   missing {key[0:3]} {num_miss[key]:7,}', file=report)

  for key in sorted(num_debug.keys()):
    key_str = f'{key[0][0:3]}:{key[1][0:3]}'
    print(f'{iso_file_date} *** debug {key_str} {num_debug[key]:7,}', file=report)
