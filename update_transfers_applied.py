#! /usr/local/bin/python3
""" Replace rows in transferred_courses where the posted_date is newer; archive replaced rows.
    Add new rows.
"""

import csv
import datetime
import sys
import argparse
from collections import namedtuple, defaultdict
import datetime
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
  files = Path('./downloads').glob('CV_QNS*')
  for file in files:
    if the_file is None or file.stat().st_mtime > the_file.stat().st_mtime:
      the_file = file
if the_file is None:
  sys.exit('No input file.')

# Using the date the file was transferred to Tumbleweed as a proxy for CF SYSDATE
file_date = datetime.datetime.fromtimestamp(the_file.stat().st_mtime)
print('Using:', the_file,
      file_date.strftime('%B %d, %Y'),
      file=sys.stderr)
iso_file_date = file_date.strftime('%Y-%m-%d')
debug = open(f'./debugs/{iso_file_date}', 'w')

# Changes, indexed by dst_institution
num_new = defaultdict(int)      # never before seen
num_alt = defaultdict(int)      # real changes
num_old = defaultdict(int)      # posted date <= max already in db
num_already = defaultdict(int)  # new data, but duplicates existing

# Multiple existing records and missing courses, indexed by src_institution
num_mult = defaultdict(int)
num_miss = defaultdict(int)

# Miscellaneous anomalies, indexed by src_institution, dst_institution
num_debug = defaultdict(int)

# Cache of repeatable courses found
repeatable = dict()

# Anything posted before the latest in our DB is ignored
max_post_added = None
trans_cursor.execute('select max(last_post) from update_history')
max_posted_date = trans_cursor.fetchone().max
if max_posted_date is None:
  # Nothing in the update history yet, get the max posted_date from the transfers_applied file
  trans_cursor.execute('select max(posted_date) from transfers_applied')
  max_posted_date = trans_cursor.fetchone().max

print(f"Using only transactions posted after {max_posted_date.strftime('%B %d, %Y')}.")

# Progress indicators
m = 0
n = len(open(the_file).readlines()) - 1

with open(the_file) as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    if reader.line_num == 1:
      headers = [h.lower().replace(' ', '_') for h in line]
      cols = [h for h in headers]
      cols.insert(1 + cols.index('src_offer_nbr'), 'src_repeatable')
      # print('cols array', len(cols), cols)
      placeholders = ((len(cols)) * '%s,').strip(', ')
      # print('placeholders', placeholders.count('s'), placeholders)
      cols = ', '.join([c for c in cols])
      # print('cols string', cols.count(',') + 1, cols)
      Row = namedtuple('Row', headers)
    else:
      m += 1
      if progress:
        print(f'  {m:06,}/{n:06,}\r', end='', file=sys.stderr)
      row = Row._make(line)

      if '/' in row.posted_date:
        mo, da, yr = row.posted_date.split('/')
        posted_date = datetime.date(int(yr), int(mo), int(da))
      else:
        posted_date = None

      # Ignore old records
      if posted_date and posted_date <= max_posted_date:
        num_old[row.dst_institution] += 1
        continue

      yr = 1900 + 100 * int(row.enrollment_term[0]) + int(row.enrollment_term[1:3])
      mo = int(row.enrollment_term[-1])
      da = 1
      enrollment_term = datetime.date(yr, mo, da)

      yr = 1900 + 100 * int(row.articulation_term[0]) + int(row.articulation_term[1:3])
      mo = int(row.articulation_term[-1])
      articulation_term = datetime.date(yr, mo, da)

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
              Increment num_old
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
        num_new[row.dst_institution] += 1
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
                        enrollment_term, row.enrollment_session, articulation_term,
                        row.model_status, posted_date, row.src_subject, src_catalog_nbr,
                        row.src_designation, row.src_grade, row.src_gpa, row.src_course_id,
                        row.src_offer_nbr, src_repeatable, row.src_description,
                        row.academic_program, row.units_taken, row.dst_institution,
                        row.dst_designation, row.dst_course_id, row.dst_offer_nbr, row.dst_subject,
                        dst_catalog_nbr, row.dst_grade, row.dst_gpa)
        trans_cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders}) ',
                             values_tuple)

      elif trans_cursor.rowcount == 1:
        # One matching row already exists in transfers_applied
        # ----------------------------------------------------
        record = trans_cursor.fetchone()
        # ... be sure this posted date is newer
        if record.posted_date and (record.posted_date >= posted_date):
          # Existing record is not older: ignore it, and add to debug file for verification
          num_debug[(row.src_institution, row.dst_institution)] += 1
          print(f'*** CF query {the_file}: posted dated is not newer than existing '
                f'tranfers_applied posted_date\n',
                f'CF row: {line}\nDB record: {record}\n', file=debug)
          continue

        #   has destination course changed?
        if int(record.dst_course_id) == dst_course_id \
           and int(record.dst_offer_nbr) == dst_offer_nbr:
           num_already[row.dst_institution] += 1
           # Most common case: nothing more to do
        else:
          # Different destination course:
          num_alt[row.dst_institution] += 1
          #   Write the previous record to the history table
          r = record._asdict()
          r.pop('id')
          values_tuple = tuple(r.values())
          trans_cursor.execute(f'insert into transfers_applied_history ({cols}) values '
                               f'({placeholders}) ', values_tuple)
          # Insert the new record
          # Determine whether the src course is repeatable or not
          try:
            src_repeatable = repeatable[(src_course_id, src_offer_nbr)]
          except KeyError:
            curric_cursor.execute(f'select repeatable from cuny_courses where course_id = '
                                  f'{src_course_id} and offer_nbr = {src_offer_nbr}')
            if curric_cursor.rowcount != 1:
              num_miss[row.src_instituion] += 1
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
                          enrollment_term, row.enrollment_session, articulation_term,
                          row.model_status, posted_date, row.src_subject, src_catalog_nbr,
                          row.src_designation, row.src_grade, row.src_gpa, row.src_course_id,
                          row.src_offer_nbr, src_repeatable, row.src_description,
                          row.academic_program, row.units_taken, row.dst_institution,
                          row.dst_designation, row.dst_course_id, row.dst_offer_nbr,
                          row.dst_subject, dst_catalog_nbr, row.dst_grade, row.dst_gpa)
          trans_cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders}) ',
                               values_tuple)
          if (max_post_added is None) or (posted_date > max_post_added):
            max_post_added = posted_date

      else:
        # Anomaly: mustiple records already exist
        #   It could be that the source course is repeatable, in which case we would need to
        #   deterimine which record to replace, for which we don't yet have an algorithm.
        #   But if the course is not repeatable, it means the student's record was updated twice in
        #   one day for some reason.
        # In either case, skip the new data and enter the issue in the debug report.
        #   Or we could just add the new data to the existing melange.
        print(f'{trans_cursor.rowcount} existing {row.student_id:8} {row.dst_institution} '
              f'{int(row.dst_course_id):06}.{row.dst_offer_nbr} {row.dst_subject} '
              f'{row.dst_catalog_nbr}', file=debug)
        for record in trans_cursor.fetchall():
          print(f'  {record.student_id:8} {record.src_institution} {record.src_subject} '
                f'{record.src_catalog_nbr} {record.src_repeatable} => '
                f'{record.dst_institution} {record.dst_subject} {record.dst_catalog_nbr}',
                file=debug)
        print(file=debug)
        num_mult[row.src_institution] += 1

if max_post_added is None:
  max_post_added = 'NULL'
else:
  max_post_added = f"'{max_post_added}'"

trans_cursor.execute(f"""
insert into update_history (file_name, file_date, last_post)
            values ('{the_file.name}', '{file_date}', {max_post_added})
""")
trans_conn.commit()
trans_conn.close()

with open('reports/' + iso_file_date, 'w') as report:
  for key in sorted(num_old.keys()):
    print(f'{iso_file_date} old {key[0:3]} {num_old[key]:7,}', file=report)

  for key in sorted(num_already.keys()):
    print(f'{iso_file_date} unchanged {key[0:3]} {num_already[key]:7,}', file=report)

  for key in sorted(num_new.keys()):
    print(f'{iso_file_date} new {key[0:3]} {num_new[key]:7,}', file=report)

  for key in sorted(num_alt.keys()):
    print(f'{iso_file_date} altered {key[0:3]} {num_alt[key]:7,}', file=report)

  for key in sorted(num_mult.keys()):
    print(f'{iso_file_date} multiple {key[0:3]} {num_mult[key]:7,}', file=report)

  for key in sorted(num_miss.keys()):
    print(f'{iso_file_date} missing {key[0:3]} {num_miss[key]:7,}', file=report)

  for key in sorted(num_debug.keys()):
    key_str = f'{key[0][0:3]}:{key[1][0:3]}'
    print(f'{iso_file_date} debug {key_str} {num_debug[key]:7,}', file=report)
