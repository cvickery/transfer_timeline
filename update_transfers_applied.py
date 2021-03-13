#! /usr/bin/env python3
""" Replace rows in transferred_courses where the posted_date is newer; archive replaced rows.
    Add new rows.
"""

import csv
import datetime
import sys
from collections import namedtuple, defaultdict
import datetime
from pathlib import Path
from pgconnection import PgConnection

debug = open('./debug', 'w')

curric_conn = PgConnection()
curric_cursor = curric_conn.cursor()
trans_conn = PgConnection('cuny_transfers')
trans_cursor = trans_conn.cursor()

trans_cursor.execute("""
create table if not exists transfers_applied_history (
id                   serial primary key,
 student_id          integer,
 src_institution     text,
 transfer_model_nbr  integer,
 enrollment_term     date,
 enrollment_session  text,
 articulation_term   date,
 model_status        text,
 posted_date         date,
 src_subject         text,
 src_catalog_nbr     text,
 src_designation     text,
 src_grade           text,
 src_gpa             real,
 src_course_id       integer,
 src_offer_nbr       integer,
 src_repeatable      boolean,
 src_description     text,
 academic_program    text,
 units_taken         real,
 dst_institution     text,
 dst_designation     text,
 dst_course_id       integer,
 dst_offer_nbr       integer,
 dst_subject         text,
 dst_catalog_nbr     text,
 dst_grade           text,
 dst_gpa             real
);
create index on transfers_applied_history (student_id,
                                           src_course_id,
                                           src_offer_nbr,
                                           posted_date);
""")

# If a file was specified on the command line, use that. Otherwise use the latest one found in
# downloads. The idea is to allow history from previous snapshots to be captured during development,
# then to use the latest snapshot on a daily basis.
the_file = None
try:
  the_file = Path(sys.argv[1])
except IndexError as ie:
  # No snapshot specified; use latest available.
  files = Path('./downloads').glob('CV_QNS*')
  for file in files:
    if the_file is None or file.stat().st_mtime > the_file.stat().st_mtime:
      the_file = file
if the_file is None:
  sys.exit('No input file.')
print('Using;', the_file,
      datetime.datetime.fromtimestamp(the_file.stat().st_mtime).strftime('%B %d, %Y'),
      file=sys.stderr)

# Changes by dst_institution
num_new = defaultdict(int)
num_alt = defaultdict(int)
num_old = defaultdict(int)

# Skip by src_institution
num_skip = defaultdict(int)

# Cache of repeatable courses found
repeatable = dict()

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
      print(f'  {m:06,}/{n:06,}\r', end='', file=sys.stderr)
      row = Row._make(line)

      yr = 1900 + 100 * int(row.enrollment_term[0]) + int(row.enrollment_term[1:3])
      mo = int(row.enrollment_term[-1])
      da = 1
      enrollment_term = datetime.date(yr, mo, da)

      yr = 1900 + 100 * int(row.articulation_term[0]) + int(row.articulation_term[1:3])
      mo = int(row.articulation_term[-1])
      articulation_term = datetime.date(yr, mo, da)

      if '/' in row.posted_date:
        mo, da, yr = row.posted_date.split('/')
        posted_date = datetime.date(int(yr), int(mo), int(da))
      else:
        posted_date = None

      src_course_id = int(row.src_course_id)
      src_offer_nbr = int(row.src_offer_nbr)
      src_catalog_nbr = row.src_catalog_nbr.strip()
      dst_course_id = int(row.dst_course_id)
      dst_offer_nbr = int(row.dst_offer_nbr)
      dst_catalog_nbr = row.dst_catalog_nbr.strip()

      mo, da, yr = row.posted_date.split('/')
      posted_date = datetime.date(int(yr), int(mo), int(da))

      yr = 1900 + 100 * int(row.enrollment_term[0]) + int(row.enrollment_term[1:3])
      mo = int(row.enrollment_term[-1])
      da = 1
      enrollment_term = datetime.date(yr, mo, da)

      yr = 1900 + 100 * int(row.articulation_term[0]) + int(row.articulation_term[1:3])
      mo = int(row.articulation_term[-1])
      articulation_term = datetime.date(yr, mo, da)

      """ Categorize the record
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
        # Not Found: add new record
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
        # Record exists: has destination course changed?
        record = trans_cursor.fetchone()
        if int(record.dst_course_id) == dst_course_id \
           and int(record.dst_offer_nbr) == dst_offer_nbr:
           num_old[row.dst_institution] += 1
           # Most common case: nothing more to do
        else:
          # Different destination course:
          #   Write the previous record to the history table
          r = record._asdict()
          r.pop('id')
          values_tuple = tuple(r.values())
          trans_cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders}) ',
                               values_tuple)
          # Insert the new record
          # Determine whether the src course is repeatable or not
          try:
            src_repeatable = repeatable[(src_course_id, src_offer_nbr)]
          except KeyError:
            curric_cursor.execute(f'select repeatable from cuny_courses where course_id = '
                                  f'{src_course_id} and offer_nbr = {src_offer_nbr}')
            if curric_cursor.rowcount != 1:
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
                          row.dst_designation, row.dst_course_id, row.dst_offer_nbr, row.dst_subject,
                          dst_catalog_nbr, row.dst_grade, row.dst_gpa)
          trans_cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders}) ',
                               values_tuple)

      else:
        # Anomaly: mustiple records already exist
        for record in trans_cursor.fetchall():
          print(f'{record.student_id:8} {record.src_institution} {record.src_subject} '
                f'{record.src_catalog_nbr} {record.src_repeatable} =>'
                f'{record.dst_institution} {record.src_catalog_nbr}', file=debug)
        print(f'Skipping {row.student_id:8} {row.dst_institution} {row.dst_subject} '
              f'{row.dst_catalog_nbr}\n',
              file=debug)
        num_skip[row.src_institution] += 1

print('\nOld:\nRecv     Count')
for key in sorted(num_old.keys()):
  print(f'{key[0:3]}: {num_old[key]:7,}')

print('\nNew:\nRecv     Count')
for key in sorted(num_new.keys()):
  print(f'{key[0:3]}: {num_new[key]:7,}')

print('\nChanged:\nRecv     Count')
for key in sorted(num_alt.keys()):
  print(f'{key[0:3]}: {num_alt[key]:7,}')

print('\nSkipped:\nSend     Count')
for key in sorted(num_skip.keys()):
  print(f'{key[0:3]}: {num_alt[key]:7,}')
