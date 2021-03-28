#! /usr/bin/env python3

import csv
import datetime
import sys
from collections import namedtuple
from pathlib import Path
from pgconnection import PgConnection

possibles = Path('/Users/vickery/transfers_applied/downloads').glob('*FULL*')
latest = None
for possible in possibles:
  if latest is None or possible.stat().st_mtime > latest.stat().st_mtime:
    latest = possible
if latest is None:
  sys.exit('No population source found.')

print('You are about to overwrite years of intense effort. Repent or Proceed? (R/p) ',
      end='', file=sys.stderr)
if not input().lower().startswith('p'):
  sys.exit('Ill-advised consequences averted.')

repeatable = dict()

curric_conn = PgConnection('cuny_curriculum')
curric_cursor = curric_conn.cursor()

trans_conn = PgConnection('cuny_transfers')
trans_cursor = trans_conn.cursor()

trans_cursor.execute("""

drop table if exists transfers_applied cascade;

create table transfers_applied (
  id                  serial primary key,
  student_id          integer not NULL,
  src_institution     text not NULL,
  transfer_model_nbr  integer not NULL,
  enrollment_term     integer not NULL,
  enrollment_session  text not NULL,
  articulation_term   integer not NULL,
  model_status        text not NULL,
  posted_date         date,
  src_subject text    not NULL,
  src_catalog_nbr     text not NULL,
  src_designation     text not NULL,
  src_grade           text not NULL,
  src_gpa             real not NULL,
  src_course_id       integer not NULL,
  src_offer_nbr       integer not NULL,
  src_repeatable      boolean, -- See ./lookup_failures for courses not found
  src_description     text not NULL,
  academic_program    text not NULL,
  units_taken         real not NULL,
  dst_institution     text not NULL,
  dst_designation     text not NULL,
  dst_course_id       integer not NULL,
  dst_offer_nbr       integer not NULL,
  dst_subject         text not NULL,
  dst_catalog_nbr     text not NULL,
  dst_grade           text not NULL,
  dst_gpa             real not NULL,
  user_id             text not NULL,
  reject_reason       text not NULL,
  transfer_overridden boolean default false,
  override_reason     text not NULL,
  comment             text not NULL,
  constraint single unique (student_id, src_course_id, src_offer_nbr, dst_institution, posted_date)
);

drop table if exists missing_courses cascade;

create table missing_courses (
course_id             integer not NULL,
offer_nbr             integer not NULL,
institution           text not NULL,
subject               text not NULL,
catalog_number        text not NULL,
primary key (course_id, offer_nbr)
);

-- Set up the archive table too
drop table if exists transfers_changed cascade;
create table transfers_changed (
  id                  serial primary key,
  student_id          integer,
  src_institution     text,
  transfer_model_nbr  integer,
  enrollment_term     integer,
  enrollment_session  text,
  articulation_term   integer,
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
  dst_gpa             real,
  user_id             text,
  reject_reason       text,
  transfer_overridden boolean,
  override_reason     text,
  comment             text
);

-- The update_history table
drop table if exists update_history;

create table update_history (
file_name   text primary key,
file_date   date,
last_post   date,
num_records integer,
num_added   integer,
num_changed integer,
num_skipped integer,
num_missing integer
);

commit;

""")

file_name = latest.name
file_date = datetime.datetime.fromtimestamp(latest.stat().st_mtime).strftime('%Y-%m-%d')
last_post = None
num_added = 0
num_changed = 0   # Will not change here.
num_skipped = 0
num_missing = 0   # Transfers where sending course is not in course catalog

m = 0
num_records = len(open(latest, newline=None, errors='replace').readlines())
with open(latest, newline=None, errors='replace') as csvfile:
  reader = csv.reader(csvfile, )
  for line in reader:

    if reader.line_num == 1:
      headers = [h.lower().replace(' ', '_') for h in line]
      cols = [h for h in headers]
      values_added = None
      if 'comment' not in cols:
        # Columns not present in queries prior to 2021-03-18
        cols += ['user_id', 'reject_reason', 'transfer_overridden', 'override_reason', 'comment']
        values_added = ('', '', False, '', '')
      # Column derived from course catalog lookup
      cols.insert(1 + cols.index('src_offer_nbr'), 'src_repeatable')
      # print('cols array', len(cols), cols)
      placeholders = ((len(cols)) * '%s,').strip(', ')
      # print('placeholders', placeholders.count('s'), placeholders)
      cols = ', '.join([c for c in cols])
      # print('cols string', cols.count(',') + 1, cols)
      Row = namedtuple('Row', headers)
    else:
      m += 1
      print(f'  {m:06,}/{num_records:06,}\r', end='', file=sys.stderr)
      row = Row._make(line)

      # yr = 1900 + 100 * int(row.enrollment_term[0]) + int(row.enrollment_term[1:3])
      # mo = int(row.enrollment_term[-1])
      # da = 1
      # enrollment_term = datetime.date(yr, mo, da)

      # yr = 1900 + 100 * int(row.articulation_term[0]) + int(row.articulation_term[1:3])
      # mo = int(row.articulation_term[-1])
      # articulation_term = datetime.date(yr, mo, da)

      if '/' in row.posted_date:
        mo, da, yr = row.posted_date.split('/')
        posted_date = datetime.date(int(yr), int(mo), int(da))
        if last_post is None or last_post < posted_date:
          last_post = posted_date
      else:
        posted_date = None

      src_course_id = int(row.src_course_id)
      src_offer_nbr = int(row.src_offer_nbr)
      src_catalog_nbr = row.src_catalog_nbr.strip()
      dst_course_id = int(row.dst_course_id)
      dst_offer_nbr = int(row.dst_offer_nbr)
      dst_catalog_nbr = row.dst_catalog_nbr.strip()

      try:
        src_repeatable = repeatable[(src_course_id, src_offer_nbr)]
      except KeyError:
        curric_cursor.execute(f'select repeatable from cuny_courses where course_id = '
                              f'{src_course_id} and offer_nbr = {src_offer_nbr}')
        if curric_cursor.rowcount != 1:
          num_missing += 1
          trans_cursor.execute(f"insert into missing_courses values({src_course_id}, "
                               f"{src_offer_nbr}, '{row.src_institution}', '{row.src_subject}', "
                               f"'{row.src_catalog_nbr}') on conflict do nothing")
          src_repeatable = None
        else:
          src_repeatable = curric_cursor.fetchone().repeatable
        repeatable[(int(row.src_course_id), (row.src_offer_nbr))] = src_repeatable

      value_tuple = (row.student_id, row.src_institution, row.transfer_model_nbr,
                     row.enrollment_term, row.enrollment_session, row.articulation_term,
                     row.model_status, posted_date, row.src_subject, src_catalog_nbr,
                     row.src_designation, row.src_grade, row.src_gpa, row.src_course_id,
                     row.src_offer_nbr, src_repeatable, row.src_description,
                     row.academic_program, row.units_taken, row.dst_institution,
                     row.dst_designation, row.dst_course_id, row.dst_offer_nbr, row.dst_subject,
                     dst_catalog_nbr, row.dst_grade, row.dst_gpa)
      if values_added is not None:
        value_tuple += values_added
      # print('values tuple', len(value_tuple), value_tuple)
      trans_cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders}) '
                           f'on conflict do nothing',
                           value_tuple)
      if trans_cursor.rowcount == 1:
        num_added += 1
      else:
        num_skipped += 1

trans_cursor.execute(f"""
insert into update_history values(
          '{file_name}', '{file_date}', '{last_post}',
          {num_records}, {num_added}, {num_changed}, {num_skipped}, {num_missing})
""")

trans_conn.commit()
exit()
