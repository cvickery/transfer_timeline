#! /usr/bin/env python3
""" Replace rows in transferred_courses where the posted_date changed; archive replaced rows.
"""

import csv
import datetime
import sys
from collections import namedtuple
from pathlib import Path
from pgconnection import PgConnection
from psycopg2 import errors

conn = PgConnection()
cursor = conn.cursor()

cursor.execute("""
create table if not exists transfers_applied_archive (
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
create unique index on transfers_applied_archive (student_id,
                                                  src_course_id,
                                                  src_offer_nbr,
                                                  posted_date);
""")

files = Path('./downloads').glob('CV_QNS*')
latest = None
for file in files:
  if latest is None or file.stat().st_mtime > latest.stat().st_mtime:
    latest = file

m = 0
n = len(open(latest).readlines()) - 1
with open(latest) as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    if reader.line_num == 1:
      headers = [h.lower().replace(' ', '_') for h in line]
      Row = namedtuple('Row', headers)
      placeholders = ((len(headers)) * '%s,').strip(', ')
      posted_date_index = headers.index('posted_date')
      enrollment_term_index = headers.index('enrollment_term')
      articulation_term_index = headers.index('articulation_term')

    else:
      row = Row._make(line)
      m += 1
      print(f'  {m:,}/{n:,}\r', end='', file=sys.stderr)

      mo, da, yr = row.posted_date.split('/')
      posted_date = datetime.date(int(yr), int(mo), int(da))

      yr = 1900 + 100 * int(row.enrollment_term[0]) + int(row.enrollment_term[1:3])
      mo = int(row.enrollment_term[-1])
      da = 1
      enrollment_term = datetime.date(yr, mo, da)

      yr = 1900 + 100 * int(row.articulation_term[0]) + int(row.articulation_term[1:3])
      mo = int(row.articulation_term[-1])
      articulation_term = datetime.date(yr, mo, da)

      cursor.execute(f"""
select * from transfers_applied
 where student_id = {row.student_id}
   and src_course_id = {row.src_course_id}
   and src_offer_nbr = {row.src_offer_nbr}
   and posted_date != '{posted_date}'::date
order by posted_date
""")
      for changed in cursor.fetchall():
        fields = list(changed._asdict().keys())[1:]
        fields = ', '.join([f for f in fields])
        old_values = list(changed._asdict().values())[1:]
        new_values = list(row._asdict().values())
        # Put terms and dates in db format
        new_values[posted_date_index] = posted_date
        new_values[enrollment_term_index] = enrollment_term
        new_values[articulation_term_index] = articulation_term
        print(old_values)
        print(new_values)
        # print(f'\n{fields}\n{values}\n{placeholders}\n', file=sys.stderr)
        print(f'{changed.posted_date} {changed.student_id} {changed.src_subject} {changed.src_catalog_nbr} => {changed.dst_subject} {changed.dst_catalog_nbr}')
        print(f'{posted_date} {row.student_id} {row.src_subject} {row.src_catalog_nbr} => {row.dst_subject} {row.dst_catalog_nbr}')
        print()
        cursor.execute(f"""
insert into transfers_applied_archive({fields}) values ({placeholders})
""", tuple(old_values))
        cursor.execute(f"""
update transfers_applied set({fields}) = ({placeholders})
 where id = {changed.id}
""", tuple(new_values))
        # TRY CHECKING ID VALUES
        conn.commit()
        exit()

conn.commit()
