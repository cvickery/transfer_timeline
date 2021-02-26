#! /usr/bin/env python3

import csv
import datetime
import sys
from collections import namedtuple
from pgconnection import PgConnection

conn = PgConnection()
cursor = conn.cursor()

cursor.execute("""
drop table if exists transfers_applied cascade;
create table transfers_applied (
id serial primary key,
student_id integer,
src_institution text,
transfer_model_nbr integer,
enrollment_term date,
enrollment_session text default '',
articulation_term date,
model_status text,
posted_date date,
src_subject text,
src_catalog_nbr text,
src_designation text,
src_grade text,
src_gpa real,
src_course_id integer,
src_offer_nbr integer,
src_description text,
academic_program text,
units_taken real,
dst_institution text,
dst_designation text,
dst_course_id integer,
dst_offer_nbr integer,
dst_subject text,
dst_catalog_nbr text,
dst_grade text,
dst_gpa real
);
""")

m = 0
with open('./CB_LEH_TRNS_DTL_SRC_CLASS_ALL.csv', errors='ignore') as csvfile:
  for n, line in enumerate(csvfile):
    pass

with open('./CB_LEH_TRNS_DTL_SRC_CLASS_ALL.csv', newline=None, errors='replace') as csvfile:
  reader = csv.reader(csvfile, )
  headers = next(reader, None)
  headers = [h.lower().replace(' ', '_') for h in headers]
  placeholders = ((len(headers)) * '%s,').strip(', ')
  cols = ', '.join([h for h in headers])
  Fields = namedtuple('Fields', headers)
  for line in reader:
    m += 1
    print(f'  {m:06,}/{n:06,}\r', end='', file=sys.stderr)
    row = Fields._make(line)

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
    cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders})',
                   (row.student_id, row.src_inst, row.transfer_model_nbr, enrollment_term,
                    articulation_term, row.model_status, posted_date, row.src_subject,
                    row.src_catalog_nbr, row.src_designation, row.src_grade, row.src_gpa,
                    row.src_course_id, row.src_offer_nbr, row.src_description,
                    row.academic_program, row.units_taken, row.dst_institution, row.dst_designation,
                    row.dst_course_id, row.dst_offer_nbr, row.dst_subject, row.dst_catalog_nbr,
                    row.dst_grade, row.dst_gpa))
conn.commit()
exit()
