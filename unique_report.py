#! /usr/bin/env python3
""" populate_transfers_applied.py generates a CSV file with records that failed the unique
    constraint for {student_id, src_course_id, src_offer_nbr, dst_institution, posted_date}.
    This code looks them up and reports what the differences were.
    For best results, it's good to sort the CSV file so that multiple cases will appear together.
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

# Assume the CSV file is named sorted.csv
sorted = Path('./sorted.csv')

m = 0
n = len(open(sorted, newline=None, errors='replace').readlines())
with open(sorted, newline=None, errors='replace') as csvfile:
  reader = csv.reader(csvfile, )
  for line in reader:

    if reader.line_num == 1:
      headers = [h.lower().replace(' ', '_') for h in line]
      Row = namedtuple('Row', headers)
    else:
      m += 1
      print(f'  {m:06,}/{n:06,}\r', end='', file=sys.stderr)
      try:
        row = Row._make(line)
      except TypeError as te:
        print(te, line, file=sys.stderr)
        continue

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

      cursor.execute(f"""
select * from transfers_applied
where student_id = {row.student_id}
  and src_course_id = {row.src_course_id}
  and src_offer_nbr = {row.src_offer_nbr}
  and posted_date = '{posted_date}'
""")
      if cursor.rowcount == 0:
        print('No Match', line, file=sys.stderr)
      else:
        print(','.join(line))
      for match in cursor.fetchall():
        print(','.join(list(f'{match._asdict().values()}')))
      print(26 * ',')

conn.commit()
exit()
