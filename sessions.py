#! /usr/local/bin/python3
"""Create sessions table for 2010 to now.

Renaming CUNYfirst fields:
  Institution             institution
  Term                    term
  Session                 session
  Session Beginning Date  start_classes
  Session End Date        end_classes
  First Date to Enroll    early_enrollment
  Open Enrollment Date    open_enrollment
  Last Date to Enroll     last_enrollment
  Census Date             census_date

"""
import csv
import psycopg

from collections import namedtuple
from datetime import date
from psycopg.rows import namedtuple_row

with psycopg.connect('dbname=cuny_transfers') as conn:
  with conn.cursor() as cursor:

    cursor.execute("""
    drop table if exists sessions;
    create table sessions (
    institution           text,
    term                  integer,
    session               text,
    early_enrollment      date,
    open_enrollment       date,
    last_enrollment       date,
    classes_start         date,
    census_date           date,
    classes_end           date,
    primary key (institution, term, session)
    )
    """)

    missing_date = date.fromisoformat('1901-01-01')
    with open('./queries/QNS_CV_SESSION_TABLE.csv') as sess:
      reader = csv.reader(sess)
      for line in reader:
        if reader.line_num == 1:
          cols = [c.lower().replace(' ', '_') for c in line]
        else:
          row = dict()
          for index, value in enumerate(line):
            row[cols[index]] = value
          if row['career'].startswith('U'):
            # Convert missing dates to 1/1/1901
            for field in ['first_date_to_enroll', 'open_enrollment_date', 'last_date_to_enroll',
                          'session_beginning_date', 'census_date', 'session_end_date']:
                row[field] = row[field] if row[field] else missing_date

            cursor.execute("""
            insert into sessions values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['institution'], row['term'], row['session'], row['first_date_to_enroll'],
                  row['open_enrollment_date'], row['last_date_to_enroll'],
                  row['session_beginning_date'], row['census_date'], row['session_end_date']))
