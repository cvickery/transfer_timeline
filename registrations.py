#! /usr/local/bin/python3
"""Build the registrations table."""

import csv
import psycopg

from collections import namedtuple
from datetime import datetime
from psycopg.rows import dict_row

start_time = datetime.now()

# Set up registrations column names
csv_to_db = {'id': 'student_id',
             'career': 'career',
             'institution': 'institution',
             'term': 'term',
             'session': 'session',
             'student_enrollment_status': 'enrollment_status',
             'enrollment_status_reason': 'enrollment_reason',
             'last_enrollment_action': 'last_enrollment_action',
             'enrollment_add_date': 'add_date',
             'enrollment_drop_date': 'drop_date',
             'designation': 'requirement_designation',
             'academic_group': 'academic_group',
             'last_enrollment_action_process': 'process_code'
             }

# Create the table
with psycopg.connect('dbname=cuny_transfers') as conn:
  with conn.cursor() as cursor:
    cursor.execute("""
    drop table if exists registrations;
    create table registrations (
    student_id               integer,
    career                   text,
    institution              text,
    term                     integer,
    session                  text,
    enrollment_status        text,
    enrollment_reason        text,
    last_enrollment_action   text,
    add_date                 date default null,
    drop_date                date default null,
    requirement_designation  text,
    academic_group           text,
    process_code             text
    )
    """)

    # Populate the table
    # One row per (student, institution, term, session) for each registraton date and add/drop
    # indicator.
    total_lines = sum(1 for line in open('queries/CV_QNS_STUDENT_SUMMARY.csv'))
    with open('queries/CV_QNS_STUDENT_SUMMARY.csv') as csv_file:
      reader = csv.reader(csv_file)

      counter = 0
      this_record_key = None
      registrations = None

      for line in reader:
        if reader.line_num == 1:
          cols = [col.lower().replace(' ', '_').replace('-', '_') for col in line]
          Row = namedtuple('Row', cols)
        else:
          print(f'\r{reader.line_num:,}/{total_lines:,}', end='')
          row = Row._make(line)
          if row.career.startswith('U'):
            counter += 1
            placeholders = ''
            column_names = []
            column_values = []
            for line_key, row_key in csv_to_db.items():
              if row_key.endswith('_date') and not line[cols.index(line_key)]:
                continue
              placeholders += ',%s '
              column_names.append(row_key)
              column_values.append(line[cols.index(line_key)])

            placeholders = placeholders.strip(', ')
            column_names = ', '.join(column_names)
            cursor.execute(f"""
            insert into registrations ({column_names}) values ({placeholders})
            """, column_values)

print(f'\n{(datetime.now() - start_time).seconds} seconds\n{counter:,} records')
