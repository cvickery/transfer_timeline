#! /usr/local/bin/python3
"""Build the admissions table."""

import csv
import psycopg
import sys

from datetime import datetime
from psycopg.rows import namedtuple_row

show_progress = len(sys.argv) > 1
start_time = datetime.now()

# More meaningful column names
csv_to_db = {'id': 'student_id',
             'career': 'career',
             'appl_nbr': 'application_number',
             'institution': 'institution',
             'acad_prog': 'academic_program',
             'status': 'status',
             'eff_date': 'effective_date',
             'effective_sequence': 'effective_sequence',
             'program_action': 'program_action',
             'action_date': 'action_date',
             'action_reason': 'action_reason',
             'admit_term': 'admit_term',
             'requirement_term': 'requirement_term',
             'campus': 'campus',
             'admit_type': 'admit_type',
             'application_fee_status': 'application_fee_status',
             'application_fee_date': 'application_fee_date',
             'last_school_attended': 'last_school_attended',
             'created_on': 'created_date',
             'last_updated_on': 'last_updated_date',
             'application_complete': 'application_complete',
             'completed_date': 'completed_date',
             'application_date': 'application_date',
             'override_deposit': 'override_deposit',
             'external_application': 'external_application'
             }

# Create the table
with psycopg.connect('dbname=cuny_transfers') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    cursor.execute("""
    drop table if exists admissions;
    create table admissions (
      student_id              integer,
      career                  text,
      application_number      integer,
      institution             text,
      academic_program        text,
      status                  text,
      effective_date          date default null,
      effective_sequence      integer,
      program_action          text,
      action_date             date default null,
      action_reason           text,
      admit_term              integer default null,
      requirement_term        integer default null,
      campus                  text,
      admit_type              text, -- Select for trd|trn
      application_fee_status  text,
      application_fee_date    date default null,
      last_school_attended    integer,
      created_date            date default null,
      last_updated_date       date default null,
      application_complete    boolean,
      completed_date          date default null,
      application_date        date default null,
      graduation_date         date default null,
      override_deposit        boolean,
      external_application    text
    )
    """)

    # Populate the table
    total_lines = sum(1 for line in open('./queries/CV_QNS_ADMISSIONS.csv'))
    with open('./queries/CV_QNS_ADMISSIONS.csv') as csv_file:
      row_count = 0
      reader = csv.reader(csv_file)
      for line in reader:
        if reader.line_num == 1:
          cols = [col.lower().replace(' ', '_') for col in line]
          admit_type_index = cols.index('admit_type')
        else:
          if show_progress:
            print(f'\r{reader.line_num:,}/{total_lines:,}', end='')
          if line[admit_type_index] in ['TRD', 'TRN']:
            # Build the row to insert, omitting missing dates and integers
            placeholders = ''
            column_names = []
            column_values = []
            row = dict()
            for index, value in enumerate(line):
              if ((cols[index].endswith('_date') and not value)
                 or (cols[index] in ['admit_term',
                                     'requirement_term',
                                     'last_school_attended'] and not value)):
                continue
              else:
                try:
                  column_names.append(csv_to_db[cols[index]])
                  column_values.append(value)
                  placeholders += ', %s'
                except KeyError:
                  # Skip unused columns
                  pass
            placeholders = placeholders.strip(', ')
            column_names = ', '.join(column_names)
            row_count += 1
            cursor.execute(f'insert into admissions'
                           f'({column_names}) values ({placeholders})', column_values)

print(f'\n{row_count:,} rows\n{(datetime.now() - start_time).seconds} seconds')
