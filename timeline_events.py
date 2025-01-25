#! /usr/local/bin/python3
"""Given a cohort list of institution-student pairs, generate a list of events for each pair.

Three categories of events: admissions, registrations, and evaluations.
"""
import csv
import psycopg
import sys

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from psycopg.rows import dict_row

start_at = datetime.now()

# Build a list of (student, institution) tuples
if len(sys.argv) > 1:
  input_file = Path(sys.argv[1])
else:
  input_file = None
  input_filename = ''

while input_file is None:
  try:
    input_file = Path(input_filename, 'r')
  except (FileNotFoundError, TypeError):
    input_filename = input('Cohort file? ')

# Assume the first three characters of the input filename can be used as a cohort code
cohort_code = input_file.name[0:3].lower()

cohorts = defaultdict(list)
reader = csv.reader(input_file.open())
for line in reader:
  if reader.line_num == 1:
    pass
  else:
    student_id = int(line[0])
    institution = line[2].upper()[0:3] + '01'
    cohorts[institution].append(student_id)

with psycopg.connect('dbname=cuny_transfers') as conn:
  with conn.cursor(row_factory=dict_row) as cursor:
    all_student_ids = []
    for institution, student_ids in sorted(cohorts.items()):
      print(institution, len(student_ids))
      all_student_ids += student_ids
    student_id_list = ','.join([f'{student_id}' for student_id in all_student_ids])

    # Admission Events
    output_file = open(f'cohort_reports/{cohort_code}_admissions_'
                       f'{str(datetime.today())[0:10]}.csv', 'w')
    writer = csv.writer(output_file)
    cursor.execute(f"""
    select * from admissions
     where student_id in ({student_id_list})
     order by (institution, student_id, action_date)
    """)
    header_row = None
    for row in cursor:
      if header_row is None:
        header_row = row.keys()
        writer.writerow(header_row)
      else:
        out_row = [f'{value}' for value in row.values()]
        writer.writerow(out_row)
    output_file.close()

    # Registration Events
    output_file = open(f'cohort_reports/{cohort_code}_registrations_'
                       f'{str(datetime.today())[0:10]}.csv', 'w')
    writer = csv.writer(output_file)
    cursor.execute(f"""
    select * from registrations
     where student_id in ({student_id_list})
     order by (institution, student_id, add_date, drop_date)
    """)
    header_row = None
    for row in cursor:
      if header_row is None:
        header_row = row.keys()
        writer.writerow(header_row)
      else:
        out_row = [f'{value}' for value in row.values()]
        writer.writerow(out_row)
    output_file.close()

    # Evaluation Events (note dst_institution field name; not just institution)
    output_file = open(f'cohort_reports/{cohort_code}_evaluations_'
                       f'{str(datetime.today())[0:10]}.csv', 'w')
    writer = csv.writer(output_file)
    cursor.execute(f"""
    select * from transfers_applied
     where student_id in ({student_id_list})
     order by (dst_institution, student_id, posted_date)
    """)
    header_row = None
    for row in cursor:
      if header_row is None:
        header_row = row.keys()
        writer.writerow(header_row)
      else:
        out_row = [f'{value}' for value in row.values()]
        writer.writerow(out_row)
    output_file.close()

print(f'{(datetime.now() - start_at).seconds} seconds')
