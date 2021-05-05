#! /usr/local/bin/python3
""" Add new rows to transfers_applied.
    Using query data that covers the past week, so skip rows that already exist.
    Since the posted_date is part of the primary key, there is no longer a need to check the posted
    date for redundancy, only for info in the update_history table.
"""

import csv
import datetime
import sys
import argparse

from collections import namedtuple, defaultdict
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
  possibles = Path('./downloads').glob('CV*ALL*')
  for possible in possibles:
    if the_file is None or possible.stat().st_mtime > the_file.stat().st_mtime:
      the_file = possible
  if the_file is None:
    sys.exit('No update source found.')
else:
  the_file = Path(the_file)

# Using the date the file was transferred to Tumbleweed as a proxy for CF SYSDATE
file_name = the_file.name
file_date = datetime.date.fromtimestamp(the_file.stat().st_mtime)
print('Using:', file_name, file_date.strftime('%B %d, %Y'), file=sys.stderr)

# Caches of all repeatable, message, and blanket credit courses
curric_cursor.execute("select course_id, offer_nbr from cuny_courses where repeatable = 'Y'")
repeatables = [(int(row.course_id), int(row.offer_nbr)) for row in curric_cursor.fetchall()]
curric_cursor.execute("""select course_id, offer_nbr
                       from cuny_courses
                       where designation in ('MLA', 'MNL')""")
messages = [(int(row.course_id), int(row.offer_nbr)) for row in curric_cursor.fetchall()]
curric_cursor.execute("""select course_id, offer_nbr
                       from cuny_courses
                       where attributes ~* 'BKCR'""")
blankets = [(int(row.course_id), int(row.offer_nbr)) for row in curric_cursor.fetchall()]
curric_conn.close()

# Latest posted date, and counts for the update_history table
last_post = None
num_added = 0
num_skipped = 0
max_new_post = None

cols = ['student_id', 'src_institution', 'enrollment_term', 'enrollment_session',
        'articulation_term', 'model_status', 'model_nbr', 'posted_date', 'src_subject',
        'src_catalog_nbr', 'src_designation', 'src_grade', 'src_gpa', 'src_course_id',
        'src_offer_nbr', 'src_is_repeatable', 'src_description', 'academic_program', 'units_taken',
        'dst_institution', 'dst_designation', 'dst_course_id', 'dst_offer_nbr', 'dst_subject',
        'dst_catalog_nbr', 'dst_grade', 'dst_gpa', 'dst_is_message', 'dst_is_blanket']
placeholders = ((len(cols)) * '%s,').strip(', ')
cols = ','.join(cols)

# Progress indicators
num_records = 0
num_lines = len(open(the_file, newline=None, errors='backslashreplace').readlines()) - 1
with open(f'./Logs/update_{file_date.isoformat()}.log', 'w') as logfile:
  with open(the_file, encoding='ascii', errors='backslashreplace') as csv_file:
    reader = csv.reader(csv_file)
    for line in reader:

      if reader.line_num == 1:
        headers = [h.lower().replace(' ', '_') for h in line]
        Row = namedtuple('Row', headers)

      else:
        row = Row._make(line)
        if reader.line_num == 2 and 'sysdate' in headers:
          mo, da, yr = row.sysdate.split('/')
          file_date = datetime.date(int(yr), int(mo), int(da))

        num_records += 1
        if progress:
          print(f'    {num_records:6,}/{num_lines:6,}\r', end='', file=sys.stderr)

        row = Row._make(line)
        if '/' in row.posted_date:
          mo, da, yr = row.posted_date.split('/')
          posted_date = datetime.date(int(yr), int(mo), int(da))
        else:
          posted_date = datetime.date(1901, 1, 1)

        src_course_id = int(row.src_course_id)
        src_offer_nbr = int(row.src_offer_nbr)
        src_catalog_nbr = row.src_catalog_nbr.strip()
        dst_course_id = int(row.dst_course_id)
        dst_offer_nbr = int(row.dst_offer_nbr)
        dst_catalog_nbr = row.dst_catalog_nbr.strip()

        # Is the src course is repeatable; is dst course is MESG or BKCR
        src_is_repeatable = (src_course_id, src_offer_nbr) in repeatables
        dst_is_message = (dst_course_id, dst_offer_nbr) in messages
        dst_is_blanket = (dst_course_id, dst_offer_nbr) in blankets

        value_tuple = (row.student_id, row.src_institution, row.enrollment_term,
                       row.enrollment_session, row.articulation_term, row.model_status,
                       row.transfer_model_nbr, posted_date, row.src_subject, src_catalog_nbr,
                       row.src_designation, row.src_grade, row.src_gpa, row.src_course_id,
                       row.src_offer_nbr, src_is_repeatable, row.src_description,
                       row.academic_program, row.units_taken, row.dst_institution,
                       row.dst_designation, row.dst_course_id, row.dst_offer_nbr, row.dst_subject,
                       dst_catalog_nbr, row.dst_grade, row.dst_gpa, dst_is_message, dst_is_blanket)
        trans_cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders}) '
                             f'on conflict do nothing',
                             value_tuple)
        if trans_cursor.rowcount == 0:
          print(f'Skipped {value_tuple}', file=logfile)
          num_skipped += 1
        else:
          assert trans_cursor.rowcount == 1, (f'inserted {trans_cursor.rowcount} rows\n'
                                              f'{trans_cursor.query}')
          num_added += trans_cursor.rowcount
          if (max_new_post is None) or (posted_date > max_new_post):
            max_new_post = posted_date

  # Report difference between num_lines and num_records.
  print(f'Lines: {num_lines}\nRecords: {num_records}', file=logfile)

# Prepare summary info
if max_new_post is None:
  max_new_post = 'NULL'
else:
  max_new_post = f"'{max_new_post}'"

trans_cursor.execute(f"""
    insert into update_history values(
          '{file_name}', '{file_date}', {max_new_post},
          {num_records}, {num_added}, {num_skipped})
          on conflict do nothing
  """)
if trans_cursor.rowcount == 0:
  print(f"""Update History conflict\n new:
        '{file_name}', '{file_date}', {max_new_post},
        {num_records}, {num_added}, {num_skipped})
        """, file=logfile)

trans_conn.commit()
trans_conn.close()
