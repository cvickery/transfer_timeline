#! /usr/bin/env python3
"""Create and populate the transfers_applied table.

The "full" CUNYfirst query includes evaluations from Spring 2018 to date. But we started recording
daily updates on March 3, 2021, so here, events after March 2, 2021 are skipped, to allow subsequent
updates to note changes. (re-evalations)
Also, ignore records where the model_status is not "Posted."
"""

import csv
import datetime
import psycopg
import resource
import sys

from collections import namedtuple
from pathlib import Path
from psycopg.rows import namedtuple_row

soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, [0x800, hard])

possibles = Path('./downloads').glob('*FULL*')
the_file = None
for possible in possibles:
  if the_file is None or possible.stat().st_mtime > the_file.stat().st_mtime:
    the_file = possible
if the_file is None:
  sys.exit('No population source found.')

file_name = the_file.name
file_date = datetime.date.fromtimestamp(the_file.stat().st_mtime)
print(f"Using: {file_name} {file_date.strftime('%B %d, %Y')}", file=sys.stderr)
print('I advise you not to do this. Repent! (or proceed anyway?) [R/p] ',
      end='', file=sys.stderr)
if input().lower().startswith('p'):
  print('Don’t say you weren’t warned!', file=sys.stderr)
else:
  sys.exit('Ill-advised consequences averted!')

curric_conn = psycopg.connect('dbname=cuny_curriculum')
curric_cursor = curric_conn.cursor(row_factory=namedtuple_row)

trans_conn = psycopg.connect('dbname=cuny_transfers')
trans_cursor = trans_conn.cursor(row_factory=namedtuple_row)

trans_cursor.execute("""

drop table if exists transfers_applied cascade;

create table transfers_applied (
  student_id          integer not NULL,
  src_institution     text not NULL,
  enrollment_term     integer not NULL,
  enrollment_session  text not NULL,
  articulation_term   integer not NULL,
  model_status        text not NULL,
  model_nbr           integer not NULL,
  posted_date         date,
  src_subject text    not NULL,
  src_catalog_nbr     text not NULL,
  src_designation     text not NULL,
  src_grade           text not NULL,
  src_gpa             real not NULL,
  src_course_id       integer not NULL,
  src_offer_nbr       integer not NULL,
  src_is_repeatable   boolean not NULL,
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
  dst_is_message      boolean not NULL,
  dst_is_blanket      boolean not NULL,
  credit_source_type  text,

  primary key (student_id, src_course_id, src_offer_nbr,
                           dst_course_id, dst_offer_nbr,
                           articulation_term, posted_date)
);


-- The update_history table
drop table if exists update_history;

create table update_history (
file_name   text primary key,
file_date   date,
last_post   date,
num_records integer,
num_added   integer,
num_skipped integer
);

commit;
""")

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
curric_cursor.close()

last_post = None
num_added = 0
num_changed = 0   # Will not change here.
num_skipped = 0

march_2_2021 = datetime.date(2021, 3, 2)

cols = ['student_id', 'src_institution', 'enrollment_term', 'enrollment_session',
        'articulation_term', 'model_status', 'model_nbr', 'posted_date', 'src_subject',
        'src_catalog_nbr', 'src_designation', 'src_grade', 'src_gpa', 'src_course_id',
        'src_offer_nbr', 'src_is_repeatable', 'src_description', 'academic_program', 'units_taken',
        'dst_institution', 'dst_designation', 'dst_course_id', 'dst_offer_nbr', 'dst_subject',
        'dst_catalog_nbr', 'dst_grade', 'dst_gpa', 'dst_is_message', 'dst_is_blanket']
placeholders = ((len(cols)) * '%s,').strip(', ')
cols = ','.join(cols)

with open('./Logs/populate.log', 'w') as logfile:

  # There are discrepancies between the number of lines in the .csv file and the number of records
  # actually found, presumably because of newlines in the comments field.
  num_records = 0
  num_lines = len(open(the_file, newline=None, errors='backslashreplace').readlines()) - 1
  with open(the_file, newline=None, errors='backslashreplace') as csvfile:
    reader = csv.reader(csvfile, )

    for line in reader:

      if reader.line_num == 1:
        headers = [h.lower().replace(' ', '_') for h in line]
        Row = namedtuple('Row', headers)

      else:

        num_records += 1
        print(f'    {num_records:6,}/{num_lines:6,}\r', end='', file=sys.stderr)

        row = Row._make(line)
        if reader.line_num == 2 and 'sysdate' in headers:
          mo, da, yr = row.sysdate.split('/')
          file_date = datetime.date(int(yr), int(mo), int(da))

        if '/' in row.posted_date:
          mo, da, yr = row.posted_date.split('/')
          posted_date = datetime.date(int(yr), int(mo), int(da))
          if last_post is None or last_post < posted_date:
            last_post = posted_date
        else:
          # Missing posted_date
          posted_date = datetime.date(1901, 1, 1)

        # Skip records that are not posted or which have a posted_date greater than March 2, 2021
        if row.model_status != 'Posted' or posted_date > march_2_2021:
          num_skipped += 1
          continue

        src_course_id = int(row.src_course_id)
        src_offer_nbr = int(row.src_offer_nbr)
        src_catalog_nbr = row.src_catalog_nbr.strip()
        dst_course_id = int(row.dst_course_id)
        dst_offer_nbr = int(row.dst_offer_nbr)
        dst_catalog_nbr = row.dst_catalog_nbr.strip()

        # Is the src course repeatable; is dst course in MESG or BKCR
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
        if trans_cursor.rowcount == 1:
          num_added += 1
        else:
          print(f'Skipped {value_tuple}', file=logfile)
          num_skipped += 1

  # Report difference between num_lines and num_records.
  print(f'Lines: {num_lines}\nRecords{num_records}', file=logfile)

  # Update the update_history table
  trans_cursor.execute(f"""
  insert into update_history values(
            '{file_name}', '{file_date}', '{last_post}',
            {num_records}, {num_added}, {num_skipped})
  """)

trans_conn.commit()
exit()
