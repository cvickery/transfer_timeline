#! /usr/local/bin/python3
"""Build the program_reasons table."""

import csv
import psycopg
import sys

from collections import namedtuple
from pathlib import Path
from psycopg.rows import namedtuple_row

home_dir = Path.home()
project_dir = Path(home_dir, 'Projects/transfer_timeline/')
query_dir = Path(project_dir, 'queries')

with psycopg.connect('dbname=cuny_transfers') as conn:
  conn.execute("""
    drop table if exists program_reasons;
    create table program_reasons (
    institution text,
    program_action text,
    action_reason text,
    description text,
    primary key (institution, program_action, action_reason)
    );
    """)
  with Path(query_dir, 'PROG_REASON_TBL.csv').open() as reason_file:
    reader = csv.reader(reason_file)
    for line in reader:
      if reader.line_num == 1:
        cols = [col.lower().replace(' ', '_') for col in line]
        Row = namedtuple('Row', cols)
      else:
        row = Row._make(line)
        if row.setid.startswith('GRD') or row.setid.startswith('UAC') or row.status != 'A':
          continue
        description = row.short_description
        if len(row.description) > len(description):
          description = row.description
        if len(row.long_description) > len(description):
          description = row.long_description
        institution = row.setid[0:3]
        conn.execute(f"""
        insert into program_reasons values('{institution}',
                                           '{row.program_action}',
                                           '{row.action_reason}',
                                           '{description}')
        """)
