#! /usr/local/bin/python3
""" Add the program reason descriptions to the db.
"""

import csv
import sys
from collections import namedtuple
from pathlib import Path

from pgconnection import PgConnection

project_dir = Path('/Users/vickery/Projects/transfers_applied/')
query_dir = Path(project_dir, 'Admissions_Registrations')
latest = None
for file in Path(query_dir).glob(f'PROG_REASON*'):
  if latest is None or file.stat().st_mtime > latest.stat().st_mtime:
    latest = file
print(f'Using {latest}')

conn = PgConnection('cuny_transfers')
cursor = conn.cursor()
cursor.execute("""
    drop table if exists program_reasons;
    create table program_reasons (
    institution text,
    program_action text,
    action_reason text,
    description text,
    primary key (institution, program_action, action_reason)
    );
    """)
with open(latest, newline='', errors='replace') as infile:
  reader = csv.reader(infile)
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
      cursor.execute(f"""
    insert into program_reasons values('{institution}',
                                       '{row.program_action}',
                                       '{row.action_reason}',
                                       '{description}')
    """)

conn.commit()
conn.close()
