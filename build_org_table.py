#! /usr/local/bin/python3
""" Build a subset of the external organizations table.
    Used to see where students are coming from when applying.
"""

import csv
from collections import namedtuple
from pathlib import Path
from pgconnection import PgConnection

conn = PgConnection('cuny_transfers')
cursor = conn.cursor()
cursor.execute("""
drop table if exists organizations;
create table organizations (
id integer primary key,
search_name text,
organization_type text,
description text,
status text
)
""")
latest = None
paths = Path('./downloads').glob('*ORG*')
for path in paths:
  if latest is None or path.stat().st_mtime > latest.st_mtime:
    latest = path
with open (latest) as csv_file:
  reader = csv.reader(csv_file)
  for line in reader:
    if reader.line_num == 1:
      cols = [col.lower().replace(' ', '_') for col in line]
      Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      if row.external_org_id.isdecimal():
        cursor.execute(f"""
    insert into organizations values(%s, %s, %s, %s, %s)
    on conflict do nothing
""", (int(row.external_org_id), row.search_name, row.organization_type, row.description,
            row.status_as_of_effective_date))
conn.commit()
