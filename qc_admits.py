#! /usr/local/bin/python3

import csv
from collections import namedtuple
import datetime
from pgconnection import PgConnection
conn = PgConnection('cuny_transfers')
cursor = conn.cursor()
cursor.execute("""
drop table if exists qc_spring_21;
create table qc_spring_21 (student_id integer,
                           admit_date date,
                           primary key (student_id, admit_date))
""")

with open('./Admissions_Registrations/QC_Spring_21_Transfer_Admit_dates.csv') as admit_file:
  reader = csv.reader(admit_file)
  for line in reader:
    id = int(line[0].replace('\ufeff', ''))
    m, d, y = line[1].split('/')
    cursor.execute(f'insert into qc_spring_21 values (%s, %s) on conflict do nothing',
                   (id, datetime.date(int(y) + 2000, int(m), int(d))))
    if cursor.rowcount == 0:
      print(f'{cursor.query.decode()}')
conn.commit()
