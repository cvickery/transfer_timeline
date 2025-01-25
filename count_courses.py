#! /usr/bin/env python3
"""  Generate report of courses transferred keyed by sending and receiving colleges, course,
     articulation term, and rule_key.
"""
from pgconnection import PgConnection

conn = PgConnection()
cursor = conn.cursor()

cursor.execute("""
select src_institution,
       lpad(src_course_id::text, 6, '0')||'.'||src_offer_nbr||': '||
       src_subject||' '||src_catalog_nbr as sending_course,
       dst_institution,
       lpad(src_course_id::text, 6, '0')||'.'||dst_offer_nbr||': '||
       dst_subject||' '||dst_catalog_nbr as receiving_course,
       dst_designation,
       count(*)
  from transfers_applied
  group by sending_course, receiving_course, dst_designation,
    rollup (src_institution, dst_institution)
order by src_institution, dst_institution, count desc, sending_course
""")
print('Count, Sending, Course, Receiving, Course, RD')
for row in cursor.fetchall():
  print(f'{row.count}, {row.src_institution}, {row.sending_course}, '
        f'{row.dst_institution}, {row.receiving_course}, {row.dst_designation}')
