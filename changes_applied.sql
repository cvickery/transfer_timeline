-- What changed when there is a new posted date? One courses or all of them?

select a.student_id, a.src_institution, a.src_course_id, a.dst_institution, a.dst_course_id,
       a.posted_date, h.dst_course_id, h.posted_date
  from transfers_applied a, transfers_changed h
 where a.student_id = h.student_id and a.src_course_id = h.src_course_id
   and a.dst_institution = h.dst_institution
order by a.student_id, a.dst_institution, a.src_course_id
;